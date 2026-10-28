#!/usr/bin/env python3
"""抓微信 4.x（Linux arm64）数据库密钥的 passphrase，写进连接器的 keys.json

只用来读【你自己登录的这个微信号】在本机的数据。需要 root 和 ptrace 权限（容器加 SYS_PTRACE）。

用法（在微信容器里，以 root 运行）：
  python3 wxkey.py capture --store /config/wechat-connector/keys.json   挂断点等扫码登录，抓到后校验并保存
  python3 wxkey.py verify  --store ... --candidates <文件>        用已抓到的候选校验并保存

原理：微信 4.1+ 只在登录时用 passphrase 派生一次库密钥。在 ELF 里找 WCDB 的
"com.Tencent.WCDB.Config.Cipher" 字符串和引用它的函数，GDB 在那里下断点，登录时读出参数。
思路来自 TANGandXUE/wcdb-key-tool（MIT，只支持 x86_64），这里改写成 arm64（ADRP+ADD 寻址、x1/x2 传参）。
x86_64 机器请直接用 wcdb-key-tool。
"""
import argparse
import glob
import os
import struct
import subprocess
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from wxdb import KeyStore, derive_key, verify_enc_key, PAGE, SALT_SZ  # noqa: E402

ANCHOR = b"com.Tencent.WCDB.Config.Cipher"
EM_AARCH64 = 183


def wechat_main_pid() -> int:
    best, best_rss = None, -1
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            if os.path.basename(os.readlink(f"/proc/{d}/exe")) != "wechat":
                continue
            rss = next(int(l.split()[1]) for l in open(f"/proc/{d}/status") if l.startswith("VmRSS:"))
        except (OSError, StopIteration):
            continue
        if rss > best_rss:
            best, best_rss = int(d), rss
    if best is None:
        sys.exit("没找到微信进程")
    return best


def load_sections(path: str) -> tuple[int, dict]:
    with open(path, "rb") as f:
        blob = f.read()
    if blob[:4] != b"\x7fELF" or blob[4] != 2:
        sys.exit("不是 64 位 ELF")
    e_type, machine = struct.unpack_from("<HH", blob, 16)
    if machine != EM_AARCH64:
        sys.exit(f"只支持 arm64（e_machine={machine}）；x86_64 请用 wcdb-key-tool")
    shoff = struct.unpack_from("<Q", blob, 0x28)[0]
    shentsize, shnum, shstrndx = struct.unpack_from("<HHH", blob, 0x3A)
    hdrs = [struct.unpack_from("<IIQQQQIIQQ", blob, shoff + i * shentsize) for i in range(shnum)]
    strtab = hdrs[shstrndx][4]
    secs = {}
    for h in hdrs:
        name = blob[strtab + h[0]: blob.index(b"\0", strtab + h[0])].decode()
        secs[name] = (h[3], blob[h[4]: h[4] + h[5]])
    return e_type, secs


def adrp_refs(text_addr: int, text: bytes) -> tuple[dict, dict]:
    """扫 .text 里的 ADRP + ADD（直接取地址）和 ADRP + LDR（经 GOT 取地址）组合，
    返回 ({目标地址: [(指令偏移, 寄存器)]}, {GOT 槽地址: [(指令偏移, 寄存器)]})"""
    adds: dict[int, list[tuple[int, int]]] = {}
    gots: dict[int, list[tuple[int, int]]] = {}
    pending: dict[int, tuple[int, int]] = {}  # reg -> (page, 指令序号)
    n = len(text) // 4
    words = struct.unpack_from(f"<{n}I", text)
    for i, w in enumerate(words):
        if (w & 0x9F000000) == 0x90000000:  # ADRP
            imm = (((w >> 5) & 0x7FFFF) << 2) | ((w >> 29) & 3)
            if imm & (1 << 20):
                imm -= 1 << 21
            pc = text_addr + i * 4
            pending[w & 0x1F] = ((pc & ~0xFFF) + (imm << 12), i)
            continue
        rd, rn = w & 0x1F, (w >> 5) & 0x1F
        hit = pending.get(rn)
        if not hit or i - hit[1] > 8:
            continue
        if (w & 0xFF800000) == 0x91000000:  # ADD Xd, Xn, #imm
            imm = (w >> 10) & 0xFFF
            if (w >> 22) & 1:
                imm <<= 12
            adds.setdefault(hit[0] + imm, []).append((i * 4, rd))
        elif (w & 0xFFC00000) == 0xF9400000:  # LDR Xt, [Xn, #imm]
            gots.setdefault(hit[0] + ((w >> 10) & 0xFFF) * 8, []).append((i * 4, rd))
    return adds, gots


def func_start(text: bytes, off: int) -> int | None:
    """从 off 往回找函数入口：开栈指令（sub sp / stp x29,x30 / paciasp / bti c），
    并且前一条是上一个函数的结尾（ret、b、br、nop 填充）"""
    def word(p):
        return struct.unpack_from("<I", text, p)[0] if p >= 0 else 0xD65F03C0

    def is_prologue(w):
        return (w in (0xD503233F, 0xD503245F)
                or (w & 0xFF8003FF) == 0xD10003FF      # sub sp, sp, #imm
                or (w & 0xFFC07FFF) == 0xA9807BFD)     # stp x29, x30, [sp, #-n]!

    def is_end(w):
        return (w in (0xD65F03C0, 0xD503201F, 0)       # ret / nop / 填充
                or (w & 0xFC000000) == 0x14000000      # b
                or (w & 0xFFFFFC1F) == 0xD61F0000)     # br xN

    for p in range(off, max(0, off - 0x4000), -4):
        if is_prologue(word(p)) and is_end(word(p - 4)):
            return p
    return None


def find_hooks(binary: str) -> tuple[int, list[int]]:
    """找 WCDB 设置数据库密码的函数入口。
    x86 版：LEA RDI,[全局名字对象] + LEA RSI,[字符串] 初始化，再找 LEA RSI,[全局名字对象] 所在的函数。
    arm64：x0=字符串、x8（经 GOT）=全局名字对象做初始化；再找把这个 GOT 槽读进 x1 的函数，断在它入口"""
    e_type, secs = load_sections(binary)
    ro_addr, ro = secs[".rodata"]
    tx_addr, tx = secs[".text"]
    adds, gots = adrp_refs(tx_addr, tx)
    out, start = [], 0
    while (pos := ro.find(ANCHOR, start)) != -1:
        start = pos + 1
        for off, _reg in adds.get(ro_addr + pos, []):
            slots = [slot for slot, lst in gots.items() if any(abs(o - off) <= 16 and r == 8 for o, r in lst)]
            for slot in slots:
                for off2, reg2 in gots[slot]:
                    if reg2 == 1 and abs(off2 - off) > 16:
                        fs = func_start(tx, off2)
                        if fs is not None and tx_addr + fs not in out:
                            out.append(tx_addr + fs)
    return e_type, sorted(out)


# 抓到一个像样的 32 字节候选就让 gdb 停下（stop 返回 True），断点随之拆除，脚本再 detach、quit。
# 不能直接杀 gdb：微信还在跑时断点指令留在内存里，之后命中会让微信崩溃。候选只写文件，不打印。
GDB_TEMPLATE = """set pagination off
set confirm off
handle SIGINT nopass
attach {pid}
python
import gdb, os
OUT = {out!r}
M = 0xFFFFFFFFFFFFFFFF
def _save(raw):
    fd = os.open(OUT, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.write(fd, (raw.hex() + "\\n").encode())
    os.close(fd)
class Cap(gdb.Breakpoint):
    def stop(self):
        inf = gdb.selected_inferior()
        try:
            x1 = int(gdb.parse_and_eval("$x1")) & M
            x2 = int(gdb.parse_and_eval("$x2")) & M
            raw = None
            if x1 and x2 == 32:
                raw = inf.read_memory(x1, 32).tobytes()
            elif x1:
                size = int.from_bytes(inf.read_memory(x1 + 16, 8).tobytes(), "little")
                ptr = int.from_bytes(inf.read_memory(x1 + 8, 8).tobytes(), "little")
                if size == 32 and ptr:
                    raw = inf.read_memory(ptr, 32).tobytes()
            if raw and len(set(raw)) >= 15:
                _save(raw)
                print("CAPTURED at " + hex(self.location_addr), flush=True)
                return True
        except Exception as e:
            print("ERR=" + str(e), flush=True)
        return False
for a in {addrs}:
    b = Cap("*" + hex(a))
    b.location_addr = a
end
continue
detach
quit
"""


def db_page1s(files_root: str) -> list[bytes]:
    out = []
    for path in glob.glob(os.path.join(files_root, "*", "db_storage", "**", "*.db"), recursive=True):
        with open(path, "rb") as f:
            p1 = f.read(PAGE)
        if len(p1) == PAGE and not p1.startswith(b"SQLite format 3"):
            out.append(p1)
    return out


def _give_to_wechat_user(store_path: str, files_root: str) -> None:
    """keys.json 由 root 写出，连接器以微信同一个用户运行，要能读写它（新库的密钥会追加进去）"""
    st = os.stat(files_root)
    for path in (os.path.dirname(store_path), store_path):
        os.chown(path, st.st_uid, st.st_gid)
    os.chmod(os.path.dirname(store_path), 0o700)


def verify_and_save(candidates: list[str], store_path: str, files_root: str) -> bool:
    page1s = db_page1s(files_root)
    salts = {p[:SALT_SZ]: p for p in page1s}
    for i, cand in enumerate(dict.fromkeys(candidates), 1):
        passphrase = bytes.fromhex(cand)
        keys = {}
        for salt, p1 in salts.items():
            key = derive_key(passphrase, salt)
            if verify_enc_key(key, p1):
                keys[salt.hex()] = key.hex()
        print(f"候选 {i}：校验通过 {len(keys)}/{len(salts)} 个库")
        if keys:
            store = KeyStore(store_path)
            store.passphrase = passphrase
            store.keys.update(keys)
            store.save()
            _give_to_wechat_user(store_path, files_root)
            print(f"已保存到 {store_path}")
            return True
    return False


def cmd_capture(args) -> None:
    pid = wechat_main_pid()
    binary = os.readlink(f"/proc/{pid}/exe")
    e_type, hooks = find_hooks(binary)
    base = min(int(l.split("-")[0], 16) for l in open(f"/proc/{pid}/maps") if l.rstrip().endswith(binary))
    if not hooks:
        sys.exit("没找到断点地址（微信版本可能变了）")
    addrs = [(base if e_type == 3 else 0) + h for h in hooks[:8]]
    os.makedirs(os.path.dirname(args.store), mode=0o700, exist_ok=True)
    cand_file = args.store + ".candidates"
    with tempfile.NamedTemporaryFile("w", suffix=".gdb", delete=False, dir=os.path.dirname(args.store)) as f:
        f.write(GDB_TEMPLATE.format(pid=pid, addrs=addrs, out=cand_file))
    proc = subprocess.Popen(["gdb", "-q", "-batch", "-x", f.name], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    timer = threading.Timer(args.timeout, lambda: proc.poll() is None and proc.send_signal(2))
    timer.start()
    print(f"pid={pid}，挂了 {len(addrs)} 个断点，现在扫码登录（最多等 {args.timeout} 秒）", flush=True)
    for line in proc.stdout:
        if line.startswith(("CAPTURED", "ERR=")):
            print(line.strip(), flush=True)
    proc.wait()
    timer.cancel()
    os.remove(f.name)
    print("gdb 已退出；登录完成后运行 verify 校验", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["capture", "verify"])
    ap.add_argument("--store", default="/config/wechat-connector/keys.json")
    ap.add_argument("--candidates", default=None)
    ap.add_argument("--files-root", default="/config/xwechat_files")
    ap.add_argument("--timeout", type=int, default=900)
    args = ap.parse_args()
    if args.cmd == "capture":
        cmd_capture(args)
    else:
        path = args.candidates or args.store + ".candidates"
        with open(path) as f:
            cands = [l.strip() for l in f if l.strip()]
        sys.exit(0 if verify_and_save(cands, args.store, args.files_root) else 1)


if __name__ == "__main__":
    main()
