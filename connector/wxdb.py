"""微信 4.x（Linux）本地数据库：密钥、增量解密、读新消息和联系人

数据库是 WCDB / SQLCipher 4（AES-256-CBC + HMAC-SHA512，每页 4096 字节，末尾保留 80 字节）。
每个库有自己的 16 字节 salt，库密钥 = PBKDF2-HMAC-SHA512(passphrase, salt, 256000)。
passphrase 由 wxkey.py 在扫码登录时抓取，存在 keys.json（600）里。

解密结果只放在内存里（sqlite3 deserialize），不落盘。
"""
import ctypes
import ctypes.util
import glob
import hashlib
import hmac
import json
import os
import re
import sqlite3
import struct
import threading

PAGE = 4096
KEY_SZ = 32
SALT_SZ = 16
RESERVE = 80  # IV(16) + HMAC(64)
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


# ---------- 加解密 ----------

class _Lib:
    crypto = None
    zstd = None


def _crypto():
    if _Lib.crypto is None:
        lib = ctypes.CDLL(ctypes.util.find_library("crypto") or "libcrypto.so.3")
        lib.EVP_CIPHER_CTX_new.restype = ctypes.c_void_p
        lib.EVP_aes_256_cbc.restype = ctypes.c_void_p
        lib.EVP_DecryptInit_ex.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
                                           ctypes.c_char_p, ctypes.c_char_p]
        lib.EVP_CIPHER_CTX_set_padding.argtypes = [ctypes.c_void_p, ctypes.c_int]
        lib.EVP_DecryptUpdate.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_int),
                                          ctypes.c_char_p, ctypes.c_int]
        lib.EVP_CIPHER_CTX_free.argtypes = [ctypes.c_void_p]
        _Lib.crypto = lib
    return _Lib.crypto


class _Decryptor:
    """复用同一个 EVP 上下文逐页解密"""

    def __init__(self, key: bytes):
        self.lib = _crypto()
        self.key = key
        self.ctx = self.lib.EVP_CIPHER_CTX_new()
        self.buf = ctypes.create_string_buffer(PAGE + 32)
        self.n = ctypes.c_int(0)

    def page(self, raw: bytes, pgno: int) -> bytes:
        off = SALT_SZ if pgno == 1 else 0
        iv = raw[PAGE - RESERVE: PAGE - RESERVE + 16]
        data = raw[off: PAGE - RESERVE]
        self.lib.EVP_DecryptInit_ex(self.ctx, self.lib.EVP_aes_256_cbc(), None, self.key, iv)
        self.lib.EVP_CIPHER_CTX_set_padding(self.ctx, 0)
        self.lib.EVP_DecryptUpdate(self.ctx, self.buf, ctypes.byref(self.n), data, len(data))
        body = self.buf.raw[:self.n.value]
        if pgno == 1:
            head = bytearray(b"SQLite format 3\0" + body[:84])
            head[18] = head[19] = 1  # WAL 改成传统日志模式，内存库才打得开
            body = bytes(head) + body[84:]
        return body + raw[PAGE - RESERVE:]

    def close(self):
        if self.ctx:
            self.lib.EVP_CIPHER_CTX_free(self.ctx)
            self.ctx = None


def verify_enc_key(enc_key: bytes, page1: bytes) -> bool:
    salt = page1[:SALT_SZ]
    mac_key = hashlib.pbkdf2_hmac("sha512", enc_key, bytes(b ^ 0x3A for b in salt), 2, dklen=KEY_SZ)
    hm = hmac.new(mac_key, page1[SALT_SZ: PAGE - RESERVE + 16], hashlib.sha512)
    hm.update(struct.pack("<I", 1))
    return hm.digest() == page1[PAGE - 64:]


def derive_key(passphrase: bytes, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha512", passphrase, salt, 256000, dklen=KEY_SZ)


def zstd_decompress(data: bytes) -> bytes:
    if _Lib.zstd is None:
        lib = ctypes.CDLL(ctypes.util.find_library("zstd") or "libzstd.so.1")
        lib.ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong
        lib.ZSTD_getFrameContentSize.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
        lib.ZSTD_decompress.restype = ctypes.c_size_t
        lib.ZSTD_decompress.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t]
        lib.ZSTD_isError.argtypes = [ctypes.c_size_t]
        _Lib.zstd = lib
    lib = _Lib.zstd
    size = lib.ZSTD_getFrameContentSize(data, len(data))
    if size >= (1 << 63) or size > 64 * 1024 * 1024:  # 未知或出错
        size = 1024 * 1024
    out = ctypes.create_string_buffer(int(size) or 1)
    n = lib.ZSTD_decompress(out, len(out), data, len(data))
    if lib.ZSTD_isError(n):
        raise ValueError("zstd 解压失败")
    return out.raw[:n]


def decode_content(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    raw = bytes(value)
    if raw.startswith(ZSTD_MAGIC):
        raw = zstd_decompress(raw)
    return raw.decode("utf-8", errors="replace")


# ---------- 密钥 ----------

class KeyStore:
    """keys.json：{"passphrase": hex, "keys": {salt_hex: key_hex}}，权限 600"""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()
        self.passphrase = None
        self.keys: dict[str, str] = {}
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
            self.passphrase = bytes.fromhex(data["passphrase"]) if data.get("passphrase") else None
            self.keys = dict(data.get("keys") or {})

    def save(self) -> None:
        tmp = self.path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump({"passphrase": self.passphrase.hex() if self.passphrase else None,
                       "keys": self.keys}, f)
        os.replace(tmp, self.path)

    def key_for(self, page1: bytes) -> bytes | None:
        salt = page1[:SALT_SZ]
        with self.lock:
            known = self.keys.get(salt.hex())
            if known:
                return bytes.fromhex(known)
            if not self.passphrase:
                return None
            key = derive_key(self.passphrase, salt)
            if not verify_enc_key(key, page1):
                return None
            self.keys[salt.hex()] = key.hex()
            self.save()
            return key


# ---------- 一个库的明文镜像 ----------

class PlainDB:
    """主库整份解密成基底，WAL 里已提交的帧增量叠加，用 sqlite3 deserialize 在内存里打开"""

    def __init__(self, path: str, keystore: KeyStore):
        self.path = path
        self.keystore = keystore
        self.key = None
        self.con = None
        self._main_sig = None
        self._base = None
        self._wal_salts = None
        self._wal_offset = 32
        self._overlay: dict[int, bytes] = {}
        self._wal_sig = None

    @staticmethod
    def _sig(path: str):
        try:
            st = os.stat(path)
            return st.st_mtime_ns, st.st_size
        except FileNotFoundError:
            return None

    def refresh(self) -> bool:
        """有变化就重建内存库，返回 True；密钥不可用时抛 LookupError"""
        main_sig = self._sig(self.path)
        wal_sig = self._sig(self.path + "-wal")
        if main_sig is None:
            return False
        if main_sig == self._main_sig and wal_sig == self._wal_sig and self.con is not None:
            return False
        with open(self.path, "rb") as f:
            raw = f.read()
        if len(raw) < PAGE:
            return False
        if self.key is None:
            self.key = self.keystore.key_for(raw[:PAGE])
            if self.key is None:
                raise LookupError(f"没有 {os.path.basename(self.path)} 的密钥")
        dec = _Decryptor(self.key)
        try:
            if main_sig != self._main_sig:
                pages = len(raw) // PAGE
                self._base = bytearray(b"".join(dec.page(raw[i * PAGE:(i + 1) * PAGE], i + 1)
                                                for i in range(pages)))
                self._main_sig = main_sig
                self._wal_salts, self._wal_offset, self._overlay = None, 32, {}
            self._read_wal(dec)
        finally:
            dec.close()
        self._wal_sig = wal_sig
        data = bytearray(self._base)
        for pgno, page in self._overlay.items():
            start = (pgno - 1) * PAGE
            if start > len(data):
                data.extend(b"\0" * (start - len(data)))
            data[start:start + PAGE] = page
        con = sqlite3.connect(":memory:", check_same_thread=False)
        con.deserialize(bytes(data))
        if self.con is not None:
            self.con.close()
        self.con = con
        return True

    def _read_wal(self, dec: _Decryptor) -> None:
        wal = self.path + "-wal"
        if not os.path.exists(wal):
            self._wal_salts, self._wal_offset, self._overlay = None, 32, {}
            return
        with open(wal, "rb") as f:
            w = f.read()
        if len(w) < 32:
            return
        salts = struct.unpack_from(">II", w, 16)
        if salts != self._wal_salts:  # WAL 重置过：从头来
            self._wal_salts, self._wal_offset, self._overlay = salts, 32, {}
        pos, pending = self._wal_offset, {}
        while pos + 24 + PAGE <= len(w):
            pgno, commit, s1, s2 = struct.unpack_from(">IIII", w, pos)
            if (s1, s2) != salts or pgno == 0:
                break
            pending[pgno] = dec.page(w[pos + 24: pos + 24 + PAGE], pgno)
            pos += 24 + PAGE
            if commit:  # 事务提交帧：之前的帧才算数
                self._overlay.update(pending)
                pending = {}
                self._wal_offset = pos


# ---------- 账号：消息和联系人 ----------

GROUP_PREFIX = re.compile(r"^[A-Za-z0-9_\-@.]+:\n")
SYSTEM_IDS = {"filehelper", "weixin", "fmessage", "medianote", "floatbottle", "newsapp",
              "notifymessage", "brandsessionholder", "qqmail", "tmessage", "qmessage"}


class WeChatStore:
    def __init__(self, files_root: str, keystore: KeyStore):
        dirs = [d for d in glob.glob(os.path.join(files_root, "*")) if os.path.isdir(os.path.join(d, "db_storage"))]
        if not dirs:
            raise FileNotFoundError(f"{files_root} 下没有已登录的账号（db_storage）")
        self.acct_dir = max(dirs, key=lambda d: os.path.getmtime(os.path.join(d, "db_storage")))
        self.self_wxid = re.sub(r"_[0-9a-f]{4}$", "", os.path.basename(self.acct_dir))
        self.keystore = keystore
        self.msg_dbs: dict[str, PlainDB] = {}
        self.contact_db = PlainDB(os.path.join(self.acct_dir, "db_storage", "contact", "contact.db"), keystore)
        self._contacts: dict[str, str] = {}
        self._contacts_sig = None
        self._scan_shards()

    def _scan_shards(self) -> None:
        pattern = os.path.join(self.acct_dir, "db_storage", "message", "message_*.db")
        for path in sorted(glob.glob(pattern)):
            if re.search(r"message_\d+\.db$", path) and path not in self.msg_dbs:
                self.msg_dbs[path] = PlainDB(path, self.keystore)

    def refresh(self) -> bool:
        self._scan_shards()
        changed = False
        for db in self.msg_dbs.values():
            changed |= db.refresh()
        return changed

    # 联系人：备注名优先，没有就用昵称
    def contacts(self) -> dict[str, str]:
        self.contact_db.refresh()
        if self._contacts_sig is not self.contact_db.con:
            rows = self.contact_db.con.execute("select username, remark, nick_name from contact").fetchall()
            self._contacts = {u: (r or n or u) for u, r, n in rows if u}
            self._contacts_sig = self.contact_db.con
        return self._contacts

    def self_name(self) -> str:
        return self.contacts().get(self.self_wxid, "")

    def display_name(self, wxid: str) -> str:
        return self.contacts().get(wxid, wxid)

    def name_is_unique(self, wxid: str) -> bool:
        """显示名在整个通讯录（含群、公众号）里只有它一个：切换会话时才不会点错"""
        name = self.display_name(wxid)
        return sum(1 for v in self.contacts().values() if v == name) == 1

    def find_by_name(self, name: str) -> list[str]:
        return [u for u, v in self.contacts().items() if v == name]

    @staticmethod
    def table_of(wxid: str) -> str:
        return "Msg_" + hashlib.md5(wxid.encode()).hexdigest()

    def _self_rowid(self, con) -> int | None:
        row = con.execute("select rowid from Name2Id where user_name = ?", (self.self_wxid,)).fetchone()
        return row[0] if row else None

    def latest_ids(self, wxid: str) -> dict[str, int]:
        table, out = self.table_of(wxid), {}
        for path, db in self.msg_dbs.items():
            if db.con is None:
                continue
            if db.con.execute("select 1 from sqlite_master where name = ?", (table,)).fetchone():
                out[os.path.basename(path)] = db.con.execute(f"select ifnull(max(local_id), 0) from {table}").fetchone()[0]
        return out

    def new_messages(self, wxid: str, marks: dict[str, int]) -> list[dict]:
        """marks：{库文件名: 已处理到的 local_id}，原地更新；返回对方发来的新消息（含非文字，type 区分）。
        调用方启动时先用 latest_ids 把已有会话的水位设到最新（不回放历史）；
        之后才出现的会话表（第一次聊天、新分库）不在 marks 里，从头读。"""
        table, out = self.table_of(wxid), []
        for path, db in self.msg_dbs.items():
            con, name = db.con, os.path.basename(path)
            if con is None or not con.execute("select 1 from sqlite_master where name = ?", (table,)).fetchone():
                continue
            me = self._self_rowid(con)
            names = dict(con.execute("select rowid, user_name from Name2Id"))
            rows = con.execute(f"select local_id, server_id, local_type, create_time, real_sender_id, message_content "
                               f"from {table} where local_id > ? order by local_id", (marks.get(name, 0),)).fetchall()
            for local_id, server_id, local_type, create_time, sender, content in rows:
                marks[name] = local_id
                if sender == me:
                    continue
                base_type = local_type & 0xFFFFFFFF  # 高 32 位是子类型（链接、文件等）
                text = decode_content(content) if base_type == 1 else ""
                if wxid.endswith("@chatroom"):  # 群里别人发的文字带「发送者wxid:换行」前缀
                    text = GROUP_PREFIX.sub("", text, count=1)
                out.append({"local_id": local_id, "server_id": server_id, "type": base_type, "time": create_time,
                            "sender": names.get(sender, ""), "text": text})
        return out

    def sent_since(self, wxid: str, since: int, text: str) -> bool:
        """发送后确认：这个会话里 since 之后有没有一条自己发出的、内容相同的文字"""
        table = self.table_of(wxid)
        for db in self.msg_dbs.values():
            con = db.con
            if con is None or not con.execute("select 1 from sqlite_master where name = ?", (table,)).fetchone():
                continue
            me = self._self_rowid(con)
            for sender, content in con.execute(f"select real_sender_id, message_content from {table} "
                                               f"where create_time >= ? and local_type = 1", (since,)):
                if sender == me and decode_content(content).strip() == text.strip():
                    return True
        return False
