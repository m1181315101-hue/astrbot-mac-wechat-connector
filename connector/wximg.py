"""收照片：微信 4.x 把聊天图片加密存成 .dat，这里推出图片密钥、解密、缩小

文件在 <账号目录>/msg/attach/<md5(会话 wxid)>/<年-月>/Img/<名字>[_h|_t].dat：不带后缀是原图，_h 高清，_t 缩略图；
名字记在消息的 packed_info_data 里。别人发的原图要微信自动下载完才有，没下的只有缩略图。

V2 格式（开头 07 08 56 32 08 07）：15 字节头（6 字节标识 + AES 段长度 + 异或段长度，小端），
AES 段补齐到 16 的倍数、AES-128-ECB 加密、PKCS7 填充；异或段每个字节异或同一个数；两段中间原样。
解法参考 hicccc77/WeFlow 的 decryptDatV4WithJs。

密钥不用扫内存：<家目录>/.xwechat/net/kvcomm/ 下有 key_<数字>_….statistic，
AES 密钥 = md5(数字 + 微信号) 的十六进制前 16 位，异或字节 = 数字 & 0xFF（参考 r266-tech/wxkey 的 imgkey.go，MIT）。
推出来以后拿几张真图片核对，只放在内存里，不落盘、不打日志。
"""
import glob
import hashlib
import io
import os
import re
import struct

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

V2 = b"\x07\x08V2\x08\x07"
MAGIC = (b"\xff\xd8\xff", b"\x89PNG", b"GIF8", b"RIFF", b"BM")
WXGF = b"wxgf"  # 微信自家的压缩格式（里面是 HEVC），密钥对也能解出这个头，但 AstrBot 认不了，这种就换缩略图


def _head_ok(key: bytes, data: bytes) -> bool:
    if data[:6] != V2 or len(data) < 31:
        return False
    return Cipher(algorithms.AES(key), modes.ECB()).decryptor().update(data[15:31]).startswith(MAGIC + (WXGF,))


def find_key(kvcomm_dir: str, acct_dir: str, samples: int = 5) -> tuple[bytes, int] | None:
    """推出 (AES 密钥, 异或字节)，最近几张 V2 图片都能解开才算；推不出返回 None"""
    codes = sorted({int(m.group(1)) for f in glob.glob(os.path.join(kvcomm_dir, "key_*"))
                    if (m := re.match(r"key_(\d+)_", os.path.basename(f)))})
    leaf = os.path.basename(acct_dir.rstrip("/"))
    wxids = list(dict.fromkeys([re.sub(r"_[0-9a-f]{4}$", "", leaf), leaf]))  # 账号目录是「微信号_4 位」
    files = sorted(glob.glob(os.path.join(acct_dir, "msg", "attach", "*", "*", "Img", "*.dat")),
                   key=os.path.getmtime, reverse=True)
    heads = []
    for f in files:
        with open(f, "rb") as fh:
            head = fh.read(31)
        if head[:6] == V2:
            heads.append(head)
            if len(heads) >= samples:
                break
    if not codes or not heads:
        return None
    for code in codes:
        for wxid in wxids:
            key = hashlib.md5(f"{code}{wxid}".encode()).hexdigest()[:16].encode()
            if all(_head_ok(key, h) for h in heads):
                return key, code & 0xFF
    return None


def decrypt(data: bytes, key: bytes, xor: int) -> bytes | None:
    """V2 的 .dat → 图片原始字节；格式不对、解出来不是 AstrBot 认得的图片（比如 wxgf）返回 None"""
    if data[:6] != V2 or len(data) < 15:
        return None
    aes_size, xor_size = struct.unpack_from("<II", data, 6)
    payload = data[15:]
    aligned = aes_size + (16 - aes_size % 16)
    if aligned > len(payload) or xor_size > len(payload) - aligned:
        return None
    d = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
    head = d.update(payload[:aligned]) + d.finalize()
    pad = head[-1]
    if not 1 <= pad <= 16 or head[-pad:] != bytes([pad]) * pad:
        return None
    rest = payload[aligned:]
    cut = len(rest) - xor_size
    out = head[:-pad] + rest[:cut] + rest[cut:].translate(bytes(i ^ xor for i in range(256)))
    return out if out.startswith(MAGIC) else None


def shrink(data: bytes, max_side: int = 1600, max_bytes: int = 1536 * 1024) -> bytes:
    """太大的照片缩到长边 max_side、存成 JPEG，省得一张图把消息撑到十几 MB；GIF 和本来就不大的原样返回"""
    if data[:4] == b"GIF8":
        return data
    from PIL import Image  # 只有缩图时才用
    with Image.open(io.BytesIO(data)) as im:
        if len(data) <= max_bytes and max(im.size) <= max_side:
            return data
        im = im.convert("RGB")
        im.thumbnail((max_side, max_side))
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=85)
        return buf.getvalue()
