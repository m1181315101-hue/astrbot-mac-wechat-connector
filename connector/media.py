"""AstrBot 发来的图片、语音 → 微信能发的临时文件

AstrBot 的 aiocqhttp 适配器把 Image / Record 都转成 base64://… 放进消息段，不用和 AstrBot 共享文件夹。
- 图片：GIF / PNG / JPEG 原样落盘；WEBP、BMP 转成 PNG。粘贴后 GIF 是动画表情，其余是图片
- 语音：wav 用 lame 转成 mp3，mp3 原样用；粘贴后是音频文件（电脑版微信发不了「按住说话」的语音条）
临时文件放在 media_dir 下各自的子目录里，超过 10 分钟的自动清掉。
"""
import base64
import io
import os
import shutil
import subprocess
import tempfile
import time
import urllib.request

MAX_BYTES = 20 * 1024 * 1024


def load_data(file_val: str) -> bytes | None:
    """消息段里的 file 字段 → 原始字节；拿不到（比如只在 AstrBot 容器里存在的路径）就返回 None"""
    if not file_val:
        return None
    if file_val.startswith("base64://"):
        return base64.b64decode(file_val[9:])
    if file_val.startswith(("http://", "https://")):
        with urllib.request.urlopen(file_val, timeout=20) as r:
            data = r.read(MAX_BYTES + 1)
        return data if len(data) <= MAX_BYTES else None
    path = file_val[7:] if file_val.startswith("file://") else file_val
    if os.path.isfile(path) and os.path.getsize(path) <= MAX_BYTES:
        with open(path, "rb") as f:
            return f.read()
    return None


def image_ext(data: bytes) -> str:
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    if data[:2] == b"BM":
        return "bmp"
    return ""


def audio_kind(data: bytes) -> str:
    if data[:4] == b"RIFF" and data[8:12] == b"WAVE":
        return "wav"
    if data[:3] == b"ID3" or (len(data) > 1 and data[0] == 0xFF and (data[1] & 0xE0) == 0xE0):
        return "mp3"
    return ""


_MP3_KBPS = {  # MPEG-1 Layer III / MPEG-2(2.5) Layer III 的比特率表
    3: [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320],
    2: [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160],
}


def audio_seconds(data: bytes) -> float:
    """时长（秒）：wav 读文件头；mp3 按第一帧的比特率估算（够用来分「唱歌」和「说一句话」）"""
    kind = audio_kind(data)
    if kind == "wav":
        import wave
        try:
            with wave.open(io.BytesIO(data)) as w:
                return w.getnframes() / float(w.getframerate() or 1)
        except Exception:
            return 0.0
    if kind == "mp3":
        pos = 0
        if data[:3] == b"ID3" and len(data) >= 10:  # 跳过 ID3v2 标签
            pos = 10 + ((data[6] << 21) | (data[7] << 14) | (data[8] << 7) | data[9])
        while pos + 4 <= len(data):
            if data[pos] == 0xFF and (data[pos + 1] & 0xE0) == 0xE0:
                version = (data[pos + 1] >> 3) & 3  # 3 = MPEG-1
                kbps = _MP3_KBPS[3 if version == 3 else 2][(data[pos + 2] >> 4) & 0xF] if (data[pos + 2] >> 4) < 15 else 0
                if kbps:
                    return (len(data) - pos) * 8 / (kbps * 1000)
            pos += 1
    return 0.0


class MediaDir:
    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, mode=0o700, exist_ok=True)

    def _cleanup(self) -> None:
        cutoff = time.time() - 600
        for name in os.listdir(self.root):
            path = os.path.join(self.root, name)
            if os.path.isdir(path) and os.path.getmtime(path) < cutoff:
                shutil.rmtree(path, ignore_errors=True)

    def _new_dir(self) -> str:
        self._cleanup()
        return tempfile.mkdtemp(dir=self.root)

    def image_file(self, data: bytes) -> str | None:
        ext = image_ext(data)
        if not ext:
            return None
        if ext in ("webp", "bmp"):
            from PIL import Image  # 只在要转格式时才用
            buf = io.BytesIO()
            Image.open(io.BytesIO(data)).save(buf, "PNG")
            data, ext = buf.getvalue(), "png"
        path = os.path.join(self._new_dir(), f"image.{ext}")
        with open(path, "wb") as f:
            f.write(data)
        return path

    def voice_file(self, data: bytes, name: str) -> str | None:
        kind = audio_kind(data)
        if not kind:
            return None
        d = self._new_dir()
        out = os.path.join(d, name)
        if kind == "mp3":
            with open(out, "wb") as f:
                f.write(data)
            return out
        src = os.path.join(d, "in.wav")
        with open(src, "wb") as f:
            f.write(data)
        r = subprocess.run(["lame", "--quiet", "-b", "64", src, out], capture_output=True, timeout=60)
        os.remove(src)
        return out if r.returncode == 0 and os.path.isfile(out) else None
