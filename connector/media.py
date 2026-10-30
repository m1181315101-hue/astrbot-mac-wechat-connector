"""AstrBot 发来的图片、语音 → 微信能发的临时文件

AstrBot 的 aiocqhttp 适配器把 Image / Record 都转成 base64://… 放进消息段，不用和 AstrBot 共享文件夹。
- 图片：PNG / JPEG 原样落盘；WEBP、BMP 转成 PNG。GIF 原样粘贴时，1MB 以内微信存成表情（47，和表情面板里的
  一样会动、按表情大小显示），超过就存成动画表情附件（49, 8），对方有时收到的是一个 .gif 文件（实测：
  0.99MB 是表情，1.13MB 就成了附件）。所以超过 gif_emoji_max_bytes 的先缩尺寸、抽帧压到上限以内，
  压不下来才取中间一帧转成 PNG；send_gif_as_still 打开时一律取一帧
- 语音：wav 用 lame 转成 mp3，mp3 原样用；粘贴后是音频文件（电脑版微信发不了「按住说话」的语音条）
临时文件放在 media_dir 下各自的子目录里，超过 10 分钟的自动清掉。
"""
import base64
import hashlib
import io
import os
import shutil
import subprocess
import tempfile
import time
import urllib.request

MAX_BYTES = 20 * 1024 * 1024
GIF_EMOJI_MAX_BYTES = 1_000_000  # 微信把粘贴的 GIF 当表情的上限（实测 0.99MB 可以、1.13MB 不行）


def shrink_gif(data: bytes, limit: int) -> bytes | None:
    """把动图压到 limit 字节以内：先缩尺寸（长边 360 → 240 → 160），还不够再隔帧抽掉（时长加倍，速度不变）。
    压不下来返回 None"""
    from PIL import Image
    with Image.open(io.BytesIO(data)) as im:
        frames, durations = [], []
        for i in range(getattr(im, "n_frames", 1)):
            im.seek(i)
            frames.append(im.convert("RGBA"))
            durations.append(im.info.get("duration", 100) or 100)
        loop = im.info.get("loop", 0)
    for step in (1, 2, 3):
        picked = frames[::step]
        times = [sum(durations[i:i + step]) for i in range(0, len(durations), step)]
        for side in (360, 240, 160):
            out = io.BytesIO()
            scaled = []
            for f in picked:
                f = f.copy()
                f.thumbnail((side, side))
                scaled.append(f)
            scaled[0].save(out, "GIF", save_all=True, append_images=scaled[1:], duration=times, loop=loop,
                           disposal=2, optimize=True)
            if out.tell() <= limit:
                return out.getvalue()
    return None


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


def fetch_sticker(url: str, md5: str, max_bytes: int) -> bytes | None:
    """表情包原图：按消息里的 cdnurl 下载，超过 max_bytes、md5 对不上或者认不出是图片就返回 None"""
    with urllib.request.urlopen(url, timeout=15) as r:
        data = r.read(max_bytes + 1)
    if len(data) > max_bytes or not image_ext(data):
        return None
    if md5 and hashlib.md5(data).hexdigest() != md5:
        return None
    return data


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

    def image_file(self, data: bytes, still_gif: bool = False, gif_max: int = GIF_EMOJI_MAX_BYTES) -> str | None:
        """GIF 在 gif_max 以内原样发（微信当表情），超过先压缩，压不下来或者 still_gif 才取一帧存成 PNG"""
        ext = image_ext(data)
        if not ext:
            return None
        if ext == "gif" and not still_gif and len(data) > gif_max:
            try:
                smaller = shrink_gif(data, gif_max)
            except Exception:  # noqa: BLE001 - 压缩失败就退回取一帧
                smaller = None
            if smaller:
                data = smaller
            else:
                still_gif = True
        if ext in ("webp", "bmp") or (ext == "gif" and still_gif):
            from PIL import Image  # 只在要转格式时才用
            buf = io.BytesIO()
            with Image.open(io.BytesIO(data)) as im:
                if ext == "gif" and getattr(im, "n_frames", 1) > 1:
                    im.seek(im.n_frames // 2)  # 中间那帧：有些动图第一帧是空的
                im.convert("RGBA").save(buf, "PNG")
            data, ext = buf.getvalue(), "png"
        path = os.path.join(self._new_dir(), f"image.{ext}")
        with open(path, "wb") as f:
            f.write(data)
        return path

    def voice_segments(self, data: bytes, max_seconds: float = 55.0) -> list[tuple[str, float]]:
        """语音条用：转成 wav（mp3 用 lame 解码），超过 max_seconds 的切成几段（微信语音条最长 60 秒）。
        返回 [(wav 路径, 秒数)]；不认识的格式返回空列表"""
        import wave
        kind = audio_kind(data)
        if not kind:
            return []
        d = self._new_dir()
        src = os.path.join(d, "in.wav")
        if kind == "mp3":
            mp3 = os.path.join(d, "in.mp3")
            with open(mp3, "wb") as f:
                f.write(data)
            r = subprocess.run(["lame", "--quiet", "--decode", mp3, src], capture_output=True, timeout=60)
            os.remove(mp3)
            if r.returncode != 0:
                return []
        else:
            with open(src, "wb") as f:
                f.write(data)
        try:
            with wave.open(src) as w:
                params, rate, total = w.getparams(), w.getframerate(), w.getnframes()
                if total / rate <= max_seconds:
                    return [(src, total / rate)]
                parts, n, per = [], 0, int(max_seconds * rate)
                while n < total:
                    frames = w.readframes(min(per, total - n))
                    out = os.path.join(d, f"part{len(parts) + 1}.wav")
                    with wave.open(out, "wb") as o:
                        o.setparams(params)
                        o.writeframes(frames)
                    parts.append((out, min(per, total - n) / rate))
                    n += per
            os.remove(src)
            return parts
        except (wave.Error, EOFError):
            return []

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
