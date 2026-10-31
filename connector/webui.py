"""状态页：微信进程和窗口、数据库密钥、AstrBot 连接、发送队列、收消息计数、运行日志；
扫码登录（登录窗口截图）、暂停发送、发测试消息、改常用设置和 AstrBot 地址口令、重启连接器

监听容器里的 127.0.0.1:6299（webui_host / webui_port），只有同一个容器里的 nginx 能访问到；
nginx 把网页桌面（3001 端口，有密码）的 /wxc/ 转过来，所以打开 https://localhost:3001/wxc/ 就能看，密码和网页桌面一样。
页面左边是导航（窄屏收进菜单），分「概览 / 登录 / 设置 / 日志」四页；首次配置（还没有 config.json 或 keys.json）时只有「首次配置 / 日志」。
不动微信窗口：查窗口用界面驱动只读的 status / login，不用会把窗口拉出来的 ping。
截图只截登录窗口：认不出是登录窗口、或者旁边开着别的微信窗口就不截，所以页面上不会出现聊天内容。
状态和日志里没有消息内容和令牌，会话只用 OneBot 号表示；「设置」里的白名单会显示 wxid 和备注名（改白名单要用）。
AstrBot 的口令只写不读：哪个接口都不返回它，页面上只显示设没设置。

  GET  /               页面（每 5 秒刷新一次状态）
  GET  /api/status     状态 JSON，也可以给脚本拿来做掉线提醒
  GET  /api/log?lines= connector.log 最后几行（默认 200，最多 2000）
  GET  /api/settings   能在页面上改的设置（口令只给设没设置）
  GET  /api/lookup?q=  按名字查好友和群（填白名单用）
  GET  /api/login      登录窗口现在什么状态（扫码 / 要在手机上确认 / 断网……）
  GET  /api/login/qr   登录窗口截图（PNG）；不是登录窗口时 409
  POST /api/settings   保存设置（写回 config.json，留一份 .bak），然后重启连接器
  POST /api/stop       {"stop": true/false}：放 / 删 STOP 文件
  POST /api/test       给文件传输助手发一条固定的测试消息（排进发送队列）
  POST /api/restart    重启连接器（退出后 s6 拉起来）；正在发消息时不重启
POST 要带页面里的随机口令（X-WXC-Token，连接器每次启动换一个）和 Content-Type: application/json：
别的网页提交不了表单，跨域 fetch 要先 OPTIONS 预检，这里一律 405、不给 CORS 头。
"""
import asyncio
import base64
import json
import logging
import os
import re
import secrets
import time
from urllib.parse import parse_qs, urlparse

log = logging.getLogger("wechat-connector")
WINDOW_CHECK_SECONDS = 30  # 查窗口要占用界面驱动，最多 30 秒一次，正在发消息时不查
QUIET_WARN_SECONDS = 6 * 3600  # 这么久数据库都没变化，多半是掉线了（夜里群也安静的话可能误报）
MAX_BODY = 64 * 1024
# 页面上能改的设置：其余的（端口、目录这些）只能改 config.json
TOGGLES = {"receive_only": "只收不发", "receive_stickers": "收表情包", "group_stickers": "群里的表情包也收",
           "receive_photos": "收照片", "group_photos": "群里的照片也收", "receive_records": "收聊天记录",
           "receive_pats": "收拍一拍", "reply_mention_when_busy": "群里回复前面插了别人的话时加 @"}
NUMBERS = {"max_sends_per_minute": ("每分钟最多发几条", 1, 60), "max_message_age": ("多少秒以前的消息不收", 30, 86400),
           "photo_wait_seconds": ("等原图最多几秒", 0, 30)}
CHOICES = {"voice_mode": ("语音怎么发", {"native": "原生语音条", "file": "mp3 文件"})}
LISTS = {"whitelist": "私聊白名单（一行一个 wxid，* 是所有好友）", "group_whitelist": "群白名单（一行一个，以 @chatroom 结尾）",
         "bot_names": "群里叫小号的其他名字（一行一个）"}
SETUP_KEYS = ("astrbot_ws_url", "access_token", "receive_only")  # 首次配置时页面上只能写这几项


def wechat_running() -> bool:
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            if os.path.basename(os.readlink(f"/proc/{d}/exe")) == "wechat":
                return True
        except OSError:
            continue
    return False


def ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} 秒"
    if seconds < 3600:
        return f"{seconds // 60} 分钟"
    if seconds < 86400:
        return f"{seconds // 3600} 小时 {seconds % 3600 // 60} 分钟"
    return f"{seconds // 86400} 天 {seconds % 86400 // 3600} 小时"


def safe_url(url: str) -> tuple[str, bool]:
    """给页面看的 AstrBot 地址：去掉可能带口令的部分（user:pass@、?参数、#），返回 (地址, 去掉过没有)"""
    u = urlparse(url)
    if not (u.query or u.fragment or "@" in u.netloc):
        return url, False
    return f"{u.scheme}://{u.netloc.rsplit('@', 1)[-1]}{u.path}", True


async def ui_request(c, req: dict, timeout: float = 20.0) -> dict:
    """问界面驱动：最多等 2 秒轮到界面（正在发消息就不等了）；首次配置时微信还没起来就没有驱动"""
    ui = c.ui
    if ui is None:
        return {"ok": False, "reason": "微信没在运行"}
    return await asyncio.to_thread(ui.request, req, timeout, 2.0)


class WindowProbe:
    def __init__(self):
        self.result: dict = {}
        self.checked = 0.0

    async def get(self, c) -> dict:
        if time.time() - self.checked < WINDOW_CHECK_SECONDS:
            return self.result
        if c.sending or c.send_queue.qsize():
            return {**self.result, "note": "正在发消息，这次没查窗口"}
        self.checked = time.time()
        resp = await ui_request(c, {"op": "status"})
        if resp.get("busy"):  # 正好轮到发消息：这次不算，下次再查
            self.checked = 0
            return {**self.result, "note": "正在发消息，这次没查窗口"}
        self.result = {"ok": bool(resp.get("ok")), "frame": bool(resp.get("frame")),
                       "width": int(resp.get("width") or 0), "height": int(resp.get("height") or 0),
                       "login": bool(resp.get("login")), "login_text": str(resp.get("login_text") or ""),
                       "reason": str(resp.get("reason") or ""), "checked_at": self.checked}
        return self.result


def window_check(w: dict) -> tuple[str, str]:
    if "ok" not in w:  # 还没查过（刚启动就赶上发消息时，只有一句 note）
        return "warn", "还没查过"
    if not w["ok"]:
        return "bad", f"界面驱动不可用：{w['reason']}"
    if not w["frame"]:
        return "bad", "找不到微信窗口：微信没开，或者退出登录了"
    if w["login"]:
        return "bad", f"停在登录窗口（{w['login_text']}），去「登录」页扫码"
    if w["width"] == 0:
        return "ok", "主窗口收在托盘里（发消息时会自动打开）"
    if w["width"] < 500:
        return "warn", f"窗口只有 {w['width']}×{w['height']}，可能是登录窗口，去「登录」页看看"
    return "ok", f"主窗口 {w['width']}×{w['height']}"


class LoginView:
    """登录页：登录窗口的状态最多 2 秒问一次界面驱动，截图最多 1 秒一张"""

    def __init__(self, probe: WindowProbe):
        self.probe = probe
        self.info: dict = {}
        self.info_at = 0.0
        self.shot: tuple[bytes, str] = (b"", "")
        self.shot_at = 0.0
        self.lock = asyncio.Lock()

    async def state(self, c) -> dict:
        async with self.lock:
            if time.time() - self.info_at < 2:
                return self.info
            if not await asyncio.to_thread(wechat_running):
                info = {"running": False, "login": False, "text": "微信没在运行"}
            elif c.sending or c.send_queue.qsize():
                info = {"running": True, "login": False, "text": "已登录（正在发消息）"}
            else:
                resp = await ui_request(c, {"op": "login"})
                if resp.get("busy"):
                    info = {"running": True, "login": False, "text": "已登录（正在发消息）"}
                elif not resp.get("ok"):
                    info = {"running": True, "login": False, "text": f"界面驱动不可用：{resp.get('reason', '')}"}
                elif resp.get("login"):
                    info = {"running": True, "login": True, "state": str(resp.get("state") or ""),
                            "text": str(resp.get("text") or ""), "others": int(resp.get("others") or 0)}
                else:
                    info = {"running": True, "login": False, "text": "没看到登录窗口（已经登录了，或者微信还没起来）"}
            if info["login"] != self.info.get("login", info["login"]):
                self.probe.checked = 0  # 登录状态变了：概览页的窗口检查下次重新查
            self.info, self.info_at = info, time.time()
            return info

    async def png(self, c) -> tuple[bytes, str]:
        async with self.lock:
            if time.time() - self.shot_at < 1:
                return self.shot
            if c.sending or c.send_queue.qsize():
                shot = (b"", "正在发消息（已经登录了），不截图")
            elif not await asyncio.to_thread(wechat_running):
                shot = (b"", "微信没在运行")
            else:
                resp = await ui_request(c, {"op": "login", "grab": True})
                if resp.get("busy"):
                    resp = {"ok": False, "reason": "正在发消息（已经登录了），不截图"}
                try:
                    data = base64.b64decode(resp.get("png") or "") if resp.get("ok") else b""
                except ValueError:
                    data = b""
                shot = (data, "" if data else str(resp.get("reason") or "没截到"))
            self.shot, self.shot_at = shot, time.time()
            return shot


async def status(c, probe: WindowProbe) -> dict:
    now = time.time()
    checks = []
    running = await asyncio.to_thread(wechat_running)
    if running:
        checks.append(("微信进程", "ok", "在运行"))
    else:
        checks.append(("微信进程", "bad", "没在运行"))
    window = await probe.get(c)
    checks.append(("微信窗口", *window_check(window)))

    dbs = list(c.store.msg_dbs.values())
    readable = sum(1 for d in dbs if d.con is not None)
    quiet = now - (c.last_change or c.started)
    if c.key_error:
        checks.append(("数据库", "bad", f"密钥不能用（{c.key_error}），按「登录」页重新抓一次"))
    elif not dbs or readable < len(dbs):
        checks.append(("数据库", "warn", f"{len(dbs)} 个消息库里能读 {readable} 个"))
    elif quiet > QUIET_WARN_SECONDS:
        checks.append(("数据库", "warn", f"{ago(quiet)}没有新数据，可能掉线了"))
    else:
        last = f"最近一次有新数据在 {ago(now - c.last_change)}前" if c.last_change else "启动后还没有新数据"
        checks.append(("数据库", "ok", f"{readable} 个消息库都能读，{last}"))

    ob = c.ob
    if c.cfg["receive_only"]:
        checks.append(("AstrBot", "ok", "只收不发模式，不连 AstrBot"))
    elif ob.ws is None:
        checks.append(("AstrBot", "bad", f"没连上：{ob.last_error or '正在连'}。收到的消息先暂存 {len(ob.buffer)} 条"))
    else:
        checks.append(("AstrBot", "ok", f"已连上 {ago(now - ob.connected_at)}，启动后断开过 {ob.disconnects} 次"))

    sends = list(c.recent_sends)
    queued = c.send_queue.qsize() + c.sending
    paused = os.path.exists(os.path.join(c.state_dir, "STOP"))
    if paused:
        checks.append(("发送", "warn", "已暂停发送（有 STOP 文件），这期间 AstrBot 要发的消息会被拒，恢复后也不补发"))
    elif len(sends) >= 3 and not any(s["ok"] for s in sends[-3:]):
        checks.append(("发送", "warn", f"最近 3 次都没发出去：{sends[-1]['reason']}"))
    elif queued >= 3:
        checks.append(("发送", "warn", f"排队 {queued} 条"))
    else:
        checks.append(("发送", "ok", f"排队 {queued} 条" if queued else "空闲"))

    if not c.cfg["receive_photos"]:
        checks.append(("照片", "ok", "没开（receive_photos）"))
    elif c.image_key:
        checks.append(("照片", "ok", "图片密钥已推出，收到的照片会解开交给 AstrBot"))
    elif c.image_key_tried:
        checks.append(("照片", "warn", "图片密钥推不出来（kvcomm_dir 里没有 key_ 文件，或者解不开），照片先跳过"))
    else:
        checks.append(("照片", "ok", "还没收到过照片，收到第一张时推图片密钥"))

    async with c.db_lock:
        people = len(await asyncio.to_thread(c.private_targets))
    level = "bad" if any(x[1] == "bad" for x in checks) else "warn" if any(x[1] == "warn" for x in checks) else "ok"
    up = ob.ws is not None
    return {
        "mode": "run", "version": c.version, "now": now, "uptime": ago(now - c.started), "level": level,
        "paused": paused, "busy": bool(queued), "receive_only": bool(c.cfg["receive_only"]),
        "test_pending": c.test_pending, "self_id": c.self_id,
        "wechat": {"running": running, "checked": "ok" in window, "login": bool(window.get("login")),
                   "window": bool(window.get("ok") and window.get("frame"))},
        # 断开多久：连上过就从上次断开算，一直没连上就从启动算
        "astrbot": {"connected": up, "url": safe_url(str(c.cfg["astrbot_ws_url"]))[0],
                    "token_set": bool(c.cfg["access_token"]), "disconnects": ob.disconnects, "buffered": len(ob.buffer),
                    "since": ago(now - (ob.connected_at if up else ob.disconnected_at or c.started)),
                    "error": "" if up else ob.last_error or "正在连"},
        "send": {"queued": queued, "sending": bool(c.sending), "paused": paused,
                 "last_minute": sum(1 for t in c.sent_times if now - t <= 60),
                 "per_minute": c.cfg["max_sends_per_minute"]},
        "checks": [{"name": n, "level": lv, "text": t} for n, lv, t in checks],
        "counts": dict(sorted(c.counts.items())),
        "recent_sends": [{**s, "ago": ago(now - s["time"])} for s in reversed(sends[-12:])],
        "config": {
            "私聊": f"所有好友（{people} 人）" if c.all_friends else f"白名单 {people} 人",
            "群": f"{len(c.groups)} 个", "OneBot 号": c.self_id,
            "AstrBot 地址": safe_url(str(c.cfg["astrbot_ws_url"]))[0],
            "口令": "已设置" if c.cfg["access_token"] else "没设置",
            "收表情包": c.cfg["receive_stickers"], "收聊天记录": c.cfg["receive_records"],
            "语音": c.cfg["voice_mode"], "每分钟最多发": c.cfg["max_sends_per_minute"],
        },
    }


async def setup_status(c, probe: WindowProbe) -> dict:
    """首次配置时的状态：微信登录没有、keys.json 和 config.json 有没有"""
    now = time.time()
    checks = []
    if not await asyncio.to_thread(wechat_running):
        checks.append(("微信", "bad", "没在运行（容器刚启动的话等一会儿）"))
    else:
        window = await probe.get(c)
        if window.get("login"):
            checks.append(("微信", "warn", f"停在登录窗口：{window['login_text']}"))
        elif window.get("ok") and window.get("frame") and (window["width"] == 0 or window["width"] >= 500):
            checks.append(("微信", "ok", "已登录"))
        else:
            checks.append(("微信", "warn", window_check(window)[1]))
    files = c.files()
    if files["keys.json"]:
        checks.append(("密钥", "ok", "keys.json 已经有了"))
    else:
        checks.append(("密钥", "warn", "还没有 keys.json：按第 1 步做"))
    if c.cfg.get("_config_error"):
        checks.append(("配置", "bad", f"config.json 读不了（{c.cfg['_config_error']}）：按第 2 步重新填，旧的会留成 config.json.bak"))
    elif files["config.json"]:
        token = "已设置" if c.cfg["access_token"] else "没设置"
        checks.append(("配置", "ok", f"config.json 已经有了：{safe_url(str(c.cfg['astrbot_ws_url']))[0]}，口令{token}"))
    else:
        checks.append(("配置", "warn", "还没有 config.json：按第 2 步填"))
    return {"mode": "setup", "version": c.version, "now": now, "uptime": ago(now - c.started),
            "level": "bad" if any(x[1] == "bad" for x in checks) else "warn", "paused": False, "busy": False,
            "files": files, "checks": [{"name": n, "level": lv, "text": t} for n, lv, t in checks]}


def tail_log(state_dir: str, lines: int = 200) -> str:
    try:
        with open(os.path.join(state_dir, "connector.log"), encoding="utf-8", errors="replace") as f:
            return "\n".join(f.read().splitlines()[-lines:])
    except OSError:
        return "（还没有日志）"


def read_settings(c) -> dict:
    """页面上能改的那几项：值以 config.json 里写的为准，没写的显示默认值；白名单带上备注名；口令只给设没设置"""
    try:
        with open(c.cfg["_config_path"], encoding="utf-8") as f:
            saved = json.load(f)
    except (OSError, ValueError, KeyError):
        saved = {}
    if not isinstance(saved, dict):
        saved = {}

    def val(k):
        return saved.get(k, c.cfg.get(k))
    url, locked = safe_url(str(val("astrbot_ws_url") or ""))
    first = not os.path.exists(c.cfg["_config_path"])  # 还没有 config.json：建议先只收不发
    astrbot = {"url": url, "locked": locked, "token_set": bool(val("access_token")),
               "receive_only": bool(saved.get("receive_only", True if first else c.cfg.get("receive_only")))}
    if c.setup:
        return {"setup": True, "astrbot": astrbot}
    names = c.store.contacts()
    return {
        "astrbot": astrbot,
        "toggles": [{"key": k, "label": v, "value": bool(val(k))} for k, v in TOGGLES.items()],
        "numbers": [{"key": k, "label": lab, "min": lo, "max": hi, "value": val(k)} for k, (lab, lo, hi) in NUMBERS.items()],
        "choices": [{"key": k, "label": lab, "options": opts, "value": val(k)} for k, (lab, opts) in CHOICES.items()],
        "lists": [{"key": k, "label": v, "value": [str(x) for x in (val(k) or [])],
                   "names": {str(x): names.get(str(x), "") for x in (val(k) or []) if k != "bot_names"}}
                  for k, v in LISTS.items()],
    }


def check_ws_url(v) -> str:
    """AstrBot 地址：ws:// 或 wss://、有主机名；不许带参数和账号（口令单独填，免得出现在页面上）"""
    if not isinstance(v, str) or not v or len(v) > 300 or re.search(r"\s", v):
        return "AstrBot 地址格式不对"
    u = urlparse(v)
    try:
        u.port
    except ValueError:
        return "AstrBot 地址里的端口不对"
    if u.scheme not in ("ws", "wss") or not u.hostname:
        return "AstrBot 地址要像 ws://astrbot:6200/ws"
    if u.query or u.fragment or "@" in u.netloc:
        return "AstrBot 地址里不要带参数和账号，口令填在下面"
    return ""


def validate_settings(data, allowed=None) -> tuple[dict, str]:
    """页面提交的设置 → (要写进 config.json 的那几项, 错误说明)。不认识的键、类型不对、超范围都算错，一个都不写。
    allowed 给了就只认这几项（首次配置）"""
    if not isinstance(data, dict) or not data:
        return {}, "没有要保存的设置"
    out = {}
    for k, v in data.items():
        if allowed is not None and k not in allowed:
            return {}, f"这一项现在不能改：{str(k)[:30]}"
        if k == "astrbot_ws_url":
            err = check_ws_url(v)
            if err:
                return {}, err
            out[k] = v
        elif k == "access_token":
            if not isinstance(v, str) or not 0 < len(v) <= 200 or v != v.strip() or re.search(r"[\x00-\x1f\x7f]", v):
                return {}, "口令：1～200 个字，前后不能有空格，不能换行"
            out[k] = v
        elif k in TOGGLES:
            if not isinstance(v, bool):
                return {}, f"{TOGGLES[k]}：要么开要么关"
            out[k] = v
        elif k in NUMBERS:
            lab, lo, hi = NUMBERS[k]
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not lo <= v <= hi or v != int(v):
                return {}, f"{lab}：要填 {lo}～{hi} 的整数"
            out[k] = int(v)
        elif k in CHOICES:
            lab, opts = CHOICES[k]
            if v not in opts:
                return {}, f"{lab}：只能选 {'、'.join(opts.values())}"
            out[k] = v
        elif k in LISTS:
            if not isinstance(v, list) or len(v) > 500 or not all(isinstance(x, str) for x in v):
                return {}, f"{LISTS[k]}：格式不对"
            items = list(dict.fromkeys(x.strip() for x in v if x.strip()))
            for x in items:
                if k == "bot_names":
                    if len(x) > 40:
                        return {}, f"名字太长：{x[:20]}…"
                elif re.search(r"\s", x) or len(x) > 80:
                    return {}, f"不像 wxid：{x[:30]}"
                elif k == "group_whitelist" and not x.endswith("@chatroom"):
                    return {}, f"群的 wxid 要以 @chatroom 结尾：{x[:30]}"
                elif k == "whitelist" and x.endswith("@chatroom"):
                    return {}, f"这是群，要放到群白名单：{x[:30]}"
            out[k] = items
        else:
            return {}, f"这一项不能在页面上改：{str(k)[:30]}"
    return out, ""


def write_private(path: str, text: str, mode: int = 0o600) -> None:
    """先写 600 的临时文件再改名替换：不会读到半截，也不会有一会儿别人能读的口令"""
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def save_settings(path: str, changes: dict, broken_ok: bool = False) -> None:
    """只改 changes 里那几项，其余原样；原来有文件就先留一份 .bak（600）。
    broken_ok（首次配置）：还没有 config.json 就新建一份（600），原来的不是合法 JSON 就整份重写"""
    try:
        with open(path, encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        if not broken_ok:
            raise
        raw = None
    try:
        cfg = json.loads(raw) if raw is not None else {}
        if not isinstance(cfg, dict):
            raise ValueError("config.json 不是一个对象")
    except ValueError:
        if not broken_ok:
            raise
        cfg = {}
    cfg.update(changes)
    mode = 0o600
    if raw is not None:
        mode = os.stat(path).st_mode & 0o777
        write_private(path + ".bak", raw)
    write_private(path, json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", mode)


def lookup(c, q: str) -> list[dict]:
    """备注名或昵称里带 q 的好友和群（最多 20 个），填白名单用"""
    q = q.strip()
    if not q:
        return []
    friends = c.store.friends()
    out = []
    for wxid, name in c.store.contacts().items():
        if q in name and (wxid.endswith("@chatroom") or wxid in friends):
            out.append({"wxid": wxid, "name": name, "group": wxid.endswith("@chatroom")})
    return sorted(out, key=lambda x: (x["name"] != q, len(x["name"])))[:20]


def restart_later(delay: float = 1.0) -> None:
    """退出进程，s6 会把连接器重新拉起来（重新读 config.json 和 keys.json）"""
    asyncio.get_running_loop().call_later(delay, os._exit, 0)


async def serve(c, host: str, port: int) -> None:
    probe = WindowProbe()
    view = LoginView(probe)
    token = secrets.token_urlsafe(24)  # 每次启动换一个，放进页面里；POST 不带对的就拒绝
    page = PAGE.replace("__TOKEN__", token).encode()

    async def post(path: str, data) -> tuple[int, dict]:
        if path == "/api/stop" and not c.setup:
            if not isinstance(data, dict) or not isinstance(data.get("stop"), bool):
                return 400, {"error": "要给 stop: true 或 false"}
            stop = os.path.join(c.state_dir, "STOP")
            if data["stop"]:
                open(stop, "a").close()
            elif os.path.exists(stop):
                os.remove(stop)
            log.info("状态页：%s发送", "暂停" if data["stop"] else "恢复")
            return 200, {"ok": True, "paused": data["stop"]}
        if path == "/api/test" and not c.setup:
            if c.cfg["receive_only"]:
                return 409, {"error": "只收不发模式不发消息（在「设置」里关掉「只收不发」）"}
            if os.path.exists(os.path.join(c.state_dir, "STOP")):
                return 409, {"error": "已暂停发送，先恢复发送"}
            if c.test_pending:
                return 409, {"error": "上一条测试消息还没发完"}
            return 200, {"ok": True, "ahead": c.send_test()}
        if path in ("/api/restart", "/api/settings"):
            if c.sending or c.send_queue.qsize():
                return 409, {"error": "正在发消息，等发完再试"}
            if path == "/api/settings":
                changes, err = validate_settings(data, SETUP_KEYS if c.setup else None)
                if err:
                    return 400, {"error": err}
                try:
                    await asyncio.to_thread(save_settings, c.cfg["_config_path"], changes, c.setup)
                except (OSError, ValueError) as e:
                    log.warning("状态页：保存设置失败（%s）", type(e).__name__)
                    return 500, {"error": f"写 config.json 失败（{type(e).__name__}），没有重启"}
                log.info("状态页：保存设置（%s），重启连接器", "、".join(changes))
            else:
                log.info("状态页：重启连接器")
            restart_later()
            return 200, {"ok": True}
        return 404, {"error": "没有这个接口"}

    async def get(path: str, query: dict) -> tuple[int, str, bytes]:
        def as_json(obj) -> tuple[int, str, bytes]:
            return 200, "application/json; charset=utf-8", json.dumps(obj, ensure_ascii=False).encode()
        if path == "/":
            return 200, "text/html; charset=utf-8", page
        if path == "/api/status":
            return as_json(await (setup_status(c, probe) if c.setup else status(c, probe)))
        if path == "/api/log":
            lines = query.get("lines", ["200"])[0]
            lines = min(2000, max(50, int(lines))) if lines.isdigit() else 200
            return 200, "text/plain; charset=utf-8", (await asyncio.to_thread(tail_log, c.state_dir, lines)).encode()
        if path == "/api/login":
            return as_json(await view.state(c))
        if path == "/api/login/qr":
            png, why = await view.png(c)
            if png:
                return 200, "image/png", png
            return 409, "application/json; charset=utf-8", json.dumps({"error": why}, ensure_ascii=False).encode()
        if path == "/api/settings" and c.setup:
            return as_json(await asyncio.to_thread(read_settings, c))
        if path in ("/api/settings", "/api/lookup") and not c.setup:
            async with c.db_lock:
                if path == "/api/settings":
                    return as_json(await asyncio.to_thread(read_settings, c))
                return as_json(await asyncio.to_thread(lookup, c, query.get("q", [""])[0]))
        return 404, "text/plain; charset=utf-8", "没有这个页面".encode()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            lines = head.decode("latin-1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            headers = {k.strip().lower(): v.strip() for k, _, v in (h.partition(":") for h in lines[1:] if ":" in h)}
            url = urlparse(target)
            path = url.path
            ctype = "text/plain; charset=utf-8"
            if method == "POST":
                length = int(headers.get("content-length") or 0)
                origin = urlparse(headers.get("origin", "")).hostname
                if not secrets.compare_digest(headers.get("x-wxc-token", ""), token) \
                        or not headers.get("content-type", "").startswith("application/json") \
                        or (origin and origin not in ("localhost", "127.0.0.1")):
                    code, body = 403, "口令不对或者不是从状态页提交的".encode()
                elif not 0 < length <= MAX_BODY:
                    code, body = 413, "内容太长或者是空的".encode()
                else:
                    raw = await asyncio.wait_for(reader.readexactly(length), 10)
                    try:
                        data = json.loads(raw)
                    except ValueError:
                        data = None
                    code, result = await post(path, data)
                    ctype, body = "application/json; charset=utf-8", json.dumps(result, ensure_ascii=False).encode()
            elif method not in ("GET", "HEAD"):
                code, body = 405, "只支持 GET 和 POST".encode()
            else:
                code, ctype, body = await get(path, parse_qs(url.query))
            reason = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 405: "Method Not Allowed",
                      409: "Conflict", 413: "Payload Too Large", 500: "Internal Server Error"}[code]
            writer.write((f"HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                          "Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\n"
                          "Cross-Origin-Resource-Policy: same-origin\r\n"
                          "Content-Security-Policy: default-src 'self'; script-src 'unsafe-inline'; "
                          "style-src 'unsafe-inline'\r\nConnection: close\r\n\r\n").encode()
                         + (b"" if method == "HEAD" else body))
            await writer.drain()
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError,
                ConnectionError):
            pass
        except Exception:  # 状态页出错不能影响收发
            log.exception("状态页出错")
        finally:
            writer.close()

    try:
        server = await asyncio.start_server(handle, host, port, limit=16384)
    except OSError as e:
        log.warning("状态页没开起来（%s:%d：%s）", host, port, e.strerror or type(e).__name__)
        return
    log.info("状态页：容器里的 http://%s:%d/，网页桌面上是 /wxc/", host, port)
    async with server:
        await server.serve_forever()


# 原始字符串：页面脚本里的 "\n"、正则里的 \d 要原样到浏览器（0.10.0～0.14.0 用的是普通字符串，"\n" 变成真换行，整段脚本都跑不起来）
PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="wxc-token" content="__TOKEN__">
<meta name="color-scheme" content="light dark">
<title>微信连接器</title>
<script>
try {
  const t = localStorage.getItem("wxc-theme");
  if (t === "light" || t === "dark") document.documentElement.dataset.theme = t;
  if (localStorage.getItem("wxc-side") === "0") document.documentElement.classList.add("side-off");
} catch (e) {}
</script>
<style>
:root {
  --sans: -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif;
  --mono: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
  --accent: #db2777; --accent-text: #b42062; --accent-soft: rgba(219, 39, 119, .11); --accent-pill: rgba(219, 39, 119, .32);
  --bg: #fbfbfd; --card: rgba(255, 255, 255, .8); --card-2: #fff; --line: rgba(15, 23, 42, .08);
  --text: #18181b; --text-2: #3f3f46; --muted: #62626b; --field: rgba(15, 23, 42, .05); --switch-off: #8b8b95;
  --shadow: 0 1px 2px rgba(15, 23, 42, .04), 0 6px 22px -10px rgba(15, 23, 42, .12);
  --ok: #15803d; --ok-bg: #dcfce7; --ok-mark: #16a34a;
  --warn: #b45309; --warn-bg: #fef3c7; --warn-mark: #d97706;
  --bad: #b91c1c; --bad-bg: #fee2e2; --bad-mark: #dc2626;
  --blob-1: rgba(244, 114, 182, .14); --blob-2: rgba(167, 139, 250, .16); --blob-3: rgba(251, 113, 133, .09);
  --term: #111216; --term-line: rgba(255, 255, 255, .07); --term-text: #d9dce3; --term-muted: #8b909c;
  color-scheme: light;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --accent-text: #ed93bb; --accent-soft: rgba(219, 39, 119, .22);
    --bg: #0d0d11; --card: rgba(28, 28, 34, .8); --card-2: #1c1c22; --line: rgba(255, 255, 255, .08);
    --text: #ececf1; --text-2: #c9c9d1; --muted: #9d9daa; --field: rgba(255, 255, 255, .07); --switch-off: #5e5e69;
    --shadow: 0 1px 2px rgba(0, 0, 0, .4), 0 8px 26px -10px rgba(0, 0, 0, .6);
    --ok: #4ade80; --ok-bg: rgba(74, 222, 128, .14); --ok-mark: #4ade80;
    --warn: #fbbf24; --warn-bg: rgba(251, 191, 36, .14); --warn-mark: #fbbf24;
    --bad: #f87171; --bad-bg: rgba(248, 113, 113, .15); --bad-mark: #f87171;
    --blob-1: rgba(236, 72, 153, .10); --blob-2: rgba(139, 92, 246, .13); --blob-3: rgba(244, 63, 94, .06);
    --term: #09090c; color-scheme: dark;
  }
}
:root[data-theme="dark"] {
  --accent-text: #ed93bb; --accent-soft: rgba(219, 39, 119, .22);
  --bg: #0d0d11; --card: rgba(28, 28, 34, .8); --card-2: #1c1c22; --line: rgba(255, 255, 255, .08);
  --text: #ececf1; --text-2: #c9c9d1; --muted: #9d9daa; --field: rgba(255, 255, 255, .07); --switch-off: #5e5e69;
  --shadow: 0 1px 2px rgba(0, 0, 0, .4), 0 8px 26px -10px rgba(0, 0, 0, .6);
  --ok: #4ade80; --ok-bg: rgba(74, 222, 128, .14); --ok-mark: #4ade80;
  --warn: #fbbf24; --warn-bg: rgba(251, 191, 36, .14); --warn-mark: #fbbf24;
  --bad: #f87171; --bad-bg: rgba(248, 113, 113, .15); --bad-mark: #f87171;
  --blob-1: rgba(236, 72, 153, .10); --blob-2: rgba(139, 92, 246, .13); --blob-3: rgba(244, 63, 94, .06);
  --term: #09090c; color-scheme: dark;
}
* { box-sizing: border-box; }
[hidden] { display: none !important; }
html { -webkit-text-size-adjust: 100%; }
body { margin: 0; min-height: 100vh; color: var(--text); font: 14px/1.55 var(--sans); background-color: var(--bg);
  background-image: radial-gradient(44rem 30rem at -4% 4%, var(--blob-1), transparent 70%),
    radial-gradient(40rem 34rem at 104% 38%, var(--blob-2), transparent 70%),
    radial-gradient(46rem 30rem at 30% 108%, var(--blob-3), transparent 70%);
  background-attachment: fixed; }
a { color: inherit; }
p { margin: 0; }
code { font: 12.5px var(--mono); }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.i { width: 18px; height: 18px; flex: none; fill: none; stroke: currentColor; stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round; }
body.setup .run-only, body:not(.setup) .setup-only { display: none !important; }

/* 框架：左边导航，右边顶栏加内容 */
.app { display: flex; min-height: 100vh; }
.side { position: sticky; top: 0; height: 100vh; width: 226px; flex: none; display: flex; flex-direction: column;
  padding: 20px 12px 18px; overflow-y: auto; }
.side-off .side { display: none; }
.brand { display: flex; align-items: center; gap: 10px; padding: 6px 12px 22px; }
.brand .bar { width: 4px; height: 20px; flex: none; border-radius: 2px; background: var(--accent); }
.brand strong { font-size: 18px; letter-spacing: .02em; white-space: nowrap; }
.brand .ver { margin-left: auto; font: 11px var(--mono); color: var(--muted); }
.brand .close { display: none; }
.nav { display: flex; flex-direction: column; gap: 6px; }
.nav a { display: flex; align-items: center; gap: 10px; min-height: 44px; padding: 0 14px; border-radius: 12px;
  color: var(--text-2); text-decoration: none; }
.nav a:hover { background: var(--field); }
.nav a span { flex: 1; }
.nav a i { width: 12px; height: 4px; border-radius: 2px; background: var(--accent-pill); }
.nav a.on { color: var(--accent-text); background: var(--accent-soft); font-weight: 600; }
.nav a.on i { width: 16px; background: var(--accent); }
.side-note { margin: 18px 12px 0; font-size: 12px; color: var(--muted); }
.side-foot { margin-top: auto; display: flex; flex-direction: column; gap: 10px; padding: 24px 8px 0; }
.pill-btn { display: flex; align-items: center; justify-content: center; gap: 6px; min-height: 44px; padding: 0 14px; border: 0;
  border-radius: 999px; font: 600 13px var(--sans); color: var(--accent-text); background-color: var(--accent-soft);
  text-decoration: none; cursor: pointer; }
.pill-btn .i { width: 16px; height: 16px; margin-right: 2px; }
.pill-btn:hover, .btn:hover, .icon-btn:hover, .x:hover, .cmd button:hover {
  background-image: linear-gradient(rgba(127, 127, 127, .12), rgba(127, 127, 127, .12)); }
.main { flex: 1; min-width: 0; padding: 16px 16px 40px; }
.topbar { position: sticky; top: 12px; z-index: 6; display: flex; align-items: center; gap: 10px; min-height: 48px;
  margin-bottom: 16px; padding: 0 14px 0 6px; border-radius: 14px; background: var(--card);
  -webkit-backdrop-filter: blur(16px) saturate(140%); backdrop-filter: blur(16px) saturate(140%);
  border: 1px solid var(--line); box-shadow: var(--shadow); }
.topbar h1 { margin: 0; font-size: 15px; font-weight: 650; white-space: nowrap; }
.icon-btn { width: 40px; height: 40px; flex: none; display: inline-flex; align-items: center; justify-content: center;
  border: 0; border-radius: 10px; color: var(--text-2); background-color: transparent; cursor: pointer; }
.status { margin-left: auto; min-width: 0; display: inline-flex; align-items: center; gap: 8px; padding: 4px 12px;
  border-radius: 999px; font-size: 12.5px; font-weight: 600; }
.status #overalltext { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.status.ok { color: var(--ok); background: var(--ok-bg); }
.status.warn { color: var(--warn); background: var(--warn-bg); }
.status.bad { color: var(--bad); background: var(--bad-bg); }
.dot { position: relative; width: 8px; height: 8px; flex: none; border-radius: 50%; background: currentColor; }
.dot.live::before { content: ""; position: absolute; inset: 0; border-radius: 50%; background: currentColor;
  animation: pulse 2.4s ease-out infinite; }
@keyframes pulse { 0% { transform: scale(.8); opacity: .6; } 70% { transform: scale(1.8); opacity: 0; } 100% { opacity: 0; } }
.stamp { font: 12px var(--mono); color: var(--muted); white-space: nowrap; }
.wrap { max-width: 1040px; margin: 0 auto; }
.tab { display: none; flex-direction: column; gap: 16px; }
.tab.on { display: flex; }

/* 通用 */
.card { min-width: 0; padding: 18px 20px; border-radius: 16px; background: var(--card);
  -webkit-backdrop-filter: blur(16px) saturate(140%); backdrop-filter: blur(16px) saturate(140%);
  border: 1px solid var(--line); box-shadow: var(--shadow); }
.card h2 { display: flex; align-items: center; gap: 8px; margin: 0; font-size: 15px; line-height: 1.4; font-weight: 650; }
.card h2 .i { width: 17px; height: 17px; color: var(--text-2); }
.head { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 8px 16px; margin-bottom: 12px; }
.sub { font-size: 12.5px; color: var(--muted); }
.lead { margin-top: 8px; font-size: 13.5px; color: var(--text-2); }
.hint { margin: 6px 4px 0; font-size: 12px; color: var(--muted); }
.row { display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-start; }
.row > * { flex: 1 1 380px; }
.chip { display: inline-flex; align-items: center; gap: 4px; padding: 3px 10px; border-radius: 999px; font-size: 12px;
  font-weight: 600; white-space: nowrap; color: var(--text-2); background: var(--field); }
.chip.ok { color: var(--ok); background: var(--ok-bg); }
.chip.warn { color: var(--warn); background: var(--warn-bg); }
.chip.bad { color: var(--bad); background: var(--bad-bg); }
.chip.accent { color: var(--accent-text); background: var(--accent-soft); }
.mono { font-family: var(--mono); }
.empty { padding: 14px 0; text-align: center; font-size: 13px; color: var(--muted); }
.actions { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.actions.center { justify-content: center; }
.btn { display: inline-flex; align-items: center; justify-content: center; gap: 8px; min-height: 44px; padding: 0 16px;
  border: 0; border-radius: 999px; font: 600 13.5px var(--sans); color: var(--text); background-color: var(--field);
  text-decoration: none; white-space: nowrap; cursor: pointer; }
.btn .i { width: 16px; height: 16px; }
.btn.primary { color: #fff; background-color: var(--accent); box-shadow: 0 8px 18px -10px var(--accent); }
.btn.soft { color: var(--accent-text); background-color: var(--accent-soft); }
.btn.small { min-height: 40px; padding: 0 14px; font-size: 13px; }
.btn:disabled { opacity: .5; cursor: not-allowed; background-image: none; }
.state { display: inline-flex; align-items: center; gap: 6px; font-weight: 600; }
.state .dot { width: 7px; height: 7px; }
.state.ok { color: var(--ok); } .state.warn { color: var(--warn); } .state.bad { color: var(--bad); }

/* 概览 */
.ov-top { display: flex; flex-wrap: wrap; gap: 16px; }
.ov-left { flex: 1 1 300px; min-width: 0; display: flex; flex-direction: column; gap: 16px; }
.ov-left .grow { flex: 1; display: flex; flex-direction: column; }
.account { position: relative; overflow: hidden; display: flex; align-items: center; gap: 14px; padding: 16px 18px; }
.account .watermark { position: absolute; right: -12px; bottom: -20px; width: 88px; height: 88px; stroke-width: 1.2; opacity: .07; }
.avatar { position: relative; width: 48px; height: 48px; flex: none; display: flex; align-items: center; justify-content: center;
  border-radius: 50%; color: var(--accent-text); background: var(--accent-soft); }
.avatar .i { width: 24px; height: 24px; }
.avatar .online { position: absolute; right: 1px; bottom: 1px; width: 12px; height: 12px; border-radius: 50%;
  background: var(--switch-off); box-shadow: 0 0 0 2px var(--card-2); }
.avatar .online.ok { background: var(--ok-mark); } .avatar .online.warn { background: var(--warn-mark); }
.avatar .online.bad { background: var(--bad-mark); }
.account .who { position: relative; flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
.account .who strong { font-size: 16px; line-height: 1.3; }
.account .who span { font: 12px var(--mono); color: var(--muted); }
.account .chip { position: relative; }
.kv { margin: 6px 0 0; }
.kv div { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 7px 0; }
.kv dt { font-size: 13px; font-weight: 500; color: var(--text-2); }
.kv dd { margin: 0; font: 12.5px var(--mono); color: var(--muted); text-align: right; overflow-wrap: anywhere; }
.card-foot { display: flex; flex-wrap: wrap; gap: 8px; margin-top: auto; padding-top: 12px; border-top: 1px solid var(--line); }
.card-foot .btn { flex: 1 1 120px; }
.ov-link { flex: 2 1 460px; display: flex; flex-wrap: wrap; align-items: center; gap: 20px 28px; }
.ov-info { flex: 1 1 300px; min-width: 0; }
.ov-info .actions { margin-top: 16px; }
.grid2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px 20px; margin: 10px 0 0; }
.grid2 .wide { grid-column: 1 / -1; }
.grid2 dt { font-size: 12px; color: var(--muted); }
.grid2 dd { margin: 0; font-size: 13.5px; font-weight: 500; overflow-wrap: anywhere; }
.grid2 dd.mono { font-size: 13px; font-weight: 400; }
.sep { height: 1px; margin: 16px 0; background: var(--line); }
.rings { flex: 0 0 auto; display: flex; flex-direction: column; align-items: center; gap: 16px; margin: 0 auto; }
.ring { position: relative; width: 132px; height: 132px; }
.ring svg { display: block; width: 100%; height: 100%; transform: rotate(-90deg); }
.ring circle { fill: none; stroke-width: 9; }
.ring .val { stroke-linecap: round; stroke-dasharray: 0 314.16; transition: stroke-dasharray .4s; }
.ring .txt { position: absolute; inset: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 2px; }
.ring .txt span { font-size: 11px; color: var(--muted); }
.ring .txt strong { font: 700 26px/1.1 var(--mono); }
.ring .txt small { font-size: 13px; font-weight: 600; color: var(--muted); }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; }
.tile { display: flex; flex-direction: column; align-items: center; gap: 2px; padding: 16px 12px 14px; text-align: center; }
.tile strong { font: 700 30px/1.2 var(--mono); }
.tile span { display: inline-flex; align-items: center; gap: 6px; font-size: 12.5px; color: var(--muted); }
.tile .mark { width: 6px; height: 6px; border-radius: 50%; background: var(--bad-mark); }
.ov-mid { display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-start; }
.checks-card { flex: 3 1 420px; padding-bottom: 6px; }
.counts-card { flex: 2 1 320px; }
.checks { list-style: none; margin: 0; padding: 0; }
.checks li { display: flex; align-items: flex-start; gap: 12px; padding: 11px 0; border-top: 1px solid var(--line); }
.checks .ic { width: 28px; height: 28px; flex: none; margin-top: 1px; display: flex; align-items: center; justify-content: center; border-radius: 50%; }
.checks .ic .i { width: 15px; height: 15px; stroke-width: 2.4; }
.checks .ic.ok { color: var(--ok); background: var(--ok-bg); }
.checks .ic.warn { color: var(--warn); background: var(--warn-bg); }
.checks .ic.bad { color: var(--bad); background: var(--bad-bg); }
.checks .body { flex: 1; min-width: 0; }
.checks b { display: block; font-size: 13.5px; font-weight: 600; }
.checks .body > span { display: block; font-size: 13px; color: var(--text-2); overflow-wrap: anywhere; }
.checks .go { display: inline-block; margin-top: 2px; font-size: 12.5px; font-weight: 600; color: var(--accent-text); }
.checks.compact { margin-top: 14px; }
h3.group { margin: 14px 0 2px; font-size: 12px; font-weight: 600; color: var(--muted); }
.bars { list-style: none; margin: 0; padding: 0; }
.bars li { display: grid; grid-template-columns: minmax(84px, 120px) minmax(0, 1fr) 44px; align-items: center; gap: 12px; padding: 5px 0; }
.bars li.empty { display: block; padding: 6px 0; text-align: left; }
.bars .name { font-size: 13px; color: var(--text-2); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.bars .track { height: 8px; border-radius: 4px; background: var(--field); overflow: hidden; }
.bars .fill { display: block; height: 8px; border-radius: 4px; background: var(--accent); }
.bars .n { text-align: right; font: 600 12.5px var(--mono); }
.sends { overflow-x: auto; }
.sends ul { list-style: none; margin: 0; padding: 0; min-width: 520px; }
.sends li { display: grid; grid-template-columns: 96px minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1.6fr); gap: 16px; align-items: center;
  padding: 10px 0; border-bottom: 1px solid var(--line); font-size: 13px; }
.sends li.headrow { padding: 8px 0; font-size: 12px; font-weight: 600; color: var(--muted); }
.sends li.empty { display: block; border: 0; }
.sends .t { font: 12.5px var(--mono); color: var(--muted); white-space: nowrap; }
.sends .to { font: 12.5px var(--mono); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sends .what { color: var(--text-2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sends .res { display: inline-flex; align-items: center; gap: 6px; min-width: 0; font-weight: 600; }
.sends .res .i { width: 15px; height: 15px; stroke-width: 2.4; }
.sends .res span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sends .res.ok { color: var(--ok); } .sends .res.bad { color: var(--bad); }

/* 登录 */
.login-card { display: flex; flex-direction: column; gap: 16px; }
.login-card .head { margin-bottom: 0; }
.live { display: inline-flex; align-items: center; gap: 8px; font: 12px var(--mono); color: var(--muted); }
.live .dot { width: 7px; height: 7px; color: var(--accent); }
.banner { display: flex; align-items: center; gap: 12px; padding: 12px 14px; border-radius: 14px; color: var(--accent-text); background: var(--accent-soft); }
.banner.ok { color: var(--ok); background: var(--ok-bg); }
.banner.warn { color: var(--warn); background: var(--warn-bg); }
.banner.bad { color: var(--bad); background: var(--bad-bg); }
.banner .i { width: 22px; height: 22px; }
.banner > div { min-width: 0; }
.banner strong { display: block; font-size: 14px; }
.banner #loginsub { display: block; font-size: 12.5px; color: var(--text-2); }
.qrstage { display: flex; justify-content: center; padding: 4px 0; }
.qrframe { width: 300px; max-width: 100%; padding: 10px; border-radius: 18px; background: #fff; border: 1px solid rgba(15, 23, 42, .06);
  box-shadow: 0 1px 2px rgba(0, 0, 0, .06), 0 18px 40px -18px rgba(15, 23, 42, .4); }
.qrframe img { display: block; width: 100%; height: auto; border-radius: 10px; }
.qrempty { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 10px; min-height: 220px; padding: 20px;
  border-radius: 18px; border: 1.5px dashed var(--line); text-align: center; font-size: 13px; color: var(--muted); }
.qrempty .i { width: 34px; height: 34px; stroke-width: 1.4; }
.foot-note { display: flex; align-items: flex-start; gap: 8px; padding-top: 14px; border-top: 1px solid var(--line); font-size: 12.5px; color: var(--muted); }
.foot-note .i { width: 15px; height: 15px; margin-top: 2px; }
.steps { list-style: none; margin: 14px 0 0; padding: 0; counter-reset: step; }
.steps > li { position: relative; display: flex; gap: 14px; padding-bottom: 18px; counter-increment: step; }
.steps > li::before { content: counter(step); width: 28px; height: 28px; flex: none; display: flex; align-items: center; justify-content: center;
  border-radius: 50%; font: 700 13px var(--mono); color: var(--accent-text); background: var(--accent-soft); }
.steps > li::after { content: ""; position: absolute; left: 13px; top: 34px; bottom: 6px; width: 2px; border-radius: 1px; background: var(--line); }
body:not(.setup) .steps > li.run-only::after, body.setup .steps > li.setup-only::after { display: none; }
.stepbody { flex: 1; min-width: 0; display: flex; flex-direction: column; align-items: flex-start; gap: 10px; padding-top: 3px; font-size: 13.5px; }
.cmd { align-self: stretch; display: flex; align-items: center; gap: 8px; padding: 6px 6px 6px 14px; border-radius: 12px; background: var(--term); color: var(--term-text); }
.cmd code { flex: 1; min-width: 0; overflow-x: auto; white-space: nowrap; }
.cmd button { flex: none; display: inline-flex; align-items: center; gap: 6px; min-height: 40px; padding: 0 12px; border: 0; border-radius: 9px;
  font: 600 12.5px var(--sans); color: var(--term-text); background-color: rgba(255, 255, 255, .08); cursor: pointer; }
.cmd button .i { width: 15px; height: 15px; }

/* 设置 */
.fields { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(280px, 100%), 1fr)); gap: 14px 16px; }
.fld { display: flex; flex-direction: column; gap: 2px; padding: 8px 14px; border-radius: 14px; background: var(--field); }
.fld:focus-within { box-shadow: inset 0 0 0 2px var(--accent); }
.fld label, .choice-label { font-size: 12px; font-weight: 500; color: var(--text-2); }
.fld input { width: 100%; min-height: 26px; padding: 0; border: 0; outline: 0; background: transparent; font: 13.5px var(--mono); color: var(--text); }
.fld input:disabled { color: var(--muted); }
.fld input::placeholder, .add input::placeholder, .search input::placeholder { color: var(--muted); opacity: 1; }
.fld .lab { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.tokset { display: inline-flex; align-items: center; gap: 4px; font-size: 11.5px; font-weight: 600; color: var(--ok); }
.tokset .i { width: 12px; height: 12px; stroke-width: 2.2; }
.check-row { display: flex; align-items: flex-start; gap: 12px; margin-top: 14px; padding: 12px 14px; border-radius: 14px; background: var(--field); }
.check-row input { width: 20px; height: 20px; margin: 2px 0 0; flex: none; accent-color: var(--accent); }
.check-row label { display: flex; flex-direction: column; cursor: pointer; }
.check-row strong { font-size: 13.5px; }
.check-row span { font-size: 12.5px; color: var(--muted); }
.foot { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-end; gap: 12px; margin-top: 16px; padding-top: 14px; border-top: 1px solid var(--line); }
.foot .sub { margin-right: auto; }
.toggles { list-style: none; margin: 0; padding: 0; }
.toggles li { display: flex; align-items: center; justify-content: space-between; gap: 16px; border-top: 1px solid var(--line); }
.toggles li.empty { display: block; border: 0; }
.toggles label { flex: 1; min-width: 0; padding: 12px 0; font-size: 13.5px; cursor: pointer; }
.changed { margin-left: 8px; padding: 1px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; color: var(--warn); background: var(--warn-bg); }
.switch { position: relative; flex: none; width: 46px; height: 28px; padding: 0; border: 0; border-radius: 999px; background: var(--switch-off);
  cursor: pointer; transition: background-color .15s; }
.switch::after { content: ""; position: absolute; top: 3px; left: 3px; width: 22px; height: 22px; border-radius: 50%; background: #fff;
  box-shadow: 0 1px 3px rgba(0, 0, 0, .3); transition: left .15s; }
.switch[aria-checked="true"] { background: var(--accent); }
.switch[aria-checked="true"]::after { left: 21px; }
.stack { display: flex; flex-direction: column; gap: 14px; }
.numfld { flex-direction: row; align-items: center; gap: 12px; padding: 8px 10px 8px 14px; }
.numfld > div { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
.numfld input { font-size: 15px; font-weight: 600; }
.numfld input:invalid { color: var(--bad); }
.range { flex: none; padding: 4px 10px; border-radius: 8px; font: 12px var(--mono); color: var(--muted); background: var(--card-2); }
.choice-label { display: block; margin: 0 0 6px 4px; }
.seg { display: flex; gap: 4px; padding: 4px; border-radius: 14px; background: var(--field); }
.seg button { flex: 1; min-height: 40px; padding: 0 14px; border: 0; border-radius: 10px; font: 600 13px var(--sans); color: var(--muted);
  background: transparent; cursor: pointer; white-space: nowrap; }
.seg button[aria-pressed="true"] { color: var(--text); background: var(--card-2); box-shadow: 0 1px 3px rgba(0, 0, 0, .12); }
.lists { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(280px, 100%), 1fr)); gap: 14px; }
.lst { min-width: 0; display: flex; flex-direction: column; gap: 8px; padding: 14px 14px 12px; border-radius: 14px; background: var(--field); }
.lst .top { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
.lst h3 { margin: 0; font-size: 14px; font-weight: 650; }
.lst .count { font: 12px var(--mono); color: var(--muted); }
.lst ul { list-style: none; margin: 0; padding: 0; max-height: 300px; overflow-y: auto; }
.lst li { display: flex; align-items: center; gap: 10px; padding: 6px 0; border-top: 1px solid var(--line); }
.lst li.empty { display: block; }
.lst .who { flex: 1; min-width: 0; display: flex; flex-direction: column; }
.lst .who b { font-size: 13.5px; font-weight: 600; overflow-wrap: anywhere; }
.lst .who code { font-size: 12px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.lst .who code.only { font-size: 13px; color: var(--text); }
.x { flex: none; width: 36px; height: 36px; display: inline-flex; align-items: center; justify-content: center; border: 0; border-radius: 10px;
  color: var(--muted); background-color: transparent; cursor: pointer; }
.chips { display: flex; flex-wrap: wrap; gap: 8px; padding-top: 8px; border-top: 1px solid var(--line); }
.chips .name { display: inline-flex; align-items: center; gap: 2px; min-height: 36px; padding: 0 4px 0 12px; border-radius: 999px; font-size: 13px;
  background: var(--card-2); border: 1px solid var(--line); }
.chips .x { width: 30px; height: 30px; border-radius: 50%; }
.chips .empty { padding: 6px 0; }
.add { display: flex; gap: 8px; margin-top: auto; }
.add input { flex: 1; min-width: 0; min-height: 40px; padding: 0 12px; border: 1px solid var(--line); border-radius: 10px; font: 13px var(--sans);
  color: var(--text); background: var(--card-2); }
.add input.mono { font: 12.5px var(--mono); }
.add button { flex: none; width: 40px; height: 40px; display: inline-flex; align-items: center; justify-content: center; border: 0; border-radius: 10px;
  color: var(--accent-text); background-color: var(--accent-soft); cursor: pointer; }
.lookup { margin-top: 16px; padding-top: 16px; border-top: 1px solid var(--line); }
.lookup .bar { display: flex; flex-wrap: wrap; align-items: stretch; gap: 10px; }
.search-fld { flex: 1 1 320px; max-width: 560px; flex-direction: row; align-items: center; gap: 10px; }
.search-fld > div { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
.search-fld input { font-family: var(--sans); }
.search-fld .i { color: var(--muted); }
.found { list-style: none; margin: 8px 0 0; padding: 0; max-width: 760px; }
.found li { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 12px; padding: 10px 0; border-bottom: 1px solid var(--line); }
.found li.empty { display: block; border: 0; text-align: left; }
.found .kind { padding: 1px 8px; border-radius: 6px; font-size: 11.5px; font-weight: 600; color: var(--text-2); background: var(--field); }
.found code { font-size: 12px; color: var(--muted); overflow-wrap: anywhere; }
.found .btn { margin-left: auto; }
.savebar { position: sticky; bottom: 16px; z-index: 5; display: flex; flex-wrap: wrap; align-items: center; gap: 10px 14px;
  padding: 10px 12px 10px 18px; border-radius: 16px; background: var(--card-2); border: 1px solid var(--line);
  box-shadow: 0 18px 40px -18px rgba(15, 23, 42, .45); }
.savebar .dot { color: var(--ok-mark); }
.savebar.dirty .dot { color: var(--warn-mark); }
.savebar strong { font-size: 13.5px; }
.savebar .sub { margin-right: auto; }

/* 日志 */
.toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 10px 12px; padding: 10px 12px; }
.toolbar .seg { border-radius: 12px; }
.toolbar .seg button { min-height: 36px; border-radius: 9px; }
.search { flex: 1 1 220px; min-width: 0; display: flex; align-items: center; gap: 8px; min-height: 44px; padding: 0 12px; border-radius: 12px; background: var(--field); }
.search:focus-within { box-shadow: inset 0 0 0 2px var(--accent); }
.search .i { width: 16px; height: 16px; color: var(--muted); }
.search input { flex: 1; min-width: 0; padding: 0; border: 0; outline: 0; background: transparent; font: 13.5px var(--sans); color: var(--text); }
select { min-height: 44px; padding: 0 12px; border: 0; border-radius: 12px; font: 13px var(--sans); color: var(--text); background: var(--field); }
.follow { display: inline-flex; align-items: center; gap: 10px; padding: 0 4px; font-size: 13px; }
.follow label { cursor: pointer; }
#logcount { margin-left: auto; padding: 0 4px; font: 12px var(--mono); color: var(--muted); }
.term { position: relative; overflow: hidden; border-radius: 16px; background: var(--term); border: 1px solid var(--line); box-shadow: var(--shadow); }
.term-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 16px; border-bottom: 1px solid var(--term-line); }
.term-head .name { display: inline-flex; align-items: center; gap: 8px; font: 600 12.5px var(--mono); color: var(--term-text); }
.term-head .name .i { width: 16px; height: 16px; }
.term-head .live { font-family: var(--sans); color: var(--term-muted); }
.term-head .live .dot { color: var(--term-muted); }
.term-head .live .dot.live { color: #4ade80; }
.logbody { height: min(68vh, 640px); overflow: auto; padding: 10px 0 16px; font: 12.5px/1.75 var(--mono); color: var(--term-text); }
.logbody .empty { font-family: var(--sans); color: var(--term-muted); }
.ln { display: grid; grid-template-columns: 116px 64px minmax(0, 1fr); gap: 12px; padding: 0 16px; }
.ln .t { color: var(--term-muted); white-space: nowrap; }
.ln .lv { font-weight: 700; }
.ln .m { white-space: pre-wrap; overflow-wrap: anywhere; }
.ln.l0 .lv { color: #a1a1aa; }
.ln.l1 .lv { color: #7dd3fc; }
.ln.l2 { background: rgba(251, 191, 36, .07); } .ln.l2 .lv { color: #fbbf24; }
.ln.l3 { background: rgba(248, 113, 113, .10); } .ln.l3 .lv { color: #f87171; }
.ln.l3.cont .m { color: #fca5a5; }
#tolatest { position: absolute; right: 16px; bottom: 16px; box-shadow: 0 8px 20px -8px rgba(0, 0, 0, .6); }

/* 首次配置 */
.stepper { list-style: none; margin: 14px 0 0; padding: 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(min(220px, 100%), 1fr)); gap: 12px; }
.stepper li { display: flex; align-items: flex-start; gap: 12px; padding: 12px 14px; border-radius: 14px; background: var(--field); }
.stepper .num { width: 30px; height: 30px; flex: none; display: flex; align-items: center; justify-content: center; border-radius: 50%;
  font: 700 13px var(--mono); color: var(--text-2); background: var(--card-2); border: 1px solid var(--line); }
.stepper .num .i { width: 15px; height: 15px; }
.stepper li.now { background: var(--accent-soft); }
.stepper li.now .num { color: #fff; background: var(--accent); border-color: transparent; }
.stepper li.done .num { color: var(--ok); background: var(--ok-bg); border-color: transparent; }
.stepper strong { display: block; font-size: 13.5px; }
.stepper .st-text { display: block; font-size: 12.5px; color: var(--muted); }
.stepno { font-family: var(--mono); color: var(--accent-text); }
.nested .card { background: var(--field); border-color: transparent; box-shadow: none; -webkit-backdrop-filter: none; backdrop-filter: none; }
.note-dashed { display: flex; align-items: center; gap: 12px; padding: 14px 18px; border-radius: 16px; border: 1.5px dashed var(--line);
  font-size: 13px; color: var(--text-2); }
.note-dashed .i { color: var(--muted); }

#notice { position: fixed; top: 72px; left: 50%; transform: translateX(-50%); z-index: 30; width: max-content;
  max-width: min(420px, calc(100vw - 24px)); padding: 12px 16px; border-radius: 14px; background: var(--text); color: var(--bg);
  font-size: 13.5px; box-shadow: 0 18px 40px -18px rgba(0, 0, 0, .5); display: none; }

@media (max-width: 899px) {
  .side { position: fixed; z-index: 20; top: 0; bottom: 0; left: 0; height: auto; width: 272px; background: var(--card-2);
    box-shadow: 12px 0 40px -12px rgba(0, 0, 0, .4); transform: translateX(-105%); transition: transform .2s ease; }
  .side-off .side { display: flex; }
  body.drawer .side { transform: none; }
  .brand .close { display: inline-flex; margin-left: 4px; }
  .scrim { position: fixed; inset: 0; z-index: 15; background: rgba(0, 0, 0, .45); }
  .main { padding: 12px 12px 32px; }
  .topbar { top: 8px; margin-bottom: 12px; }
}
@media (max-width: 640px) {
  .card { padding: 14px 16px; }
  .stamp { display: none; }
  .tiles { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
  .tile strong { font-size: 26px; }
  .rings { flex-direction: row; gap: 12px; }
  .ring { width: 116px; height: 116px; }
  .ring .txt strong { font-size: 22px; }
  .ln { display: block; padding: 2px 12px; }
  .ln .t, .ln .lv { margin-right: 8px; }
  .ln .m { display: block; }
  .savebar .sub { display: none; }
  #notice { top: 64px; }
}
@media (prefers-reduced-motion: reduce) {
  .dot.live::before { animation: none; }
  .side, .switch, .switch::after, .ring .val { transition: none; }
}
</style>
</head>
<body>
<svg width="0" height="0" style="position:absolute" aria-hidden="true">
<symbol id="i-grid" viewBox="0 0 24 24"><path d="M3 3h7v9H3zM14 3h7v5h-7zM14 12h7v9h-7zM3 16h7v5H3z"/></symbol>
<symbol id="i-scan" viewBox="0 0 24 24"><path d="M3 7V5a2 2 0 0 1 2-2h2M17 3h2a2 2 0 0 1 2 2v2M21 17v2a2 2 0 0 1-2 2h-2M7 21H5a2 2 0 0 1-2-2v-2M7 12h10"/></symbol>
<symbol id="i-sliders" viewBox="0 0 24 24"><path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M1 14h6M9 8h6M17 16h6"/></symbol>
<symbol id="i-term" viewBox="0 0 24 24"><path d="M4 17l6-6-6-6M12 19h8"/></symbol>
<symbol id="i-spark" viewBox="0 0 24 24"><path d="M12 3v4M12 17v4M3 12h4M17 12h4M5.6 5.6l2.8 2.8M15.6 15.6l2.8 2.8M5.6 18.4l2.8-2.8M15.6 8.4l2.8-2.8"/></symbol>
<symbol id="i-sun" viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></symbol>
<symbol id="i-monitor" viewBox="0 0 24 24"><path d="M3 4h18v12H3zM8 20h8M12 16v4"/></symbol>
<symbol id="i-menu" viewBox="0 0 24 24"><path d="M3 6h18M3 12h11M3 18h18M21 9l-3 3 3 3"/></symbol>
<symbol id="i-bubble" viewBox="0 0 24 24"><path d="M21 11.5a8.4 8.4 0 0 1-9 8.4 9 9 0 0 1-3.9-.9L3 20.5l1.6-4.3A8 8 0 0 1 3 11.5 8.4 8.4 0 0 1 12 3a8.4 8.4 0 0 1 9 8.5z"/></symbol>
<symbol id="i-info" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 16v-4M12 8h.01"/></symbol>
<symbol id="i-restart" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.6-6.4M21 4v5h-5"/></symbol>
<symbol id="i-bell" viewBox="0 0 24 24"><path d="M18 8a6 6 0 1 0-12 0c0 7-3 9-3 9h18s-3-2-3-9M13.7 21a2 2 0 0 1-3.4 0"/></symbol>
<symbol id="i-link" viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7.5.5l3-3a5 5 0 0 0-7-7l-1.7 1.7M14 11a5 5 0 0 0-7.5-.5l-3 3a5 5 0 0 0 7 7l1.7-1.7"/></symbol>
<symbol id="i-send" viewBox="0 0 24 24"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4z"/></symbol>
<symbol id="i-pause" viewBox="0 0 24 24"><path d="M8 5v14M16 5v14"/></symbol>
<symbol id="i-play" viewBox="0 0 24 24"><path d="M7 4l13 8-13 8z"/></symbol>
<symbol id="i-shield" viewBox="0 0 24 24"><path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6zM9 12l2 2 4-4"/></symbol>
<symbol id="i-check" viewBox="0 0 24 24"><path d="M20 6L9 17l-5-5"/></symbol>
<symbol id="i-alert" viewBox="0 0 24 24"><path d="M10.3 3.9L1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0zM12 9v4M12 17h.01"/></symbol>
<symbol id="i-x" viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></symbol>
<symbol id="i-bars" viewBox="0 0 24 24"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></symbol>
<symbol id="i-history" viewBox="0 0 24 24"><path d="M3 12a9 9 0 1 0 3-6.7L3 8M3 3v5h5M12 7v5l3 2"/></symbol>
<symbol id="i-phone" viewBox="0 0 24 24"><path d="M7 2h10a1 1 0 0 1 1 1v18a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1V3a1 1 0 0 1 1-1zM11 18h2"/></symbol>
<symbol id="i-loader" viewBox="0 0 24 24"><path d="M12 3a9 9 0 1 0 9 9"/></symbol>
<symbol id="i-offline" viewBox="0 0 24 24"><path d="M2 2l20 20M8.5 16.5a5 5 0 0 1 7 0M5 13a10 10 0 0 1 5.2-2.7M19 13a10 10 0 0 0-2.3-1.6M2 8.8a15 15 0 0 1 4.2-2.6M22 8.8a15 15 0 0 0-11.2-3.7M12 20h.01"/></symbol>
<symbol id="i-lock" viewBox="0 0 24 24"><path d="M5 11h14v10H5zM8 11V7a4 4 0 0 1 8 0v4"/></symbol>
<symbol id="i-key" viewBox="0 0 24 24"><circle cx="7.5" cy="15.5" r="4.5"/><path d="M10.7 12.3L21 2M15.5 7.5l3 3M18 5l2 2"/></symbol>
<symbol id="i-copy" viewBox="0 0 24 24"><path d="M9 9h11v11H9zM5 15H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v1"/></symbol>
<symbol id="i-search" viewBox="0 0 24 24"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></symbol>
<symbol id="i-plus" viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></symbol>
<symbol id="i-users" viewBox="0 0 24 24"><circle cx="9" cy="8" r="4"/><path d="M2 21a7 7 0 0 1 14 0M16 3.5a4 4 0 0 1 0 7.5M22 21a7 7 0 0 0-4.5-6.5"/></symbol>
<symbol id="i-toggle" viewBox="0 0 24 24"><rect x="2" y="6" width="20" height="12" rx="6"/><circle cx="16" cy="12" r="3"/></symbol>
<symbol id="i-down" viewBox="0 0 24 24"><path d="M12 5v14M5 12l7 7 7-7"/></symbol>
</svg>

<div class="app">
<aside class="side" id="side" aria-label="导航">
  <div class="brand"><span class="bar"></span><strong>微信连接器</strong><span class="ver" id="ver"></span>
    <button class="icon-btn close" id="sideclose" type="button" aria-label="关闭菜单"><svg class="i" aria-hidden="true"><use href="#i-x"/></svg></button></div>
  <nav class="nav" aria-label="页面">
    <a href="#overview" data-tab="overview"><svg class="i" aria-hidden="true"><use href="#i-grid"/></svg><span>概览</span><i></i></a>
    <a href="#setup" data-tab="setup" hidden><svg class="i" aria-hidden="true"><use href="#i-spark"/></svg><span>首次配置</span><i></i></a>
    <a href="#login" data-tab="login"><svg class="i" aria-hidden="true"><use href="#i-scan"/></svg><span>登录</span><i></i></a>
    <a href="#settings" data-tab="settings"><svg class="i" aria-hidden="true"><use href="#i-sliders"/></svg><span>设置</span><i></i></a>
    <a href="#log" data-tab="log"><svg class="i" aria-hidden="true"><use href="#i-term"/></svg><span>日志</span><i></i></a>
  </nav>
  <p class="side-note setup-only">配好之前只有这两页；概览和设置会在切到正常模式后出现。</p>
  <div class="side-foot">
    <button class="pill-btn" id="theme" type="button" title="页面主题：跟随系统、浅色、深色轮换"><svg class="i" aria-hidden="true"><use href="#i-sun"/></svg>主题：<span id="themename">跟随系统</span></button>
    <a class="pill-btn" href="../"><svg class="i" aria-hidden="true"><use href="#i-monitor"/></svg>打开网页桌面</a>
  </div>
</aside>
<div class="scrim" id="scrim" hidden></div>

<main class="main">
  <header class="topbar">
    <button class="icon-btn" id="menu" type="button" aria-label="显示或收起侧栏" aria-controls="side"><svg class="i" aria-hidden="true"><use href="#i-menu"/></svg></button>
    <h1 id="title">概览</h1>
    <span class="status warn" id="overall"><span class="dot live"></span><span id="overalltext">读取中</span></span>
    <span class="stamp" id="stamp"></span>
  </header>
  <div class="wrap">

  <section id="tab-overview" class="tab">
    <div class="ov-top">
      <div class="ov-left">
        <section class="card account" aria-label="小号">
          <svg class="i watermark" aria-hidden="true"><use href="#i-bubble"/></svg>
          <span class="avatar"><svg class="i" aria-hidden="true"><use href="#i-bubble"/></svg><span class="online" id="acc-dot"></span></span>
          <div class="who"><strong>微信小号</strong><span id="acc-id">OneBot 号 —</span></div>
          <span class="chip" id="acc-chip">读取中</span>
        </section>
        <section class="card grow" aria-labelledby="h-run">
          <h2 id="h-run"><svg class="i" aria-hidden="true"><use href="#i-info"/></svg>运行信息</h2>
          <dl class="kv" id="run"></dl>
          <div class="card-foot">
            <button class="btn small" id="restart" type="button" title="重新读配置和密钥；正在发消息时不重启"><svg class="i" aria-hidden="true"><use href="#i-restart"/></svg>重启连接器</button>
            <button class="btn small" id="notify" type="button">开启掉线通知</button>
          </div>
        </section>
      </div>
      <section class="card ov-link" aria-label="连接与发送">
        <div class="ov-info">
          <h2><svg class="i" aria-hidden="true"><use href="#i-link"/></svg>AstrBot</h2>
          <dl class="grid2">
            <div class="wide"><dt>地址</dt><dd class="mono" id="ab-url-v">—</dd></div>
            <div><dt>状态</dt><dd id="ab-state-v">—</dd></div>
            <div><dt>启动后断开</dt><dd class="mono" id="ab-drops">—</dd></div>
            <div><dt>口令</dt><dd id="ab-token-v">—</dd></div>
            <div><dt>没连上时暂存</dt><dd class="mono" id="ab-buf">—</dd></div>
          </dl>
          <div class="sep"></div>
          <h2><svg class="i" aria-hidden="true"><use href="#i-send"/></svg>发送</h2>
          <dl class="grid2">
            <div><dt>状态</dt><dd id="sd-state">—</dd></div>
            <div><dt>排队</dt><dd class="mono" id="sd-queue">—</dd></div>
            <div><dt>这一分钟已发</dt><dd class="mono" id="sd-minute">—</dd></div>
            <div><dt>只收不发</dt><dd id="sd-ro">—</dd></div>
          </dl>
          <div class="actions">
            <button class="btn primary" id="test" type="button"><svg class="i" aria-hidden="true"><use href="#i-send"/></svg>发测试消息</button>
            <button class="btn" id="pause" type="button" title="暂停期间 AstrBot 要发的消息会被拒，恢复后也不补发"><svg class="i" aria-hidden="true"><use href="#i-pause"/></svg>暂停发送</button>
            <span class="sub" id="testhint">测试消息只发给「文件传输助手」</span>
          </div>
        </div>
        <div class="rings">
          <div class="ring"><svg viewBox="0 0 120 120" aria-hidden="true"><circle class="track" id="r1-track" cx="60" cy="60" r="50"/><circle class="val" id="r1-val" cx="60" cy="60" r="50"/></svg>
            <div class="txt"><span>检查通过</span><strong id="r1-num">–</strong></div></div>
          <div class="ring"><svg viewBox="0 0 120 120" aria-hidden="true"><circle class="track" id="r2-track" cx="60" cy="60" r="50"/><circle class="val" id="r2-val" cx="60" cy="60" r="50"/></svg>
            <div class="txt"><span>发送限速占用</span><strong id="r2-num">–</strong></div></div>
        </div>
      </section>
    </div>
    <section class="tiles" aria-label="收发数字（启动以来）">
      <div class="card tile"><strong id="t-recv">–</strong><span>收到消息</span></div>
      <div class="card tile"><strong id="t-sent">–</strong><span>发出消息</span></div>
      <div class="card tile"><strong id="t-fail">–</strong><span><i class="mark"></i>发送失败</span></div>
      <div class="card tile"><strong id="t-queue">–</strong><span>排队</span></div>
      <div class="card tile"><strong id="t-skip">–</strong><span>跳过的消息</span></div>
    </section>
    <div class="ov-mid">
      <section class="card checks-card" aria-labelledby="h-checks">
        <div class="head"><h2 id="h-checks"><svg class="i" aria-hidden="true"><use href="#i-shield"/></svg>检查项</h2><span class="sub" id="checks-sum">每 5 秒刷新</span></div>
        <ul class="checks" id="checks"></ul>
      </section>
      <section class="card counts-card" aria-labelledby="h-counts">
        <div class="head"><h2 id="h-counts"><svg class="i" aria-hidden="true"><use href="#i-bars"/></svg>收发计数</h2><span class="sub">启动以来</span></div>
        <h3 class="group">收到</h3><ul class="bars" id="bars-recv"></ul>
        <h3 class="group">跳过和发送</h3><ul class="bars" id="bars-other"></ul>
      </section>
    </div>
    <section class="card" aria-labelledby="h-sends">
      <div class="head"><h2 id="h-sends"><svg class="i" aria-hidden="true"><use href="#i-history"/></svg>最近发送</h2><span class="sub">最近 12 条 · 只记长度和类型，不记内容</span></div>
      <div class="sends"><ul id="sends"></ul></div>
    </section>
  </section>

  <section id="tab-setup" class="tab">
    <section class="card" aria-label="进度">
      <p class="lead" style="margin-top:0">还没有 <code>config.json</code> 或 <code>keys.json</code>，所以连接器先只开这个页面，不收也不发。
        两份都有了会自动切到正常模式（十几秒），然后去「设置」填白名单。</p>
      <ol class="stepper">
        <li id="st1"><span class="num">1</span><span><strong>扫码登录，同时抓密钥</strong><span class="st-text">读取中</span></span></li>
        <li id="st2"><span class="num">2</span><span><strong>填 AstrBot 地址和口令</strong><span class="st-text">读取中</span></span></li>
        <li id="st3"><span class="num">3</span><span><strong>自动切到正常模式</strong><span class="st-text">前两步做完，十几秒内自己切</span></span></li>
      </ol>
      <ul class="checks compact" id="setupchecks"></ul>
    </section>
    <section class="card" aria-labelledby="h-s1">
      <div class="head"><h2 id="h-s1"><span class="stepno">1</span>扫码登录，同时抓密钥</h2><span class="chip" id="s1chip">读取中</span></div>
      <div class="row nested" id="step1"></div>
    </section>
    <section class="card" aria-labelledby="h-s2">
      <div class="head"><h2 id="h-s2"><span class="stepno">2</span>填 AstrBot 地址和口令</h2><span class="chip" id="s2chip">读取中</span></div>
      <div class="nested" id="step2"></div>
    </section>
    <div class="note-dashed"><svg class="i" aria-hidden="true"><use href="#i-info"/></svg>两份都有了，连接器会在十几秒内切到正常模式，这页会自己重新载入成「概览」。</div>
  </section>

  <section id="tab-login" class="tab">
    <div class="row">
      <section class="card login-card" id="logincard" aria-labelledby="h-qr">
        <div class="head"><h2 id="h-qr"><svg class="i" aria-hidden="true"><use href="#i-scan"/></svg>扫码登录</h2>
          <span class="live" id="qrlive" hidden><span class="dot live"></span><span id="qrlivetext">每 3 秒更新一次</span></span></div>
        <div class="banner" id="loginbanner"><span id="loginicon"></span><div><strong id="loginstate">读取中</strong><span id="loginsub" hidden></span></div></div>
        <div class="qrstage" id="qrbox" hidden><div class="qrframe"><img id="qr" alt="微信登录窗口截图，用小号扫码"></div></div>
        <div class="qrempty" id="qrempty"><svg class="i" aria-hidden="true"><use href="#i-scan"/></svg><span id="qremptytext">点「显示二维码」才开始截图，之后每 3 秒刷新一次</span></div>
        <div class="actions center">
          <button class="btn primary" id="qrbtn" type="button" disabled>显示二维码</button>
          <a class="btn" href="../"><svg class="i" aria-hidden="true"><use href="#i-monitor"/></svg>在网页桌面里扫</a>
        </div>
        <p class="foot-note"><svg class="i" aria-hidden="true"><use href="#i-lock"/></svg><span>只截微信的登录窗口：认不出是登录窗口、已经登录了、或者旁边开着别的微信窗口时，一律不截。</span></p>
      </section>
      <section class="card" id="keycard" aria-labelledby="h-key">
        <h2 id="h-key"><svg class="i" aria-hidden="true"><use href="#i-key"/></svg>抓密钥</h2>
        <p class="lead run-only">数据库：<span id="dbstate">读取中</span></p>
        <p class="lead run-only">只是重新登录（没换号、手机上没退出）一般不用重抓密钥；数据库标红「密钥不能用」时按下面做：</p>
        <p class="lead setup-only">新版微信只在登录那一刻派生数据库密钥，所以要先挂上断点再扫码。微信已经登录的话，先在手机上退出这台电脑上的登录，等出现二维码再做：</p>
        <ol class="steps">
          <li><div class="stepbody"><span>在电脑的终端里运行，等它提示「现在扫码登录」：</span>
            <div class="cmd"><code id="cmd1">docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py capture</code><button type="button" data-copy="cmd1"><svg class="i" aria-hidden="true"><use href="#i-copy"/></svg>复制</button></div></div></li>
          <li><div class="stepbody"><span>用小号扫「扫码登录」里的二维码（也可以<a href="../">打开网页桌面</a>扫）。</span></div></li>
          <li><div class="stepbody"><span>抓到后运行，看到「校验通过 N/N 个库」就成功了：</span>
            <div class="cmd"><code id="cmd2">docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py verify</code><button type="button" data-copy="cmd2"><svg class="i" aria-hidden="true"><use href="#i-copy"/></svg>复制</button></div></div></li>
          <li class="run-only"><div class="stepbody"><span>回到这里重启连接器，让它读新密钥：</span>
            <button class="btn primary" id="restart2" type="button"><svg class="i" aria-hidden="true"><use href="#i-restart"/></svg>重启连接器</button></div></li>
          <li class="setup-only"><div class="stepbody"><span>成功后上面的「密钥」会标绿；两份文件都齐了会自动切到正常模式。</span></div></li>
        </ol>
        <p class="hint">抓密钥要 root 跑 gdb，连接器自己没这个权限，所以要在终端里运行。容器名不是 wechat 的话把命令里的换掉。</p>
      </section>
    </div>
  </section>

  <section id="tab-settings" class="tab">
    <section class="card" id="astrbotcard" aria-labelledby="h-ab">
      <div class="head"><h2 id="h-ab"><svg class="i" aria-hidden="true"><use href="#i-link"/></svg>连接 AstrBot</h2><span class="chip run-only" id="ab-chip">读取中</span></div>
      <div class="fields">
        <div>
          <div class="fld"><label for="ab-url">反向 WebSocket 地址</label><input id="ab-url" type="text" placeholder="ws://astrbot:6200/ws" spellcheck="false" autocomplete="off"></div>
          <p class="hint" id="ab-urlhint">AstrBot 容器名加平台里填的端口，比如 ws://astrbot:6200/ws</p>
        </div>
        <div>
          <div class="fld"><span class="lab"><label for="ab-token">口令（和 AstrBot 平台里填的一样）</label><span class="tokset" id="ab-tokset" hidden><svg class="i" aria-hidden="true"><use href="#i-lock"/></svg>已设置</span></span>
            <input id="ab-token" type="password" autocomplete="new-password" spellcheck="false"></div>
          <p class="hint">只写不读：页面上不显示已经保存的口令，留空就是不改</p>
        </div>
      </div>
      <div class="check-row setup-only"><input id="ab-ro" type="checkbox"><label for="ab-ro"><strong>先只收不发</strong><span>第一次部署建议开着，确认能收到消息再到「设置」里关掉</span></label></div>
      <div class="foot"><span class="sub run-only">保存后看「概览」的 AstrBot 一行：连不上会写原因</span><button class="btn primary" id="ab-save" type="button">保存连接并重启</button></div>
    </section>
    <div class="row run-only">
      <section class="card" aria-labelledby="h-tg">
        <div class="head"><h2 id="h-tg"><svg class="i" aria-hidden="true"><use href="#i-toggle"/></svg>收发开关</h2><span class="sub">改完在底部保存</span></div>
        <h3 class="group">收</h3><ul class="toggles" id="tg-recv"><li class="empty">读取中</li></ul>
        <h3 class="group">发</h3><ul class="toggles" id="tg-send"></ul>
      </section>
      <section class="card" aria-labelledby="h-num">
        <div class="head"><h2 id="h-num"><svg class="i" aria-hidden="true"><use href="#i-sliders"/></svg>发送和收图</h2></div>
        <div class="stack" id="numbers"></div>
      </section>
    </div>
    <section class="card run-only" aria-labelledby="h-wl">
      <div class="head"><h2 id="h-wl"><svg class="i" aria-hidden="true"><use href="#i-users"/></svg>白名单</h2><span class="sub">只有这一块会显示 wxid 和备注名</span></div>
      <div class="lists" id="lists"></div>
      <div class="lookup">
        <div class="bar">
          <div class="fld search-fld"><svg class="i" aria-hidden="true"><use href="#i-search"/></svg><div><label for="q">按名字查 wxid（备注名或昵称里带这几个字的好友和群）</label><input id="q" type="search" placeholder="名字" autocomplete="off"></div></div>
          <button class="btn" id="find" type="button">查</button>
        </div>
        <ul class="found" id="found"></ul>
      </div>
    </section>
    <div class="savebar run-only" id="savebar">
      <span class="dot"></span><strong id="dirty">没有改动</strong>
      <span class="sub">收发开关、数字和白名单一起保存；保存前会留一份 config.json.bak</span>
      <button class="btn" id="reset" type="button" disabled>撤销改动</button>
      <button class="btn primary" id="save" type="button" disabled>保存并重启</button>
    </div>
  </section>

  <section id="tab-log" class="tab">
    <section class="card toolbar" aria-label="筛选">
      <div class="seg" id="loglevel" role="group" aria-label="级别">
        <button type="button" data-min="0" aria-pressed="true">全部</button>
        <button type="button" data-min="2" aria-pressed="false">警告以上</button>
        <button type="button" data-min="3" aria-pressed="false">只看错误</button>
      </div>
      <div class="search"><svg class="i" aria-hidden="true"><use href="#i-search"/></svg><input id="logq" type="search" placeholder="筛选关键字" aria-label="筛选关键字"></div>
      <select id="loglines" aria-label="显示多少行">
        <option value="200">最后 200 行</option><option value="500">最后 500 行</option>
        <option value="1000">最后 1000 行</option><option value="2000">最后 2000 行</option>
      </select>
      <span class="follow"><label for="follow">跟到最新</label><button class="switch" id="follow" type="button" role="switch" aria-checked="true"></button></span>
      <span id="logcount"></span>
    </section>
    <section class="term" aria-label="日志内容">
      <div class="term-head"><span class="name"><svg class="i" aria-hidden="true"><use href="#i-term"/></svg>connector.log</span>
        <span class="live"><span class="dot live" id="logdot"></span><span id="loglive">跟到最新 · 每 3 秒刷新</span></span></div>
      <div class="logbody" id="logbox" tabindex="0">读取中</div>
      <button class="btn primary small" id="tolatest" type="button" hidden><svg class="i" aria-hidden="true"><use href="#i-down"/></svg>回到最新</button>
    </section>
  </section>

  </div>
</main>
</div>
<div id="notice" role="status" aria-live="polite"></div>
<script>
const TOKEN = document.querySelector('meta[name="wxc-token"]').content;
const TABS = { run: ["overview", "login", "settings", "log"], setup: ["setup", "log"] };
const TITLES = { overview: "概览", setup: "首次配置", login: "登录", settings: "设置", log: "日志" };
const WORD = { ok: "正常", warn: "注意", bad: "出错" };
const MARK = { ok: "check", warn: "alert", bad: "x" };
const COLOR = { ok: ["var(--ok-mark)", "var(--ok-bg)"], warn: ["var(--warn-mark)", "var(--warn-bg)"], bad: ["var(--bad-mark)", "var(--bad-bg)"] };
const VOICE = { native: "原生语音条", file: "mp3 文件" };
const FIX = {  // 检查项出问题时给一个去处
  "微信窗口": c => c.text.includes("登录") && ["login", "去扫码"],
  "数据库": c => c.text.includes("密钥") && ["login", "去抓密钥"],
  "AstrBot": c => c.level === "bad" && ["settings", "去设置"],
};
const $ = id => document.getElementById(id);
const narrow = window.matchMedia("(max-width: 899px)");
let mode = "", busy = false, alerting = false, leaving = false, qrOn = false;
let settings = null, edit = null, names = {};
const drawList = {};
let logLines = [], logMin = 0, follow = true;

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = String(text);
  return e;
}
function icon(name, size) {
  const ns = "http://www.w3.org/2000/svg", s = document.createElementNS(ns, "svg"), u = document.createElementNS(ns, "use");
  s.setAttribute("class", "i");
  s.setAttribute("aria-hidden", "true");
  if (size) { s.style.width = size + "px"; s.style.height = size + "px"; }
  u.setAttribute("href", "#i-" + name);
  s.append(u);
  return s;
}
function label(btn, name, text) { btn.replaceChildren(icon(name), document.createTextNode(text)); }
function notice(text) {
  const n = $("notice");
  n.textContent = text; n.style.display = "block";
  clearTimeout(notice.t); notice.t = setTimeout(() => { n.style.display = "none"; }, 5000);
}
async function getJSON(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(r.status);
  return r.json();
}
async function post(path, data) {
  const r = await fetch(path, { method: "POST", cache: "no-store",
    headers: { "Content-Type": "application/json", "X-WXC-Token": TOKEN }, body: JSON.stringify(data) });
  let j = {};
  try { j = await r.json(); } catch (e) { j = { error: r.status === 403 ? "页面过期了（连接器重启过），刷新一下" : "出错了（" + r.status + "）" }; }
  if (!r.ok) throw new Error(j.error || r.status);
  return j;
}
function afterRestart() {
  leaving = true;
  notice("连接器正在重启，过一会儿自动刷新页面");
  setTimeout(() => location.reload(), mode === "setup" ? 5000 : 12000);
}
function clock(ts) { return new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false }); }
function setOverall(level, text) { $("overall").className = "status " + level; $("overalltext").textContent = text; }
function setChip(chip, level, text) { chip.className = "chip " + level; chip.textContent = text; }
function stateText(dd, level, text) {
  const s = el("span", "state " + level);
  s.append(el("span", "dot"), document.createTextNode(text));
  dd.replaceChildren(level ? s : document.createTextNode(text));
}

// ---------- 主题、侧栏 ----------
const THEMES = ["auto", "light", "dark"], THEME_NAME = { auto: "跟随系统", light: "浅色", dark: "深色" };
function theme() { try { return localStorage.getItem("wxc-theme") || "auto"; } catch (e) { return "auto"; } }
function applyTheme(t) {
  if (t === "auto") delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t;
  $("themename").textContent = THEME_NAME[t];
}
$("theme").onclick = () => {
  const t = THEMES[(THEMES.indexOf(theme()) + 1) % THEMES.length];
  try { localStorage.setItem("wxc-theme", t); } catch (e) {}
  applyTheme(t);
};
function syncMenu() {
  const open = narrow.matches ? document.body.classList.contains("drawer") : !document.documentElement.classList.contains("side-off");
  $("menu").setAttribute("aria-expanded", String(open));
}
function setDrawer(open) {
  document.body.classList.toggle("drawer", open);
  $("scrim").hidden = !open;
  syncMenu();
}
$("menu").onclick = () => {
  if (narrow.matches) { setDrawer(!document.body.classList.contains("drawer")); return; }
  const off = document.documentElement.classList.toggle("side-off");  // 宽屏：收起或展开侧栏，记在本机浏览器里
  try { localStorage.setItem("wxc-side", off ? "0" : "1"); } catch (e) {}
  syncMenu();
};
$("scrim").onclick = () => setDrawer(false);
$("sideclose").onclick = () => setDrawer(false);
document.querySelectorAll(".nav a").forEach(a => a.addEventListener("click", () => setDrawer(false)));  // 点的是当前页时不触发 hashchange
narrow.addEventListener("change", () => setDrawer(false));
document.addEventListener("keydown", e => { if (e.key === "Escape" && document.body.classList.contains("drawer")) setDrawer(false); });

// ---------- 标签 ----------
function currentTab() {
  const list = TABS[mode] || TABS.run, t = location.hash.slice(1);
  return list.includes(t) ? t : list[0];
}
function showTab() {
  const t = currentTab();
  document.querySelectorAll(".nav a").forEach(a => {
    const on = a.dataset.tab === t;
    a.classList.toggle("on", on);
    if (on) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
  });
  document.querySelectorAll(".tab").forEach(s => s.classList.toggle("on", s.id === "tab-" + t));
  $("title").textContent = TITLES[t];
  setDrawer(false);
  if (t === "log") refreshLog();
  if (t === "login" || t === "setup") refreshLogin(); else stopQr();
  if ((t === "settings" || t === "setup") && !settings) loadSettings();
}
window.addEventListener("hashchange", () => { window.scrollTo(0, 0); showTab(); });
function setMode(m) {
  if (m === mode) return;
  if (mode === "setup") { leaving = true; location.reload(); return; }  // 首次配置做完切到正常模式：口令换了、卡片也要挪回去，直接重新载入
  mode = m;
  document.body.classList.toggle("setup", m === "setup");
  document.querySelectorAll(".nav a").forEach(a => { a.hidden = !TABS[m].includes(a.dataset.tab); });
  if (m === "setup") {  // 首次配置：把登录、抓密钥、连接 AstrBot 三张卡片挪进「首次配置」
    $("step1").append($("logincard"), $("keycard"));
    $("step2").append($("astrbotcard"));
    $("ab-save").textContent = "保存";
  }
  showTab();
}

// ---------- 概览 ----------
async function restart() {
  if (busy) { notice("正在发消息，等发完再重启"); return; }
  if (!confirm("重启连接器？大约十几秒收不到、发不了消息，之后自动补收这期间的新消息。")) return;
  try { await post("api/restart", {}); afterRestart(); } catch (e) { notice("没重启：" + e.message); }
}
$("restart").onclick = restart;
$("restart2").onclick = restart;
$("pause").onclick = async () => {
  const stop = $("pause").dataset.paused !== "1";
  if (stop && !confirm("暂停发送？暂停期间 AstrBot 要发的消息会被拒，恢复以后也不会补发。收消息不受影响。")) return;
  try { await post("api/stop", { stop }); notice(stop ? "已暂停发送" : "已恢复发送"); refresh(); }
  catch (e) { notice("没成功：" + e.message); }
};
$("test").onclick = async () => {
  if (!confirm("给「文件传输助手」发一条测试消息？内容是固定的一句话（版本号和时间），和 AstrBot 的消息排同一个队。")) return;
  try {
    const j = await post("api/test", {});
    notice(j.ahead ? "已排进发送队列，前面还有 " + j.ahead + " 条；结果看「最近发送」" : "正在发，结果看「最近发送」");
    [3000, 8000, 15000, 25000].forEach(t => setTimeout(refresh, t));
  } catch (e) { notice("没发：" + e.message); }
};
function alertPref() { try { return localStorage.getItem("wxc-notify") === "1"; } catch (e) { return false; } }
function showNotifyButton() {
  const on = alertPref() && "Notification" in window && Notification.permission === "granted";
  label($("notify"), "bell", on ? "关闭掉线通知" : "开启掉线通知");
}
$("notify").onclick = async () => {
  if (!("Notification" in window)) { notice("这个浏览器不支持通知"); return; }
  if (alertPref()) { try { localStorage.setItem("wxc-notify", "0"); } catch (e) {} showNotifyButton(); return; }
  const p = await Notification.requestPermission();
  if (p !== "granted") { notice("浏览器没给通知权限"); return; }
  try { localStorage.setItem("wxc-notify", "1"); } catch (e) {}
  showNotifyButton(); notice("开好了：这个页面开着时，出问题会弹通知（同一次问题只弹一次）");
};
function checkAlert(s) {
  // 掉线：有标红的项，或者窗口小得像登录窗口
  const bad = s.checks.filter(c => c.level === "bad" || (c.name === "微信窗口" && c.level === "warn"));
  const now = bad.length > 0;
  document.title = now ? "⚠ 有问题 · 微信连接器" : "微信连接器";
  if (now && !alerting && alertPref() && "Notification" in window && Notification.permission === "granted") {
    new Notification("微信连接器出问题了", { body: bad.map(c => c.name + "：" + c.text).join("\n") });
  }
  alerting = now;
}
function renderChecks(box, checks, links) {
  box.replaceChildren(...checks.map(c => {
    const li = el("li"), ic = el("span", "ic " + c.level), body = el("div", "body");
    ic.append(icon(MARK[c.level]));
    body.append(el("b", "", c.name), el("span", "", c.text));
    const fix = links && c.level !== "ok" && FIX[c.name] && FIX[c.name](c);
    if (fix) { const a = el("a", "go", fix[1] + " →"); a.href = "#" + fix[0]; body.append(a); }
    li.append(ic, body, el("span", "chip " + c.level, WORD[c.level]));
    return li;
  }));
}
function setRing(n, frac, color, track, num, unit) {
  const v = $("r" + n + "-val");
  frac = Math.max(0, Math.min(1, frac || 0));
  v.style.strokeDasharray = (frac * 314.16).toFixed(1) + " 314.16";
  v.style.visibility = frac > 0 ? "visible" : "hidden";
  v.style.stroke = color;
  $("r" + n + "-track").style.stroke = track;
  $("r" + n + "-num").replaceChildren(document.createTextNode(num), el("small", "", unit));
}
function renderCounts(counts) {
  const recv = [], other = [];
  Object.entries(counts).forEach(([k, v]) => (k.startsWith("私聊·") || k.startsWith("群·") ? recv : other).push([k, v]));
  recv.sort((a, b) => b[1] - a[1]);
  const max = Math.max(1, ...recv.map(x => x[1]), ...other.map(x => x[1]));
  const draw = (list, box) => box.replaceChildren(...(list.length ? list.map(([k, v]) => {
    const li = el("li"), track = el("span", "track"), fill = el("span", "fill");
    li.title = k + "：" + v + " 条";
    fill.style.width = Math.max(2, Math.round(v / max * 100)) + "%";
    if (k === "发送失败") fill.style.background = "var(--bad-mark)";
    else if (k.startsWith("跳过·")) fill.style.background = "var(--switch-off)";
    track.append(fill);
    li.append(el("span", "name", k), track, el("span", "n", v));
    return li;
  }) : [el("li", "empty", "还没有")]));
  draw(recv, $("bars-recv"));
  draw(other, $("bars-other"));
}
function renderSends(list) {
  const head = el("li", "headrow");
  head.setAttribute("aria-hidden", "true");
  ["时间", "发给", "内容", "结果"].forEach(x => head.append(el("span", "", x)));
  $("sends").replaceChildren(head, ...(list.length ? list.map(x => {
    const li = el("li"), res = el("span", "res " + (x.ok ? "ok" : "bad"));
    res.append(icon(x.ok ? "check" : "x"), el("span", "", x.ok ? "已发出" : "没发出：" + (x.reason || "失败")));
    li.append(el("span", "t", x.ago + "前"), el("span", "to", x.to), el("span", "what", x.what), res);
    return li;
  }) : [el("li", "empty", "启动后还没发过")]));
}
function renderOverview(s) {
  const w = s.wechat, a = s.astrbot, sd = s.send, cfg = s.config;
  const acc = !w.running ? ["bad", "微信没在运行"] : w.login ? ["warn", "停在登录窗口"] : !w.checked ? ["", "还没查过"]
    : w.window ? ["ok", "已登录"] : ["bad", "找不到窗口"];
  $("acc-dot").className = "online " + acc[0];
  setChip($("acc-chip"), acc[0], acc[1]);
  $("acc-id").textContent = "OneBot 号 " + s.self_id;
  $("run").replaceChildren(...[["版本", s.version], ["已运行", s.uptime], ["私聊", cfg["私聊"]], ["群", cfg["群"]],
    ["语音", VOICE[cfg["语音"]] || cfg["语音"]]].map(([k, v]) => { const d = el("div"); d.append(el("dt", "", k), el("dd", "", v)); return d; }));

  $("ab-url-v").textContent = a.url;
  if (s.receive_only) stateText($("ab-state-v"), "", "只收不发，不连");
  else if (a.connected) stateText($("ab-state-v"), "ok", "已连上 " + a.since);
  else stateText($("ab-state-v"), "bad", "已经 " + a.since + "没连上：" + a.error);
  $("ab-drops").textContent = a.disconnects + " 次";
  $("ab-token-v").textContent = a.token_set ? "已设置" : "没设置";
  $("ab-buf").textContent = a.buffered + " 条";
  if (sd.paused) stateText($("sd-state"), "warn", "已暂停");
  else stateText($("sd-state"), "", sd.sending ? "正在发" : sd.queued ? "排队中" : "空闲");
  $("sd-queue").textContent = sd.queued + " 条";
  $("sd-minute").textContent = sd.last_minute + " / " + sd.per_minute + " 条";
  $("sd-ro").textContent = s.receive_only ? "开" : "关";
  const pause = $("pause");
  pause.dataset.paused = s.paused ? "1" : "0";
  label(pause, s.paused ? "play" : "pause", s.paused ? "恢复发送" : "暂停发送");
  const why = s.receive_only ? "只收不发模式不发消息" : s.paused ? "已暂停发送" : s.test_pending ? "上一条测试消息还没发完" : "";
  $("test").disabled = !!why;
  $("test").title = why || "给「文件传输助手」发一条固定的测试消息";
  $("testhint").textContent = why || "测试消息只发给「文件传输助手」";

  const okN = s.checks.filter(c => c.level === "ok").length, total = s.checks.length;
  setRing(1, okN / total, COLOR[s.level][0], COLOR[s.level][1], okN, "/" + total);
  const pct = sd.per_minute ? Math.round(sd.last_minute / sd.per_minute * 100) : 0;
  const rc = pct >= 80 ? COLOR.warn : ["var(--accent)", "var(--accent-soft)"];
  setRing(2, pct / 100, rc[0], rc[1], Math.min(pct, 100), "%");

  const counts = s.counts;
  const sum = pre => Object.entries(counts).filter(([k]) => pre.some(p => k.startsWith(p))).reduce((n, [, v]) => n + v, 0);
  $("t-recv").textContent = sum(["私聊·", "群·"]);
  $("t-sent").textContent = counts["发出"] || 0;
  $("t-fail").textContent = counts["发送失败"] || 0;
  $("t-queue").textContent = sd.queued;
  $("t-skip").textContent = sum(["跳过·"]);
  $("checks-sum").textContent = (okN < total ? okN + " 项正常 · " + (total - okN) + " 项要看" : total + " 项都正常") + " · 每 5 秒刷新";
  renderChecks($("checks"), s.checks, true);
  renderCounts(counts);
  renderSends(s.recent_sends);

  const db = s.checks.find(c => c.name === "数据库");
  if (db) { $("dbstate").className = "state " + db.level; $("dbstate").textContent = db.text; }
  if (s.receive_only) setChip($("ab-chip"), "", "只收不发，不连");
  else if (a.connected) setChip($("ab-chip"), "ok", "已连上 " + a.since);
  else setChip($("ab-chip"), "bad", "没连上：" + a.error);
}
function setStep(n, done, now, text) {
  const li = $("st" + n);
  li.className = done ? "done" : now ? "now" : "";
  li.querySelector(".num").replaceChildren(done ? icon("check") : document.createTextNode(n));
  li.querySelector(".st-text").textContent = text;
}
function renderSetup(s) {
  document.title = "首次配置 · 微信连接器";
  renderChecks($("setupchecks"), s.checks, false);
  const keys = !!s.files["keys.json"];
  const conf = !!s.files["config.json"] && !s.checks.some(c => c.name === "配置" && c.level === "bad");
  const left = (keys ? 0 : 1) + (conf ? 0 : 1);
  setOverall(left ? "warn" : "ok", left ? "首次配置 · 还差 " + left + " 步" : "首次配置 · 马上切到正常模式");
  setStep(1, keys, !keys, keys ? "keys.json 已经有了" : "还没有 keys.json");
  setStep(2, conf, keys && !conf, conf ? "config.json 已经有了" : "还没有 config.json");
  setStep(3, false, keys && conf, keys && conf ? "马上就切" : "前两步做完，十几秒内自己切");
  setChip($("s1chip"), keys ? "ok" : "accent", keys ? "做完了" : "正在做");
  setChip($("s2chip"), conf ? "ok" : keys ? "accent" : "", conf ? "做完了" : keys ? "正在做" : "还没填");
}
async function refresh() {
  let s;
  try { s = await getJSON("api/status"); }
  catch (e) {
    setOverall("bad", "连接器没响应");
    document.title = "⚠ 没响应 · 微信连接器";
    return;
  }
  setMode(s.mode);
  if (mode !== s.mode) return;  // 正在重新载入
  busy = s.busy;
  $("ver").textContent = s.version;
  $("stamp").textContent = clock(s.now) + " 刷新";
  if (s.mode === "setup") { renderSetup(s); return; }
  const bad = s.checks.filter(c => c.level === "bad"), warn = s.checks.filter(c => c.level === "warn");
  setOverall(s.level, bad.length ? bad.length + " 项出错：" + bad.map(c => c.name).join("、")
    : warn.length ? warn.length + " 项要留意" : "一切正常");
  checkAlert(s);
  renderOverview(s);
}

// ---------- 登录 ----------
const LOGIN_LOOK = { qr: ["accent", "scan"], confirm: ["warn", "phone"], loading: ["accent", "loader"], offline: ["bad", "offline"] };
function setBanner(cls, ic, title, sub) {
  $("loginbanner").className = "banner " + cls;
  $("loginicon").replaceChildren(icon(ic));
  $("loginstate").textContent = title;
  $("loginsub").textContent = sub;
  $("loginsub").hidden = !sub;
}
function stopQr() {
  qrOn = false;
  $("qrbox").hidden = true;
  $("qrempty").hidden = false;
  $("qrlive").hidden = true;
  $("qrbtn").textContent = "显示二维码";
  $("qrbtn").classList.add("primary");
}
$("qrbtn").onclick = () => {
  if (qrOn) { stopQr(); return; }
  qrOn = true;
  $("qrbtn").textContent = "收起二维码";
  $("qrbtn").classList.remove("primary");
  refreshLogin(true);
};
$("qr").onerror = () => { $("qrlivetext").textContent = "这次没截到，过几秒自动再试"; };
$("qr").onload = () => { $("qrlivetext").textContent = "每 3 秒更新 · " + clock(Date.now() / 1000); $("qrlive").hidden = false; };
async function refreshLogin(force) {
  let s;
  try { s = await getJSON("api/login"); } catch (e) { setBanner("bad", "x", "读不到登录窗口的状态", ""); return; }
  if (s.login) {
    const look = LOGIN_LOOK[s.state] || LOGIN_LOOK.qr;
    setBanner(look[0], look[1], s.text, s.others ? "旁边还开着别的微信窗口，不截图，去网页桌面看" : "微信停在登录窗口");
  } else {
    setBanner(s.running ? "ok" : "bad", s.running ? "check" : "x", s.text, "");
  }
  $("qremptytext").textContent = !s.login ? "没有登录窗口，不用扫码"
    : s.others ? "旁边开着别的微信窗口，不截图" : "点「显示二维码」才开始截图，之后每 3 秒刷新一次";
  $("qrbtn").disabled = !s.login && !qrOn;
  if (!qrOn) return;
  if (s.login && !s.others) {
    $("qr").src = "api/login/qr?t=" + Date.now();
    $("qrbox").hidden = false;
    $("qrempty").hidden = true;
  } else {
    $("qrbox").hidden = true;
    $("qrempty").hidden = false;
    if (!s.login) { stopQr(); if (!force) notice("登录窗口没了：" + s.text); }
  }
}
document.querySelectorAll("[data-copy]").forEach(b => b.onclick = async () => {
  try { await navigator.clipboard.writeText($(b.dataset.copy).textContent); notice("已复制"); }
  catch (e) { notice("复制不了，手动选中复制吧"); }
});

// ---------- 设置 ----------
const SEND_KEYS = ["receive_only", "reply_mention_when_busy"];
const LIST_HINT = { whitelist: "填 wxid；只填 * 就是所有好友", group_whitelist: "填群 id，以 @chatroom 结尾", bot_names: "群里有人这样叫，也算叫了小号" };
function fillAstrbot(a) {
  $("ab-url").value = a.url;
  $("ab-url").disabled = a.locked;
  if (a.locked) $("ab-urlhint").textContent = "地址里带了参数或账号（可能有口令），页面上不显示全，也不能在这里改，要改请改 config.json";
  $("ab-token").value = "";
  $("ab-token").placeholder = a.token_set ? "已设置，留空不改" : "还没设置";
  $("ab-tokset").hidden = !a.token_set;
  $("ab-ro").checked = a.receive_only;
}
async function loadSettings() {
  try { settings = await getJSON("api/settings"); }
  catch (e) { $("tg-recv").replaceChildren(el("li", "empty", "读不了设置")); return; }
  fillAstrbot(settings.astrbot);
  if (!settings.setup) resetEdit();
}
function resetEdit() {
  edit = { toggles: {}, numbers: {}, choices: {}, lists: {} };
  names = {};
  settings.toggles.forEach(t => { edit.toggles[t.key] = t.value; });
  settings.numbers.forEach(n => { edit.numbers[n.key] = n.value; });
  settings.choices.forEach(c => { edit.choices[c.key] = c.value; });
  settings.lists.forEach(l => { edit.lists[l.key] = l.value.slice(); Object.assign(names, l.names); });
  renderToggles();
  renderNumbers();
  renderLists();
  renderDirty();
}
function changes() {
  const out = {};
  if (!edit) return out;
  settings.toggles.forEach(t => { if (edit.toggles[t.key] !== t.value) out[t.key] = edit.toggles[t.key]; });
  settings.numbers.forEach(n => { if (edit.numbers[n.key] !== n.value) out[n.key] = edit.numbers[n.key]; });
  settings.choices.forEach(c => { if (edit.choices[c.key] !== c.value) out[c.key] = edit.choices[c.key]; });
  settings.lists.forEach(l => { if (edit.lists[l.key].join("\n") !== l.value.join("\n")) out[l.key] = edit.lists[l.key]; });
  return out;
}
function renderDirty() {
  const n = Object.keys(changes()).length;
  $("savebar").classList.toggle("dirty", n > 0);
  $("dirty").textContent = n ? "有 " + n + " 处改动没保存" : "没有改动";
  $("save").disabled = !n;
  $("reset").disabled = !n;
}
function renderToggles() {
  const row = t => {
    const li = el("li"), lab = el("label", "", t.label), sw = el("button", "switch"), mark = el("span", "changed", "改了");
    sw.type = "button";
    sw.id = "tg-" + t.key;
    sw.setAttribute("role", "switch");
    lab.htmlFor = sw.id;
    lab.append(mark);
    const sync = () => { sw.setAttribute("aria-checked", String(edit.toggles[t.key])); mark.hidden = edit.toggles[t.key] === t.value; };
    sw.onclick = () => { edit.toggles[t.key] = !edit.toggles[t.key]; sync(); renderDirty(); };
    sync();
    li.append(lab, sw);
    return li;
  };
  $("tg-recv").replaceChildren(...settings.toggles.filter(t => !SEND_KEYS.includes(t.key)).map(row));
  $("tg-send").replaceChildren(...settings.toggles.filter(t => SEND_KEYS.includes(t.key)).map(row));
}
function renderNumbers() {
  const nums = settings.numbers.map(n => {
    const f = el("div", "fld numfld"), d = el("div"), lab = el("label", "", n.label), inp = el("input");
    inp.type = "number"; inp.min = n.min; inp.max = n.max; inp.step = 1; inp.value = n.value; inp.id = "num-" + n.key;
    inp.inputMode = "numeric";
    lab.htmlFor = inp.id;
    inp.oninput = () => { edit.numbers[n.key] = inp.value === "" ? null : Number(inp.value); renderDirty(); };
    d.append(lab, inp);
    f.append(d, el("span", "range", n.min + "–" + n.max));
    return f;
  });
  const choices = settings.choices.map(c => {
    const wrap = el("div"), lab = el("span", "choice-label", c.label), seg = el("div", "seg");
    lab.id = "ch-" + c.key;
    seg.setAttribute("role", "group");
    seg.setAttribute("aria-labelledby", lab.id);
    const btns = Object.entries(c.options).map(([v, text]) => {
      const b = el("button", "", text);
      b.type = "button";
      b.onclick = () => { edit.choices[c.key] = v; btns.forEach(([x, bb]) => bb.setAttribute("aria-pressed", String(x === v))); renderDirty(); };
      return [v, b];
    });
    btns.forEach(([v, b]) => { b.setAttribute("aria-pressed", String(edit.choices[c.key] === v)); seg.append(b); });
    wrap.append(lab, seg);
    return wrap;
  });
  $("numbers").replaceChildren(...nums, ...choices);
}
function renderLists() {
  $("lists").replaceChildren(...settings.lists.map(l => {
    const title = l.label.split("（")[0], chips = l.key === "bot_names";
    const box = el("div", "lst"), top = el("div", "top"), count = el("span", "count"), items = el(chips ? "div" : "ul", chips ? "chips" : "");
    const add = el("div", "add"), inp = el("input"), btn = el("button");
    top.append(el("h3", "", title), count);
    inp.type = "text"; inp.spellcheck = false; inp.autocomplete = "off";
    inp.placeholder = l.key === "whitelist" ? "添加 wxid" : l.key === "group_whitelist" ? "添加 …@chatroom" : "添加名字";
    inp.setAttribute("aria-label", "加到「" + title + "」");
    if (!chips) inp.classList.add("mono");
    btn.type = "button";
    btn.setAttribute("aria-label", "加到「" + title + "」");
    btn.append(icon("plus"));
    const draw = () => {
      const list = edit.lists[l.key];
      count.textContent = list.length + " 个";
      if (!list.length) { items.replaceChildren(el(chips ? "span" : "li", "empty", "还没有")); return; }
      items.replaceChildren(...list.map(x => {
        const rm = el("button", "x"), who = names[x] || "";
        rm.type = "button";
        rm.append(icon("x", 16));
        rm.setAttribute("aria-label", "从「" + title + "」去掉 " + (who || x));
        rm.onclick = () => { edit.lists[l.key] = edit.lists[l.key].filter(y => y !== x); draw(); renderDirty(); };
        if (chips) { const c = el("span", "name", x); c.append(rm); return c; }
        const li = el("li"), w = el("div", "who");
        if (x === "*") w.append(el("b", "", "所有好友"), el("code", "", "*"));
        else if (who) w.append(el("b", "", who), el("code", "", x));
        else w.append(el("code", "only", x));
        li.append(w, rm);
        return li;
      }));
    };
    const doAdd = () => {
      const v = inp.value.trim();
      if (!v) return;
      const err = chips ? (v.length > 40 ? "名字太长" : "")
        : /\s/.test(v) ? "wxid 里不能有空格"
        : l.key === "group_whitelist" && !v.endsWith("@chatroom") ? "群的 id 要以 @chatroom 结尾"
        : l.key === "whitelist" && v.endsWith("@chatroom") ? "这是群，要加到群白名单" : "";
      if (err) { notice(err); return; }
      if (!edit.lists[l.key].includes(v)) edit.lists[l.key].push(v);
      inp.value = "";
      draw();
      renderDirty();
    };
    btn.onclick = doAdd;
    inp.onkeydown = e => { if (e.key === "Enter") doAdd(); };
    drawList[l.key] = draw;
    draw();
    add.append(inp, btn);
    box.append(top, el("span", "sub", LIST_HINT[l.key] || ""), items, add);
    return box;
  }));
}
$("ab-save").onclick = async () => {
  if (!settings) { notice("设置还没读出来，等一下"); return; }
  if (busy) { notice("正在发消息，等发完再保存"); return; }
  const data = {}, token = $("ab-token").value;
  if (!settings.astrbot.locked) data.astrbot_ws_url = $("ab-url").value.trim();
  if (token) data.access_token = token;
  if (mode === "setup") data.receive_only = $("ab-ro").checked;
  if (!confirm(mode === "setup" ? "保存到 config.json？" : "保存 AstrBot 地址和口令，并重启连接器？")) return;
  try { await post("api/settings", data); $("ab-token").value = ""; afterRestart(); }
  catch (e) { notice("没保存：" + e.message); }
};
$("save").onclick = async () => {
  if (busy) { notice("正在发消息，等发完再保存"); return; }
  const data = changes(), n = Object.keys(data).length;
  if (!n) { notice("没有改动"); return; }
  if (!confirm("保存这 " + n + " 处改动，并重启连接器？")) return;
  try { await post("api/settings", data); afterRestart(); } catch (e) { notice("没保存：" + e.message); }
};
$("reset").onclick = () => { if (settings && !settings.setup) { resetEdit(); notice("改动都撤销了"); } };
async function find() {
  const q = $("q").value.trim(), out = $("found");
  if (!q) return;
  try {
    const list = await getJSON("api/lookup?q=" + encodeURIComponent(q));
    out.replaceChildren(...(list.length ? list.map(x => {
      const li = el("li"), key = x.group ? "group_whitelist" : "whitelist", b = el("button", "btn small soft");
      b.type = "button";
      label(b, "plus", x.group ? "加到群白名单" : "加到私聊白名单");
      b.onclick = () => {
        if (!edit) { notice("白名单还没读出来，等一下"); return; }
        names[x.wxid] = x.name;
        if (!edit.lists[key].includes(x.wxid)) edit.lists[key].push(x.wxid);
        drawList[key]();
        renderDirty();
        notice("已加进去，点底部「保存并重启」才生效");
      };
      li.append(el("span", "kind", x.group ? "群" : "好友"), el("strong", "", x.name), el("code", "", x.wxid), b);
      return li;
    }) : [el("li", "empty", "没找到")]));
  } catch (e) { out.replaceChildren(el("li", "empty", "查不了")); }
}
$("find").onclick = find;
$("q").onkeydown = e => { if (e.key === "Enter") find(); };
window.addEventListener("beforeunload", e => {
  if (!leaving && Object.keys(changes()).length) { e.preventDefault(); e.returnValue = ""; }
});

// ---------- 日志 ----------
const LV = { DEBUG: 0, INFO: 1, WARNING: 2, ERROR: 3, CRITICAL: 3 };
async function refreshLog() {
  try {
    const r = await fetch("api/log?lines=" + $("loglines").value, { cache: "no-store" });
    logLines = (await r.text()).split("\n");
    renderLog();
  } catch (e) {}
}
function renderLog() {
  const q = $("logq").value.trim().toLowerCase(), box = $("logbox"), out = [];
  let lv = 1;
  for (const line of logLines) {
    const m = /^(\d\d-\d\d \d\d:\d\d:\d\d) (DEBUG|INFO|WARNING|ERROR|CRITICAL) (.*)$/.exec(line);
    if (m) lv = LV[m[2]];  // 没有时间和级别的行（报错的调用栈）跟着上一行算
    if (lv < logMin || (q && !line.toLowerCase().includes(q))) continue;
    const d = el("div", "ln l" + lv + (m ? "" : " cont"));
    if (m) d.append(el("span", "t", m[1]), el("span", "lv", m[2]), el("span", "m", m[3]));
    else d.append(el("span", "t", ""), el("span", "lv", ""), el("span", "m", line || " "));
    out.push(d);
  }
  box.replaceChildren(...(out.length ? out : [el("div", "empty", "没有符合条件的行")]));
  $("logcount").textContent = "显示 " + out.length + " / " + logLines.length + " 行";
  if (follow) box.scrollTop = box.scrollHeight;
}
function setFollow(on) {
  follow = on;
  $("follow").setAttribute("aria-checked", String(on));
  $("tolatest").hidden = on;
  $("logdot").classList.toggle("live", on);
  $("loglive").textContent = on ? "跟到最新 · 每 3 秒刷新" : "已停在这里，不再滚动";
  if (on) $("logbox").scrollTop = $("logbox").scrollHeight;
}
$("logbox").addEventListener("scroll", () => {
  const b = $("logbox"), bottom = b.scrollTop + b.clientHeight >= b.scrollHeight - 20;
  if (bottom !== follow) setFollow(bottom);  // 往上翻就先不跟，翻回底部接着跟
});
$("follow").onclick = () => setFollow(!follow);
$("tolatest").onclick = () => setFollow(true);
document.querySelectorAll("#loglevel button").forEach(b => b.onclick = () => {
  logMin = Number(b.dataset.min);
  document.querySelectorAll("#loglevel button").forEach(x => x.setAttribute("aria-pressed", String(x === b)));
  renderLog();
});
$("logq").oninput = renderLog;
$("loglines").onchange = refreshLog;

applyTheme(theme());
showNotifyButton();
syncMenu();
showTab();  // 先按正常模式显示；拿到状态后是首次配置的话再换
refresh();
setInterval(refresh, 5000);
setInterval(() => {
  if (document.hidden) return;
  const t = currentTab();
  if (t === "log") refreshLog();
  if (t === "login" || t === "setup") refreshLogin();
}, 3000);
</script>
</body>
</html>
"""
