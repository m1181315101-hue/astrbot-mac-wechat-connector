"""状态页：微信进程和窗口、数据库密钥、AstrBot 连接、发送队列、收消息计数、运行日志；
扫码登录（登录窗口截图）、暂停发送、发测试消息、改常用设置和 AstrBot 地址口令、重启连接器

监听容器里的 127.0.0.1:6299（webui_host / webui_port），只有同一个容器里的 nginx 能访问到；
nginx 把网页桌面（3001 端口，有密码）的 /wxc/ 转过来，所以打开 https://localhost:3001/wxc/ 就能看，密码和网页桌面一样。
页面分「概览 / 登录 / 设置 / 日志」四页；首次配置（还没有 config.json 或 keys.json）时只有「首次配置 / 日志」。
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
    if await asyncio.to_thread(wechat_running):
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
    return {
        "mode": "run", "version": c.version, "now": now, "uptime": ago(now - c.started), "level": level,
        "paused": paused, "busy": bool(queued), "receive_only": bool(c.cfg["receive_only"]),
        "test_pending": c.test_pending,
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
try { const t = localStorage.getItem("wxc-theme"); if (t === "light" || t === "dark") document.documentElement.dataset.theme = t; } catch (e) {}
</script>
<style>
:root {
  --bg: #f6f7f9; --card: #ffffff; --text: #1d2129; --muted: #6b7280; --line: #e5e7eb;
  --ok: #16a34a; --ok-bg: #dcfce7; --warn: #b45309; --warn-bg: #fef3c7; --bad: #dc2626; --bad-bg: #fee2e2;
  --code: #f3f4f6; color-scheme: light;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #111317; --card: #1a1d23; --text: #e5e7eb; --muted: #9ca3af; --line: #2b2f36;
    --ok: #4ade80; --ok-bg: #14301f; --warn: #fbbf24; --warn-bg: #3a2e10; --bad: #f87171; --bad-bg: #3b1717;
    --code: #0d0f12; color-scheme: dark;
  }
}
:root[data-theme="dark"] {
  --bg: #111317; --card: #1a1d23; --text: #e5e7eb; --muted: #9ca3af; --line: #2b2f36;
  --ok: #4ade80; --ok-bg: #14301f; --warn: #fbbf24; --warn-bg: #3a2e10; --bad: #f87171; --bad-bg: #3b1717;
  --code: #0d0f12; color-scheme: dark;
}
* { box-sizing: border-box; }
[hidden] { display: none !important; }
html { -webkit-text-size-adjust: 100%; }
body { margin: 0; background: var(--bg); color: var(--text);
  font: 14px/1.6 -apple-system, BlinkMacSystemFont, "PingFang SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif; }
main { max-width: 1080px; margin: 0 auto; padding: 0 16px 40px; }
.top { display: flex; align-items: center; gap: 8px 12px; flex-wrap: wrap; padding: 16px 0 10px; }
.top .tools { margin-left: auto; display: flex; gap: 10px; align-items: center; }
h1 { font-size: 20px; margin: 0; }
h2 { font-size: 15px; margin: 0 0 10px; }
h3.step { font-size: 15px; margin: 18px 0 8px; }
nav.tabs { position: sticky; top: 0; z-index: 5; display: flex; overflow-x: auto; scrollbar-width: none;
  background: var(--bg); border-bottom: 1px solid var(--line); margin: 0 -16px 14px; padding: 0 10px; }
nav.tabs::-webkit-scrollbar { display: none; }
nav.tabs a { flex: 0 0 auto; padding: 9px 14px; color: var(--muted); text-decoration: none; border-bottom: 2px solid transparent; }
nav.tabs a.on { color: var(--text); border-bottom-color: var(--text); font-weight: 600; }
.tab { display: none; }
.tab.on { display: block; }
body.setup .run-only, body:not(.setup) .setup-only { display: none !important; }
.muted { color: var(--muted); }
.pill { padding: 2px 10px; border-radius: 999px; font-weight: 600; font-size: 13px; }
.ok { color: var(--ok); } .warn { color: var(--warn); } .bad { color: var(--bad); }
.pill.ok { background: var(--ok-bg); } .pill.warn { background: var(--warn-bg); } .pill.bad { background: var(--bad-bg); }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(320px, 100%), 1fr)); gap: 12px; margin-bottom: 12px; }
.card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; min-width: 0; margin-bottom: 12px; }
.grid > .card { margin-bottom: 0; }
.check { display: flex; gap: 10px; padding: 6px 0; border-top: 1px solid var(--line); }
.check:first-of-type { border-top: 0; }
.check b { flex: 0 0 72px; font-weight: 600; }
.check span:last-child { min-width: 0; overflow-wrap: anywhere; }
.dot { flex: 0 0 8px; height: 8px; margin-top: 8px; border-radius: 50%; background: currentColor; }
table { width: 100%; border-collapse: collapse; }
td, th { text-align: left; padding: 4px 6px; border-top: 1px solid var(--line); vertical-align: top; }
th { color: var(--muted); font-weight: 500; }
#sends th, #sends td:nth-child(-n+3) { white-space: nowrap; }
.kv td:first-child { color: var(--muted); width: 40%; }
.kv td:last-child { overflow-wrap: anywhere; }
code { background: var(--code); border-radius: 4px; padding: 1px 5px; font-size: 12px; word-break: break-all; }
a { color: inherit; }
ul, ol { margin: 0; padding-left: 20px; }
li { margin: 6px 0; }
p { margin: 0 0 8px; }
.scroll { overflow-x: auto; }
button { font: inherit; padding: 5px 12px; border-radius: 8px; border: 1px solid var(--line); background: var(--card);
  color: var(--text); cursor: pointer; }
button:hover { border-color: var(--muted); }
button.primary { background: var(--text); color: var(--bg); border-color: var(--text); }
button:disabled { opacity: .5; cursor: default; }
.actions { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
input[type=number], input[type=text], input[type=password], select, textarea { font: inherit; color: var(--text);
  background: var(--bg); border: 1px solid var(--line); border-radius: 6px; padding: 4px 8px; max-width: 100%; }
input[type=text].wide, input[type=password].wide { width: 100%; }
textarea { width: 100%; min-height: 72px; font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; }
.field { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 6px 0;
  border-top: 1px solid var(--line); }
.field:first-child { border-top: 0; }
.field.block { display: block; }
.field.block > label { display: block; margin-bottom: 4px; }
.hint { font-size: 12px; color: var(--muted); }
.found { display: flex; justify-content: space-between; align-items: center; gap: 8px; padding: 3px 0; overflow-wrap: anywhere; }
.qr { margin-top: 12px; }
.qr img { display: block; width: 280px; max-width: 100%; height: auto; border: 1px solid var(--line); border-radius: 8px;
  background: #fff; }
.logbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin-bottom: 8px; }
.logbar input[type=text] { flex: 1 1 160px; }
.log { max-height: min(70vh, 640px); overflow: auto; background: var(--code); border-radius: 8px; padding: 10px;
  font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; word-break: break-all; }
.log .warn, .log .bad { font-weight: 600; }
#notice { position: fixed; right: 16px; bottom: 16px; max-width: 360px; padding: 10px 14px; border-radius: 10px;
  background: var(--text); color: var(--bg); display: none; z-index: 10; }
@media (max-width: 640px) {
  main { padding: 0 12px 32px; }
  nav.tabs { margin: 0 -12px 12px; padding: 0 4px; }
  h1 { font-size: 18px; }
  .card { padding: 12px; }
  .check b { flex-basis: 64px; }
  .kv td:first-child { width: 45%; }
  #notice { left: 12px; right: 12px; max-width: none; }
}
@media (pointer: coarse) {
  button, select, input[type=text], input[type=password], input[type=number] { min-height: 40px; }
  nav.tabs a { padding: 11px 16px; }
}
</style>
</head>
<body>
<main>
  <header class="top">
    <h1>微信连接器</h1>
    <span id="overall" class="pill warn">读取中</span>
    <span id="meta" class="muted"></span>
    <span class="tools">
      <button id="theme" title="页面主题：跟随系统、浅色、深色轮换">主题：自动</button>
      <a href="../">网页桌面</a>
    </span>
  </header>
  <nav class="tabs">
    <a href="#overview" data-tab="overview">概览</a>
    <a href="#setup" data-tab="setup" hidden>首次配置</a>
    <a href="#login" data-tab="login">登录</a>
    <a href="#settings" data-tab="settings">设置</a>
    <a href="#log" data-tab="log">日志</a>
  </nav>

  <section id="tab-overview" class="tab">
    <div class="grid">
      <section class="card">
        <h2>状态</h2>
        <div id="checks"></div>
        <div class="actions" style="margin-top:10px">
          <button id="pause" title="暂停期间 AstrBot 要发的消息会被拒，恢复后也不补发">暂停发送</button>
          <button id="test" title="给「文件传输助手」发一条固定的测试消息">发测试消息</button>
          <button id="restart" title="重新读配置和密钥；正在发消息时不重启">重启连接器</button>
          <button id="notify">开启掉线通知</button>
        </div>
      </section>
      <section class="card"><h2>收发计数（启动以来）</h2><table class="kv" id="counts"></table></section>
    </div>
    <div class="grid">
      <section class="card scroll"><h2>最近发送</h2><table id="sends"></table></section>
      <section class="card"><h2>配置</h2><table class="kv" id="config"></table></section>
    </div>
  </section>

  <section id="tab-setup" class="tab">
    <section class="card">
      <h2>首次配置</h2>
      <p>还没有 <code>config.json</code> 或 <code>keys.json</code>，所以连接器先只开这个页面，不收也不发。
        两份都有了会自动切到正常模式（十几秒），然后去「设置」填白名单。</p>
      <div id="setupchecks"></div>
    </section>
    <h3 class="step">1. 扫码登录，同时抓密钥</h3>
    <div class="grid" id="step1"></div>
    <h3 class="step">2. 填 AstrBot 地址和口令</h3>
    <div class="grid" id="step2"></div>
  </section>

  <section id="tab-login" class="tab">
    <div class="grid" id="loginslot">
      <section class="card" id="logincard">
        <h2>扫码登录</h2>
        <p>现在：<span id="loginstate" class="muted">读取中</span></p>
        <div class="actions">
          <button id="qrbtn" class="primary" disabled>显示二维码</button>
          <span class="hint">只截微信的登录窗口；已经登录、或者旁边开着别的微信窗口时不截图</span>
        </div>
        <div id="qrbox" class="qr" hidden>
          <img id="qr" alt="微信登录窗口截图">
          <p class="hint" id="qrnote" style="margin-top:6px">每 3 秒更新一次，用小号扫码</p>
        </div>
      </section>
      <section class="card" id="keycard">
        <h2>抓密钥</h2>
        <p class="run-only muted">数据库：<span id="dbstate">读取中</span></p>
        <p class="run-only">只是重新登录（没换号、手机上没退出）一般不用重抓密钥；数据库标红「密钥不能用」时按下面做：</p>
        <p class="setup-only">新版微信只在登录那一刻派生数据库密钥，所以要先挂上断点再扫码。微信已经登录的话，先在手机上退出这台电脑上的登录，等出现二维码再做：</p>
        <ol>
          <li>在电脑的终端里运行，等它提示「现在扫码登录」：<br><code id="cmd1">docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py capture</code>
            <button data-copy="cmd1">复制</button></li>
          <li>用小号扫「扫码登录」里的二维码（也可以<a href="../">打开网页桌面</a>扫）。</li>
          <li>抓到后运行，看到「校验通过 N/N 个库」就成功了：<br><code id="cmd2">docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py verify</code>
            <button data-copy="cmd2">复制</button></li>
          <li class="run-only">回到这里点<button id="restart2">重启连接器</button>，让它读新密钥。</li>
          <li class="setup-only">成功后这里会把「密钥」标绿；两份文件都齐了会自动切到正常模式。</li>
        </ol>
        <p class="hint" style="margin-top:6px">抓密钥要 root 跑 gdb，连接器自己没这个权限，所以要在终端里运行。容器名不是 wechat 的话把命令里的换掉。</p>
      </section>
    </div>
  </section>

  <section id="tab-settings" class="tab">
    <div class="grid" id="astrbotslot">
      <section class="card" id="astrbotcard">
        <h2>连接 AstrBot</h2>
        <div class="field block">
          <label for="ab-url">反向 WebSocket 地址</label>
          <input id="ab-url" class="wide" type="text" placeholder="ws://astrbot:6200/ws" spellcheck="false" autocomplete="off">
          <div class="hint" id="ab-urlhint">AstrBot 容器名加平台里填的端口，比如 ws://astrbot:6200/ws</div>
        </div>
        <div class="field block">
          <label for="ab-token">口令（和 AstrBot 平台里填的一样）</label>
          <input id="ab-token" class="wide" type="password" autocomplete="new-password" spellcheck="false">
          <div class="hint">只写不读：页面上不显示已经保存的口令，留空就是不改</div>
        </div>
        <div class="field setup-only">
          <label for="ab-ro">先只收不发（第一次部署建议开着，确认能收到消息再到「设置」里关掉）</label>
          <input id="ab-ro" type="checkbox">
        </div>
        <div class="actions" style="margin-top:10px">
          <button id="ab-save" class="primary">保存连接并重启</button>
          <span class="hint run-only">保存后看「概览」的 AstrBot 一行：连不上会写原因</span>
        </div>
      </section>
      <section class="card run-only">
        <h2>收发开关</h2>
        <div id="toggles" class="muted">读取中</div>
      </section>
    </div>
    <section class="card run-only">
      <h2>白名单</h2>
      <div id="lists"></div>
      <div class="field block">
        <label for="q">按名字查 wxid（备注名或昵称里带这几个字的好友和群）</label>
        <div class="actions"><input id="q" type="text" placeholder="名字"><button id="find">查</button></div>
        <div id="found"></div>
      </div>
      <div class="actions" style="margin-top:10px">
        <button id="save" class="primary">保存并重启</button>
        <span class="hint">收发开关和白名单一起保存；保存前会留一份 config.json.bak</span>
      </div>
    </section>
  </section>

  <section id="tab-log" class="tab">
    <section class="card">
      <div class="logbar">
        <select id="loglevel" aria-label="级别">
          <option value="0">全部级别</option><option value="2">警告和错误</option><option value="3">只看错误</option>
        </select>
        <input id="logq" type="text" placeholder="筛选关键字" aria-label="关键字">
        <select id="loglines" aria-label="行数">
          <option value="200">最后 200 行</option><option value="500">最后 500 行</option>
          <option value="1000">最后 1000 行</option><option value="2000">最后 2000 行</option>
        </select>
        <label><input id="follow" type="checkbox" checked> 跟到最新</label>
        <span id="logcount" class="hint"></span>
      </div>
      <div id="logbox" class="log">读取中</div>
    </section>
  </section>
</main>
<div id="notice"></div>
<script>
const LEVEL = { ok: "正常", warn: "要留意", bad: "有问题" };
const TOKEN = document.querySelector('meta[name="wxc-token"]').content;
const TABS = { run: ["overview", "login", "settings", "log"], setup: ["setup", "log"] };
const $ = id => document.getElementById(id);
let mode = "", busy = false, alerting = false, settings = null, qrOn = false, logLines = [];

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = String(text);
  return e;
}
function row(cells, head) {
  const tr = el("tr");
  cells.forEach(c => tr.appendChild(el(head ? "th" : "td", c && c.cls, c && typeof c === "object" ? c.text : c)));
  return tr;
}
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
  notice("连接器正在重启，过一会儿自动刷新页面");
  setTimeout(() => location.reload(), mode === "setup" ? 5000 : 12000);
}

// ---------- 主题 ----------
const THEMES = ["auto", "light", "dark"], THEME_NAME = { auto: "自动", light: "浅色", dark: "深色" };
function theme() { try { return localStorage.getItem("wxc-theme") || "auto"; } catch (e) { return "auto"; } }
function applyTheme(t) {
  if (t === "auto") delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t;
  $("theme").textContent = "主题：" + THEME_NAME[t];
}
$("theme").onclick = () => {
  const t = THEMES[(THEMES.indexOf(theme()) + 1) % THEMES.length];
  try { localStorage.setItem("wxc-theme", t); } catch (e) {}
  applyTheme(t);
};

// ---------- 标签 ----------
function currentTab() {
  const list = TABS[mode] || TABS.run, t = location.hash.slice(1);
  return list.includes(t) ? t : list[0];
}
function showTab() {
  const t = currentTab();
  document.querySelectorAll("nav.tabs a").forEach(a => a.classList.toggle("on", a.dataset.tab === t));
  document.querySelectorAll(".tab").forEach(s => s.classList.toggle("on", s.id === "tab-" + t));
  if (t === "log") refreshLog();
  if (t === "login" || t === "setup") refreshLogin(); else stopQr();
  if ((t === "settings" || t === "setup") && !settings) loadSettings();
}
window.addEventListener("hashchange", () => { window.scrollTo(0, 0); showTab(); });
function setMode(m) {
  if (m === mode) return;
  mode = m;
  document.body.classList.toggle("setup", m === "setup");
  document.querySelectorAll("nav.tabs a").forEach(a => { a.hidden = !TABS[m].includes(a.dataset.tab); });
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
  $("notify").textContent = on ? "关闭掉线通知" : "开启掉线通知";
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
function renderChecks(box, checks) {
  box.replaceChildren(...checks.map(c => {
    const d = el("div", "check");
    d.append(el("span", "dot " + c.level), el("b", "", c.name), el("span", "", c.text));
    return d;
  }));
}
async function refresh() {
  let s;
  try { s = await getJSON("api/status"); }
  catch (e) {
    $("overall").className = "pill bad"; $("overall").textContent = "连接器没响应";
    document.title = "⚠ 没响应 · 微信连接器";
    return;
  }
  setMode(s.mode);
  busy = s.busy;
  $("meta").textContent = "版本 " + s.version + " · 已运行 " + s.uptime;
  if (s.mode === "setup") {
    $("overall").className = "pill warn"; $("overall").textContent = "首次配置";
    document.title = "首次配置 · 微信连接器";
    renderChecks($("setupchecks"), s.checks);
    return;
  }
  $("overall").className = "pill " + s.level; $("overall").textContent = LEVEL[s.level];
  checkAlert(s);
  renderChecks($("checks"), s.checks);
  const pause = $("pause");
  pause.dataset.paused = s.paused ? "1" : "0";
  pause.textContent = s.paused ? "恢复发送" : "暂停发送";
  const test = $("test");
  test.disabled = s.receive_only || s.paused || s.test_pending;
  test.title = s.receive_only ? "只收不发模式不发消息" : s.paused ? "已暂停发送" : s.test_pending ? "上一条测试消息还没发完"
    : "给「文件传输助手」发一条固定的测试消息";
  const db = s.checks.find(c => c.name === "数据库");
  if (db) { $("dbstate").className = db.level; $("dbstate").textContent = db.text; }
  const counts = Object.entries(s.counts);
  $("counts").replaceChildren(...(counts.length ? counts.map(([k, v]) => row([k, v])) : [row(["还没有", ""])]));
  $("sends").replaceChildren(row(["时间", "发给", "内容", "结果"], true),
    ...(s.recent_sends.length ? s.recent_sends.map(x => row([x.ago + "前", x.to, x.what,
      x.ok ? { cls: "ok", text: "发出" } : { cls: "bad", text: x.reason || "失败" }])) : [row(["启动后还没发过", "", "", ""])]));
  $("config").replaceChildren(...Object.entries(s.config).map(([k, v]) => row([k, v === true ? "开" : v === false ? "关" : v])));
}

// ---------- 登录 ----------
function stopQr() {
  qrOn = false;
  $("qrbox").hidden = true;
  $("qrbtn").textContent = "显示二维码";
}
$("qrbtn").onclick = () => {
  if (qrOn) { stopQr(); return; }
  qrOn = true; $("qrbtn").textContent = "收起二维码";
  refreshLogin(true);
};
$("qr").onerror = () => { $("qrnote").textContent = "这次没截到，过几秒自动再试"; };
$("qr").onload = () => { $("qrnote").textContent = "每 3 秒更新一次，用小号扫码"; };
async function refreshLogin(force) {
  let s;
  try { s = await getJSON("api/login"); } catch (e) { $("loginstate").className = "bad"; $("loginstate").textContent = "读不到"; return; }
  const line = $("loginstate");
  line.className = s.login ? "warn" : s.running ? "ok" : "bad";
  line.textContent = s.text + (s.login && s.others ? "（旁边还开着别的微信窗口，不截图，去网页桌面看）" : "");
  $("qrbtn").disabled = !s.login && !qrOn;
  if (!qrOn) return;
  if (s.login && !s.others) {
    $("qr").src = "api/login/qr?t=" + Date.now();
    $("qrbox").hidden = false;
  } else {
    $("qrbox").hidden = true;
    if (!s.login) { stopQr(); if (!force) notice("登录窗口没了：" + s.text); }
  }
}
document.querySelectorAll("[data-copy]").forEach(b => b.onclick = async () => {
  try { await navigator.clipboard.writeText($(b.dataset.copy).textContent); notice("已复制"); }
  catch (e) { notice("复制不了，手动选中复制吧"); }
});

// ---------- 设置 ----------
function fillAstrbot(a) {
  $("ab-url").value = a.url;
  $("ab-url").disabled = a.locked;
  if (a.locked) $("ab-urlhint").textContent = "地址里带了参数或账号（可能有口令），页面上不显示全，也不能在这里改，要改请改 config.json";
  $("ab-token").value = "";
  $("ab-token").placeholder = a.token_set ? "已设置，留空不改" : "还没设置";
  $("ab-ro").checked = a.receive_only;
}
async function loadSettings() {
  const box = $("toggles");
  try { settings = await getJSON("api/settings"); } catch (e) { box.textContent = "读不了设置"; return; }
  fillAstrbot(settings.astrbot);
  if (settings.setup) return;
  box.className = ""; box.replaceChildren();
  settings.toggles.forEach(t => {
    const f = el("div", "field"), cb = el("input"), lab = el("label", "", t.label);
    cb.type = "checkbox"; cb.checked = t.value; cb.dataset.key = t.key; cb.dataset.kind = "toggle"; cb.id = "set-" + t.key;
    lab.htmlFor = cb.id;
    f.append(lab, cb); box.append(f);
  });
  settings.numbers.forEach(n => {
    const f = el("div", "field"), i = el("input");
    i.type = "number"; i.min = n.min; i.max = n.max; i.value = n.value; i.style.width = "90px";
    i.dataset.key = n.key; i.dataset.kind = "number";
    f.append(el("label", "", n.label + "（" + n.min + "～" + n.max + "）"), i); box.append(f);
  });
  settings.choices.forEach(ch => {
    const f = el("div", "field"), sel = el("select");
    Object.entries(ch.options).forEach(([v, lab]) => { const o = el("option", "", lab); o.value = v; sel.append(o); });
    sel.value = ch.value; sel.dataset.key = ch.key; sel.dataset.kind = "choice";
    f.append(el("label", "", ch.label), sel); box.append(f);
  });
  const lists = $("lists");
  lists.replaceChildren();
  settings.lists.forEach(l => {
    const f = el("div", "field block"), ta = el("textarea");
    ta.value = l.value.join("\n"); ta.dataset.key = l.key; ta.dataset.kind = "list"; ta.id = "list-" + l.key;
    const named = l.value.filter(x => l.names[x]).map(x => l.names[x]);
    f.append(el("label", "", l.label), ta);
    if (named.length) f.append(el("div", "hint", "现在有：" + named.join("、")));
    lists.append(f);
  });
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
  const data = {};
  document.querySelectorAll("#toggles [data-key], #lists [data-key]").forEach(x => {
    const k = x.dataset.key, kind = x.dataset.kind;
    data[k] = kind === "toggle" ? x.checked : kind === "number" ? Number(x.value)
      : kind === "list" ? x.value.split("\n").map(v => v.trim()).filter(Boolean) : x.value;
  });
  if (!Object.keys(data).length) { notice("设置还没读出来，等一下"); return; }
  if (!confirm("保存收发开关和白名单，并重启连接器？")) return;
  try { await post("api/settings", data); afterRestart(); } catch (e) { notice("没保存：" + e.message); }
};
async function find() {
  const q = $("q").value.trim(), out = $("found");
  if (!q) return;
  try {
    const list = await getJSON("api/lookup?q=" + encodeURIComponent(q));
    out.replaceChildren(...(list.length ? list.map(x => {
      const d = el("div", "found"), b = el("button", "", x.group ? "加到群白名单" : "加到私聊白名单");
      b.onclick = () => {
        const ta = $("list-" + (x.group ? "group_whitelist" : "whitelist"));
        if (!ta) { notice("白名单还没读出来，等一下"); return; }
        const items = ta.value.split("\n").map(v => v.trim()).filter(Boolean);
        if (!items.includes(x.wxid)) { items.push(x.wxid); ta.value = items.join("\n"); }
        notice("已加进去，点「保存并重启」才生效");
      };
      d.append(el("span", "", (x.group ? "群 " : "") + x.name + "  " + x.wxid), b);
      return d;
    }) : [el("div", "hint", "没找到")]));
  } catch (e) { out.textContent = "查不了"; }
}
$("find").onclick = find;
$("q").onkeydown = e => { if (e.key === "Enter") find(); };

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
  const min = Number($("loglevel").value), q = $("logq").value.trim().toLowerCase(), box = $("logbox"), out = [];
  let lv = 1;
  for (const line of logLines) {
    const m = /^\d\d-\d\d \d\d:\d\d:\d\d (DEBUG|INFO|WARNING|ERROR|CRITICAL) /.exec(line);
    if (m) lv = LV[m[1]];  // 没有时间和级别的行（报错的调用栈）跟着上一行算
    if (lv < min || (q && !line.toLowerCase().includes(q))) continue;
    out.push(el("div", lv >= 3 ? "bad" : lv === 2 ? "warn" : "", line || " "));
  }
  box.replaceChildren(...(out.length ? out : [el("div", "muted", "没有符合条件的行")]));
  $("logcount").textContent = "显示 " + out.length + " / " + logLines.length + " 行";
  if ($("follow").checked) box.scrollTop = box.scrollHeight;
}
$("logbox").addEventListener("scroll", () => {
  const b = $("logbox");
  $("follow").checked = b.scrollTop + b.clientHeight >= b.scrollHeight - 20;  // 往上翻就先不跟，翻回底部接着跟
});
$("follow").onchange = () => { if ($("follow").checked) $("logbox").scrollTop = $("logbox").scrollHeight; };
$("loglevel").onchange = renderLog;
$("logq").oninput = renderLog;
$("loglines").onchange = refreshLog;

applyTheme(theme());
showNotifyButton();
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
