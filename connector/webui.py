"""状态页：微信进程和窗口、数据库密钥、AstrBot 连接、发送队列、收消息计数、运行日志；暂停发送、改常用设置、重启连接器

监听容器里的 127.0.0.1:6299（webui_host / webui_port），只有同一个容器里的 nginx 能访问到；
nginx 把网页桌面（3001 端口，有密码）的 /wxc/ 转过来，所以打开 https://localhost:3001/wxc/ 就能看，密码和网页桌面一样。
不发消息、不动微信窗口（查窗口用界面驱动的 status，不用会把窗口拉出来的 ping）。
状态和日志里没有消息内容和令牌，会话只用 OneBot 号表示；「设置」里的白名单会显示 wxid 和备注名（改白名单要用）。
口令、AstrBot 地址这些不在页面上，也改不了。

  GET  /              页面（每 5 秒刷新一次状态）
  GET  /api/status    状态 JSON，也可以给脚本拿来做掉线提醒
  GET  /api/log       connector.log 最后 200 行
  GET  /api/settings  能在页面上改的那几项设置
  GET  /api/lookup?q= 按名字查好友和群（填白名单用）
  POST /api/settings  保存设置（写回 config.json，留一份 .bak），然后重启连接器
  POST /api/stop      {"stop": true/false}：放 / 删 STOP 文件
  POST /api/restart   重启连接器（退出后 s6 拉起来）；正在发消息时不重启
POST 要带页面里的随机口令（X-WXC-Token，连接器每次启动换一个）和 Content-Type: application/json：
别的网页提交不了表单，跨域 fetch 要先 OPTIONS 预检，这里一律 405、不给 CORS 头。
"""
import asyncio
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
# 页面上能改的设置：其余的（口令、地址、端口、目录）只能改 config.json
TOGGLES = {"receive_only": "只收不发", "receive_stickers": "收表情包", "group_stickers": "群里的表情包也收",
           "receive_photos": "收照片", "group_photos": "群里的照片也收", "receive_records": "收聊天记录",
           "receive_pats": "收拍一拍", "reply_mention_when_busy": "群里回复前面插了别人的话时加 @"}
NUMBERS = {"max_sends_per_minute": ("每分钟最多发几条", 1, 60), "max_message_age": ("多少秒以前的消息不收", 30, 86400),
           "photo_wait_seconds": ("等原图最多几秒", 0, 30)}
CHOICES = {"voice_mode": ("语音怎么发", {"native": "原生语音条", "file": "mp3 文件"})}
LISTS = {"whitelist": "私聊白名单（一行一个 wxid，* 是所有好友）", "group_whitelist": "群白名单（一行一个，以 @chatroom 结尾）",
         "bot_names": "群里叫小号的其他名字（一行一个）"}


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
        resp = await asyncio.to_thread(c.ui.request, {"op": "status"}, 20.0)
        self.result = {"ok": bool(resp.get("ok")), "frame": bool(resp.get("frame")),
                       "width": int(resp.get("width") or 0), "height": int(resp.get("height") or 0),
                       "reason": str(resp.get("reason") or ""), "checked_at": self.checked}
        return self.result


def window_check(w: dict) -> tuple[str, str]:
    if not w:
        return "warn", "还没查过"
    if not w["ok"]:
        return "bad", f"界面驱动不可用：{w['reason']}"
    if not w["frame"]:
        return "bad", "找不到微信窗口：微信没开，或者退出登录了"
    if w["width"] == 0:
        return "ok", "主窗口收在托盘里（发消息时会自动打开）"
    if w["width"] < 500:
        return "warn", f"窗口只有 {w['width']}×{w['height']}，可能是登录（扫码）窗口，去网页桌面看一眼"
    return "ok", f"主窗口 {w['width']}×{w['height']}"


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
        checks.append(("数据库", "bad", f"密钥不能用（{c.key_error}），按 README 第 2 步重新抓一次"))
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
        checks.append(("AstrBot", "bad", f"没连上（{ob.last_error or '正在连'}），收到的消息先暂存 {len(ob.buffer)} 条"))
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
        "version": c.version, "now": now, "uptime": ago(now - c.started), "level": level, "paused": paused,
        "busy": bool(queued),
        "checks": [{"name": n, "level": lv, "text": t} for n, lv, t in checks],
        "counts": dict(sorted(c.counts.items())),
        "recent_sends": [{**s, "ago": ago(now - s["time"])} for s in reversed(sends[-12:])],
        "config": {
            "私聊": f"所有好友（{people} 人）" if c.all_friends else f"白名单 {people} 人",
            "群": f"{len(c.groups)} 个", "OneBot 号": c.self_id,
            "AstrBot 地址": c.cfg["astrbot_ws_url"], "口令": "已设置" if c.cfg["access_token"] else "没设置",
            "收表情包": c.cfg["receive_stickers"], "收聊天记录": c.cfg["receive_records"],
            "语音": c.cfg["voice_mode"], "每分钟最多发": c.cfg["max_sends_per_minute"],
        },
    }


def tail_log(state_dir: str, lines: int = 200) -> str:
    try:
        with open(os.path.join(state_dir, "connector.log"), encoding="utf-8", errors="replace") as f:
            return "\n".join(f.read().splitlines()[-lines:])
    except OSError:
        return "（还没有日志）"


def read_settings(c) -> dict:
    """页面上能改的那几项：值以 config.json 里写的为准，没写的显示默认值；白名单带上备注名"""
    try:
        with open(c.cfg["_config_path"], encoding="utf-8") as f:
            saved = json.load(f)
    except (OSError, ValueError, KeyError):
        saved = {}
    def val(k):
        return saved.get(k, c.cfg.get(k))
    names = c.store.contacts()
    return {
        "toggles": [{"key": k, "label": v, "value": bool(val(k))} for k, v in TOGGLES.items()],
        "numbers": [{"key": k, "label": lab, "min": lo, "max": hi, "value": val(k)} for k, (lab, lo, hi) in NUMBERS.items()],
        "choices": [{"key": k, "label": lab, "options": opts, "value": val(k)} for k, (lab, opts) in CHOICES.items()],
        "lists": [{"key": k, "label": v, "value": [str(x) for x in (val(k) or [])],
                   "names": {str(x): names.get(str(x), "") for x in (val(k) or []) if k != "bot_names"}}
                  for k, v in LISTS.items()],
    }


def validate_settings(data) -> tuple[dict, str]:
    """页面提交的设置 → (要写进 config.json 的那几项, 错误说明)。不认识的键、类型不对、超范围都算错，一个都不写"""
    if not isinstance(data, dict) or not data:
        return {}, "没有要保存的设置"
    out = {}
    for k, v in data.items():
        if k in TOGGLES:
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


def save_settings(path: str, changes: dict) -> None:
    """只改 changes 里那几项，其余原样；先留一份 .bak，再写临时文件、改名替换"""
    with open(path, encoding="utf-8") as f:
        raw = f.read()
    cfg = json.loads(raw)
    cfg.update(changes)
    with open(path + ".bak", "w", encoding="utf-8") as f:
        f.write(raw)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.chmod(tmp, os.stat(path).st_mode & 0o777)
    os.replace(tmp, path)


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
    token = secrets.token_urlsafe(24)  # 每次启动换一个，放进页面里；POST 不带对的就拒绝
    page = PAGE.replace("__TOKEN__", token).encode()

    async def post(path: str, data) -> tuple[int, dict]:
        if path == "/api/stop":
            if not isinstance(data, dict) or not isinstance(data.get("stop"), bool):
                return 400, {"error": "要给 stop: true 或 false"}
            stop = os.path.join(c.state_dir, "STOP")
            if data["stop"]:
                open(stop, "a").close()
            elif os.path.exists(stop):
                os.remove(stop)
            log.info("状态页：%s发送", "暂停" if data["stop"] else "恢复")
            return 200, {"ok": True, "paused": data["stop"]}
        if path in ("/api/restart", "/api/settings"):
            if c.sending or c.send_queue.qsize():
                return 409, {"error": "正在发消息，等发完再试"}
            if path == "/api/settings":
                changes, err = validate_settings(data)
                if err:
                    return 400, {"error": err}
                try:
                    await asyncio.to_thread(save_settings, c.cfg["_config_path"], changes)
                except (OSError, ValueError) as e:
                    log.warning("状态页：保存设置失败（%s）", type(e).__name__)
                    return 500, {"error": f"写 config.json 失败（{type(e).__name__}），没有重启"}
                log.info("状态页：保存设置（%s），重启连接器", "、".join(changes))
            else:
                log.info("状态页：重启连接器")
            restart_later()
            return 200, {"ok": True}
        return 404, {"error": "没有这个接口"}

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
            elif path == "/":
                code, ctype, body = 200, "text/html; charset=utf-8", page
            elif path == "/api/status":
                code, ctype = 200, "application/json; charset=utf-8"
                body = json.dumps(await status(c, probe), ensure_ascii=False).encode()
            elif path == "/api/log":
                code, body = 200, (await asyncio.to_thread(tail_log, c.state_dir)).encode()
            elif path in ("/api/settings", "/api/lookup"):
                q = parse_qs(url.query).get("q", [""])[0]
                async with c.db_lock:
                    result = await asyncio.to_thread(read_settings, c) if path == "/api/settings" \
                        else await asyncio.to_thread(lookup, c, q)
                code, ctype, body = 200, "application/json; charset=utf-8", json.dumps(result, ensure_ascii=False).encode()
            else:
                code, body = 404, "没有这个页面".encode()
            reason = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 405: "Method Not Allowed",
                      409: "Conflict", 413: "Payload Too Large", 500: "Internal Server Error"}[code]
            writer.write((f"HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                          "Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\n"
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


PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="wxc-token" content="__TOKEN__">
<title>微信连接器</title>
<style>
:root {
  --bg: #f6f7f9; --card: #ffffff; --text: #1d2129; --muted: #6b7280; --line: #e5e7eb;
  --ok: #16a34a; --ok-bg: #dcfce7; --warn: #b45309; --warn-bg: #fef3c7; --bad: #dc2626; --bad-bg: #fee2e2;
  --code: #f3f4f6;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #111317; --card: #1a1d23; --text: #e5e7eb; --muted: #9ca3af; --line: #2b2f36;
    --ok: #4ade80; --ok-bg: #14301f; --warn: #fbbf24; --warn-bg: #3a2e10; --bad: #f87171; --bad-bg: #3b1717;
    --code: #0d0f12;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text);
  font: 14px/1.6 -apple-system, BlinkMacSystemFont, "PingFang SC", "Noto Sans CJK SC", "Microsoft YaHei", sans-serif; }
main { max-width: 1080px; margin: 0 auto; padding: 20px 16px 40px; }
header { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
h1 { font-size: 20px; margin: 0; }
h2 { font-size: 15px; margin: 0 0 10px; }
.muted { color: var(--muted); }
.pill { padding: 2px 10px; border-radius: 999px; font-weight: 600; font-size: 13px; }
.ok { color: var(--ok); } .warn { color: var(--warn); } .bad { color: var(--bad); }
.pill.ok { background: var(--ok-bg); } .pill.warn { background: var(--warn-bg); } .pill.bad { background: var(--bad-bg); }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 12px; margin-bottom: 12px; }
.card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; min-width: 0; }
.check { display: flex; gap: 10px; padding: 6px 0; border-top: 1px solid var(--line); }
.check:first-of-type { border-top: 0; }
.check b { flex: 0 0 72px; font-weight: 600; }
.dot { flex: 0 0 8px; height: 8px; margin-top: 8px; border-radius: 50%; background: currentColor; }
table { width: 100%; border-collapse: collapse; }
td, th { text-align: left; padding: 4px 6px; border-top: 1px solid var(--line); vertical-align: top; }
th { color: var(--muted); font-weight: 500; }
#sends th, #sends td:nth-child(-n+3) { white-space: nowrap; }
.kv td:first-child { color: var(--muted); width: 40%; }
pre { margin: 0; max-height: 420px; overflow: auto; background: var(--code); border-radius: 8px; padding: 10px;
  font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; white-space: pre-wrap; word-break: break-all; }
code { background: var(--code); border-radius: 4px; padding: 1px 5px; font-size: 12px; word-break: break-all; }
a { color: inherit; }
ul, ol { margin: 0; padding-left: 20px; }
li { margin: 4px 0; }
.scroll { overflow-x: auto; }
button { font: inherit; padding: 5px 12px; border-radius: 8px; border: 1px solid var(--line); background: var(--card);
  color: var(--text); cursor: pointer; }
button:hover { border-color: var(--muted); }
button.primary { background: var(--text); color: var(--bg); border-color: var(--text); }
button:disabled { opacity: .5; cursor: default; }
.actions { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
input[type=number], input[type=text], select, textarea { font: inherit; color: var(--text); background: var(--bg);
  border: 1px solid var(--line); border-radius: 6px; padding: 4px 8px; }
textarea { width: 100%; min-height: 72px; font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; }
.field { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 5px 0;
  border-top: 1px solid var(--line); }
.field:first-child { border-top: 0; }
.field.block { display: block; }
.field.block label { display: block; margin-bottom: 4px; }
.hint { font-size: 12px; color: var(--muted); }
#notice { position: fixed; right: 16px; bottom: 16px; max-width: 360px; padding: 10px 14px; border-radius: 10px;
  background: var(--text); color: var(--bg); display: none; }
.found { display: flex; justify-content: space-between; align-items: center; gap: 8px; padding: 3px 0; }
</style>
</head>
<body>
<main>
  <header>
    <h1>微信连接器</h1>
    <span id="overall" class="pill warn">读取中</span>
    <span id="meta" class="muted"></span>
    <span class="actions" style="margin-left:auto">
      <button id="pause" title="暂停期间 AstrBot 要发的消息会被拒，恢复后也不补发">暂停发送</button>
      <button id="restart" title="重新读配置和密钥；正在发消息时不重启">重启连接器</button>
      <button id="notify">开启掉线通知</button>
      <a href="../">打开网页桌面</a>
    </span>
  </header>
  <div class="grid">
    <section class="card"><h2>状态</h2><div id="checks"></div></section>
    <section class="card"><h2>收发计数（启动以来）</h2><table class="kv" id="counts"></table></section>
  </div>
  <div class="grid">
    <section class="card scroll"><h2>最近发送</h2><table id="sends"></table></section>
    <section class="card"><h2>配置</h2><table class="kv" id="config"></table></section>
  </div>
  <div class="grid">
    <section class="card">
      <h2>设置</h2>
      <div id="settings" class="muted">读取中</div>
      <div class="actions" style="margin-top:10px">
        <button id="save" class="primary">保存并重启</button>
        <span class="hint">只改这几项，口令、地址这些要改 config.json；保存前会留一份 config.json.bak</span>
      </div>
      <div class="field block" style="margin-top:10px">
        <label>按名字查 wxid（备注名或昵称里带这几个字的好友和群）</label>
        <div class="actions"><input id="q" type="text" placeholder="名字"><button id="find">查</button></div>
        <div id="found"></div>
      </div>
    </section>
    <section class="card">
      <h2>重新登录、重新抓密钥</h2>
      <p class="muted" style="margin:0 0 6px">现在：<span id="dbstate">读取中</span></p>
      <p style="margin:0 0 6px">窗口找不到、窗口很小（像登录窗口），或者很久没有新数据：先<a href="../">打开网页桌面</a>看微信是不是要重新扫码。
        只是重新登录（没换号、手机上没退出）一般不用重抓密钥；数据库标红「密钥不能用」时按下面做：</p>
      <ol>
        <li>在电脑的终端里运行，等它提示「现在扫码登录」：<br><code id="cmd1">docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py capture</code>
          <button data-copy="cmd1">复制</button></li>
        <li><a href="../">打开网页桌面</a>，用小号扫码登录。</li>
        <li>抓到后运行，看到「校验通过 N/N 个库」就成功了：<br><code id="cmd2">docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py verify</code>
          <button data-copy="cmd2">复制</button></li>
        <li>回到这里点<button id="restart2">重启连接器</button>，让它读新密钥。</li>
      </ol>
      <p class="hint" style="margin:6px 0 0">抓密钥要 root 跑 gdb，连接器自己没这个权限，所以要在终端里运行。</p>
    </section>
  </div>
  <section class="card"><h2>运行日志（最后 200 行）</h2><pre id="log">读取中</pre></section>
</main>
<div id="notice"></div>
<script>
const LEVEL = { ok: "正常", warn: "要留意", bad: "有问题" };
const TOKEN = document.querySelector('meta[name="wxc-token"]').content;
let busy = false, alerting = false, settings = null;
function notice(text) {
  const n = document.getElementById("notice");
  n.textContent = text; n.style.display = "block";
  clearTimeout(notice.t); notice.t = setTimeout(() => { n.style.display = "none"; }, 5000);
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
  notice("连接器正在重启，十来秒后自动刷新页面");
  setTimeout(() => location.reload(), 12000);
}
async function restart() {
  if (busy) { notice("正在发消息，等发完再重启"); return; }
  if (!confirm("重启连接器？大约十几秒收不到、发不了消息，之后自动补收这期间的新消息。")) return;
  try { await post("api/restart", {}); afterRestart(); } catch (e) { notice("没重启：" + e.message); }
}
document.getElementById("restart").onclick = restart;
document.getElementById("restart2").onclick = restart;
document.getElementById("pause").onclick = async () => {
  const stop = document.getElementById("pause").dataset.paused !== "1";
  if (stop && !confirm("暂停发送？暂停期间 AstrBot 要发的消息会被拒，恢复以后也不会补发。收消息不受影响。")) return;
  try { await post("api/stop", { stop }); notice(stop ? "已暂停发送" : "已恢复发送"); refresh(); }
  catch (e) { notice("没成功：" + e.message); }
};
function alertPref() { try { return localStorage.getItem("wxc-notify") === "1"; } catch (e) { return false; } }
function showNotifyButton() {
  const b = document.getElementById("notify");
  const on = alertPref() && "Notification" in window && Notification.permission === "granted";
  b.textContent = on ? "关闭掉线通知" : "开启掉线通知";
}
document.getElementById("notify").onclick = async () => {
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
document.querySelectorAll("[data-copy]").forEach(b => b.onclick = async () => {
  try { await navigator.clipboard.writeText(document.getElementById(b.dataset.copy).textContent); notice("已复制"); }
  catch (e) { notice("复制不了，手动选中复制吧"); }
});
async function loadSettings() {
  const box = document.getElementById("settings");
  try {
    const r = await fetch("api/settings", { cache: "no-store" });
    settings = await r.json();
  } catch (e) { box.textContent = "读不了设置"; return; }
  box.className = ""; box.replaceChildren();
  settings.toggles.forEach(t => {
    const f = el("div", "field"), cb = el("input");
    cb.type = "checkbox"; cb.checked = t.value; cb.dataset.key = t.key; cb.dataset.kind = "toggle";
    f.append(el("label", "", t.label), cb); box.append(f);
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
  settings.lists.forEach(l => {
    const f = el("div", "field block"), ta = el("textarea");
    ta.value = l.value.join("\n"); ta.dataset.key = l.key; ta.dataset.kind = "list"; ta.id = "list-" + l.key;
    const named = l.value.filter(x => l.names[x]).map(x => l.names[x]);
    f.append(el("label", "", l.label), ta);
    if (named.length) f.append(el("div", "hint", "现在有：" + named.join("、")));
    box.append(f);
  });
}
document.getElementById("save").onclick = async () => {
  if (busy) { notice("正在发消息，等发完再保存"); return; }
  const data = {};
  document.querySelectorAll("#settings [data-key]").forEach(x => {
    const k = x.dataset.key, kind = x.dataset.kind;
    data[k] = kind === "toggle" ? x.checked : kind === "number" ? Number(x.value)
      : kind === "list" ? x.value.split("\n").map(v => v.trim()).filter(Boolean) : x.value;
  });
  if (!confirm("保存这些设置并重启连接器？")) return;
  try { await post("api/settings", data); afterRestart(); } catch (e) { notice("没保存：" + e.message); }
};
async function find() {
  const q = document.getElementById("q").value.trim(), out = document.getElementById("found");
  if (!q) return;
  try {
    const r = await fetch("api/lookup?q=" + encodeURIComponent(q), { cache: "no-store" });
    const list = await r.json();
    out.replaceChildren(...(list.length ? list.map(x => {
      const d = el("div", "found"), b = el("button", "", x.group ? "加到群白名单" : "加到私聊白名单");
      b.onclick = () => {
        const ta = document.getElementById("list-" + (x.group ? "group_whitelist" : "whitelist"));
        const items = ta.value.split("\n").map(v => v.trim()).filter(Boolean);
        if (!items.includes(x.wxid)) { items.push(x.wxid); ta.value = items.join("\n"); }
        notice("已加进去，点「保存并重启」才生效");
      };
      d.append(el("span", "", (x.group ? "群 " : "") + x.name + "  " + x.wxid), b);
      return d;
    }) : [el("div", "hint", "没找到")]));
  } catch (e) { out.textContent = "查不了"; }
}
document.getElementById("find").onclick = find;
document.getElementById("q").onkeydown = e => { if (e.key === "Enter") find(); };
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
async function refresh() {
  try {
    const r = await fetch("api/status", { cache: "no-store" });
    if (!r.ok) throw new Error(r.status);
    const s = await r.json();
    busy = s.busy;
    const pause = document.getElementById("pause");
    pause.dataset.paused = s.paused ? "1" : "0";
    pause.textContent = s.paused ? "恢复发送" : "暂停发送";
    const db = s.checks.find(c => c.name === "数据库");
    if (db) { const d = document.getElementById("dbstate"); d.className = db.level; d.textContent = db.text; }
    checkAlert(s);
    const overall = document.getElementById("overall");
    overall.className = "pill " + s.level;
    overall.textContent = LEVEL[s.level];
    document.getElementById("meta").textContent = "版本 " + s.version + " · 已运行 " + s.uptime;
    const checks = document.getElementById("checks");
    checks.replaceChildren(...s.checks.map(c => {
      const d = el("div", "check");
      d.append(el("span", "dot " + c.level), el("b", "", c.name), el("span", "", c.text));
      return d;
    }));
    const counts = Object.entries(s.counts);
    document.getElementById("counts").replaceChildren(
      ...(counts.length ? counts.map(([k, v]) => row([k, v])) : [row(["还没有", ""])]));
    document.getElementById("sends").replaceChildren(
      row(["时间", "发给", "内容", "结果"], true),
      ...(s.recent_sends.length ? s.recent_sends.map(x => row([x.ago + "前", x.to, x.what,
        x.ok ? { cls: "ok", text: "发出" } : { cls: "bad", text: x.reason || "失败" }])) : [row(["启动后还没发过", "", "", ""])]));
    document.getElementById("config").replaceChildren(
      ...Object.entries(s.config).map(([k, v]) => row([k, v === true ? "开" : v === false ? "关" : v])));
  } catch (e) {
    const overall = document.getElementById("overall");
    overall.className = "pill bad";
    overall.textContent = "连接器没响应";
    document.title = "⚠ 没响应 · 微信连接器";
  }
}
async function refreshLog() {
  try {
    const r = await fetch("api/log", { cache: "no-store" });
    const pre = document.getElementById("log");
    const atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 20;
    pre.textContent = await r.text();
    if (atBottom) pre.scrollTop = pre.scrollHeight;
  } catch (e) {}
}
showNotifyButton(); loadSettings();
refresh(); refreshLog();
setInterval(refresh, 5000);
setInterval(refreshLog, 10000);
</script>
</body>
</html>
"""
