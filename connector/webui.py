"""只读状态页：微信进程和窗口、数据库密钥、AstrBot 连接、发送队列、收消息计数、运行日志

监听容器里的 127.0.0.1:6299（webui_host / webui_port），只有同一个容器里的 nginx 能访问到；
nginx 把网页桌面（3001 端口，有密码）的 /wxc/ 转过来，所以打开 https://localhost:3001/wxc/ 就能看，密码和网页桌面一样。
只读：不改配置、不发消息、不动微信窗口（查窗口用界面驱动的 status，不用会把窗口拉出来的 ping）。
页面和接口里没有消息内容、昵称和令牌，会话只用 OneBot 号表示。

  GET /            页面（每 5 秒刷新一次状态）
  GET /api/status  状态 JSON，也可以给脚本拿来做掉线提醒
  GET /api/log     connector.log 最后 200 行
"""
import asyncio
import json
import logging
import os
import time

log = logging.getLogger("wechat-connector")
WINDOW_CHECK_SECONDS = 30  # 查窗口要占用界面驱动，最多 30 秒一次，正在发消息时不查
QUIET_WARN_SECONDS = 6 * 3600  # 这么久数据库都没变化，多半是掉线了（夜里群也安静的话可能误报）


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
    if os.path.exists(os.path.join(c.state_dir, "STOP")):
        checks.append(("发送", "warn", "有 STOP 文件，暂停发送（删掉就恢复）"))
    elif len(sends) >= 3 and not any(s["ok"] for s in sends[-3:]):
        checks.append(("发送", "warn", f"最近 3 次都没发出去：{sends[-1]['reason']}"))
    elif queued >= 3:
        checks.append(("发送", "warn", f"排队 {queued} 条"))
    else:
        checks.append(("发送", "ok", f"排队 {queued} 条" if queued else "空闲"))

    async with c.db_lock:
        people = len(await asyncio.to_thread(c.private_targets))
    level = "bad" if any(x[1] == "bad" for x in checks) else "warn" if any(x[1] == "warn" for x in checks) else "ok"
    return {
        "version": c.version, "now": now, "uptime": ago(now - c.started), "level": level,
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


async def serve(c, host: str, port: int) -> None:
    probe = WindowProbe()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            method, target, _ = head.split(b"\r\n", 1)[0].decode("latin-1").split(" ", 2)
            path = target.split("?", 1)[0]
            ctype = "text/plain; charset=utf-8"
            if method not in ("GET", "HEAD"):
                code, body = 405, "只读，只支持 GET".encode()
            elif path == "/":
                code, ctype, body = 200, "text/html; charset=utf-8", PAGE.encode()
            elif path == "/api/status":
                code, ctype = 200, "application/json; charset=utf-8"
                body = json.dumps(await status(c, probe), ensure_ascii=False).encode()
            elif path == "/api/log":
                code, body = 200, (await asyncio.to_thread(tail_log, c.state_dir)).encode()
            else:
                code, body = 404, "没有这个页面".encode()
            reason = {200: "OK", 404: "Not Found", 405: "Method Not Allowed"}[code]
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
ul { margin: 0; padding-left: 18px; }
.scroll { overflow-x: auto; }
</style>
</head>
<body>
<main>
  <header>
    <h1>微信连接器</h1>
    <span id="overall" class="pill warn">读取中</span>
    <span id="meta" class="muted"></span>
    <span class="muted" style="margin-left:auto"><a href="../">打开网页桌面</a></span>
  </header>
  <div class="grid">
    <section class="card"><h2>状态</h2><div id="checks"></div></section>
    <section class="card"><h2>收发计数（启动以来）</h2><table class="kv" id="counts"></table></section>
  </div>
  <div class="grid">
    <section class="card scroll"><h2>最近发送</h2><table id="sends"></table></section>
    <section class="card"><h2>配置</h2><table class="kv" id="config"></table></section>
  </div>
  <section class="card" style="margin-bottom:12px">
    <h2>出问题时</h2>
    <ul>
      <li>窗口找不到、窗口很小，或者很久没有新数据：先<a href="../">打开网页桌面</a>看微信是不是要重新扫码登录。</li>
      <li>重新登录后数据库读不了（密钥不能用）：在宿主机上运行 <code>docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py capture</code>，再扫码登录，最后运行 <code>docker exec -u 0 wechat /usr/bin/python3 /opt/wechat-connector/wxkey.py verify</code>。</li>
      <li>想先停发：在 <code>./data/wechat-connector/</code> 里放一个名为 <code>STOP</code> 的文件，删掉恢复。</li>
    </ul>
  </section>
  <section class="card"><h2>运行日志（最后 200 行）</h2><pre id="log">读取中</pre></section>
</main>
<script>
const LEVEL = { ok: "正常", warn: "要留意", bad: "有问题" };
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
refresh(); refreshLog();
setInterval(refresh, 5000);
setInterval(refreshLog, 10000);
</script>
</body>
</html>
"""
