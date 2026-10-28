#!/usr/bin/env python3
"""微信 ↔ AstrBot 连接器（OneBot v11）· 第一阶段：文字

收：轮询本地数据库（wxdb），白名单里的人（私聊）和白名单里的群发来的新文字 → OneBot 消息事件 → AstrBot
    群里有人 @ 小号时，事件前面加一个 at 段，AstrBot 按「被 @」处理
回：AstrBot 的 send_private_msg / send_group_msg → 发送队列 → 界面驱动（wxui）切换会话、核对、粘贴、回车
    → 回查数据库确认

用法（在微信容器里，用和微信相同的用户运行）：
  python3 bridge.py run    [--config /config/wechat-connector/config.json]
  python3 bridge.py check  [--config ...]            检查密钥、数据库、界面驱动是否就绪
  python3 bridge.py lookup <备注名或昵称> [--config ...]  按名字查 wxid（配置白名单用）
"""
import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import re
import select
import subprocess
import sys
import threading
import time
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import onebot  # noqa: E402
import wxdb  # noqa: E402

VERSION = "0.1.0"
HERE = os.path.dirname(os.path.abspath(__file__))
log = logging.getLogger("wechat-connector")

DEFAULTS = {
    "astrbot_ws_url": "ws://astrbot:6200/ws",
    "access_token": "",
    "files_root": "/config/xwechat_files",
    "state_dir": "/config/wechat-connector",
    "whitelist": [],
    "group_whitelist": [],
    "bot_names": [],
    "receive_only": False,
    "poll_interval": 1.0,
    "max_message_age": 600,
    "max_sends_per_minute": 20,
    "typing_delay": {"base": 0.5, "per_char": 0.05, "max": 3.0},
    "ui_python": "/usr/bin/python3",
    "face_map": "",
}


def load_config(path: str) -> dict:
    cfg = json.loads(json.dumps(DEFAULTS))
    with open(path, encoding="utf-8") as f:
        cfg.update(json.load(f))
    for key in ("whitelist", "group_whitelist", "bot_names"):
        cfg[key] = [str(w).strip() for w in cfg[key] if str(w).strip()]
    return cfg


def setup_logging(state_dir: str) -> None:
    os.makedirs(state_dir, mode=0o700, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%m-%d %H:%M:%S")
    fh = logging.handlers.RotatingFileHandler(os.path.join(state_dir, "connector.log"), maxBytes=1 << 20,
                                              backupCount=3, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    for h in (fh, sh):
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)


def wechat_env() -> dict:
    """界面驱动要和微信用同一个 X 显示和会话总线：从微信主进程的环境变量里读"""
    env = {}
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            if os.path.basename(os.readlink(f"/proc/{d}/exe")) != "wechat":
                continue
            with open(f"/proc/{d}/environ", "rb") as f:
                pairs = [p.split(b"=", 1) for p in f.read().split(b"\0") if b"=" in p]
        except OSError:
            continue
        found = {k.decode(): v.decode() for k, v in pairs if k in (b"DISPLAY", b"DBUS_SESSION_BUS_ADDRESS")}
        if len(found) == 2:
            return found
        env.update(found)
    return env


class State:
    """state.json：每个会话处理到哪条（marks），以及 OneBot 整数 ID → wxid 的对照表"""

    def __init__(self, path: str):
        self.path = path
        self.marks: dict[str, dict[str, int]] = {}
        self.ids: dict[str, str] = {}
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            self.marks = data.get("marks", {})
            self.ids = data.get("ids", {})

    def save(self) -> None:
        tmp = self.path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"marks": self.marks, "ids": self.ids}, f)
        os.replace(tmp, self.path)

    def id_of(self, wxid: str) -> int:
        uid = onebot.stable_id(wxid)
        known = self.ids.get(str(uid))
        if known and known != wxid:
            raise ValueError(f"整数 ID {uid} 冲突")
        self.ids[str(uid)] = wxid
        return uid


class UiDriver:
    """wxui.py 常驻子进程，一问一答（JSON 行）"""

    def __init__(self, python: str, env: dict):
        self.python = python
        self.env = {**os.environ, **env}
        self.proc = None
        self.lock = threading.Lock()

    def _start(self) -> None:
        self.proc = subprocess.Popen([self.python, os.path.join(HERE, "wxui.py")], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=sys.stderr, env=self.env, text=True, bufsize=1)

    def request(self, req: dict, timeout: float = 40.0) -> dict:
        with self.lock:
            if self.proc is None or self.proc.poll() is not None:
                self._start()
            try:
                self.proc.stdin.write(json.dumps(req, ensure_ascii=False) + "\n")
                self.proc.stdin.flush()
                ready, _, _ = select.select([self.proc.stdout], [], [], timeout)
                line = self.proc.stdout.readline() if ready else ""
            except (BrokenPipeError, OSError):
                line = ""
            if not line:
                self.proc.kill()  # 卡住或崩了：下次重新拉起
                self.proc = None
                return {"ok": False, "reason": "界面驱动无响应"}
            return json.loads(line)


def segments_to_text(message, face_names: dict[str, str], at_name=lambda qq: "") -> tuple[str, list[str]]:
    """OneBot 消息 → 纯文字；返回 (文字, 丢掉的段类型)。
    QQ 表情编号翻回 [名字]，微信自己认这种写法；at 段写成「@名字 」文字（只是显示，不会真的提醒对方）"""
    dropped = []
    if isinstance(message, str):
        text = re.sub(r"\[CQ:[^\]]*\]", "", message)
        text = text.replace("&#91;", "[").replace("&#93;", "]").replace("&#44;", ",").replace("&amp;", "&")
        return text, dropped
    parts = []
    for seg in message or []:
        if not isinstance(seg, dict):
            continue
        kind, data = seg.get("type"), seg.get("data") or {}
        if kind == "text":
            parts.append(str(data.get("text", "")))
        elif kind == "face" and str(data.get("id")) in face_names:
            parts.append(f"[{face_names[str(data.get('id'))]}]")
        elif kind == "at":
            name = at_name(str(data.get("qq", "")))
            if name:
                parts.append(f"@{name} ")
        elif kind != "reply":
            dropped.append(str(kind))
    return "".join(parts), dropped


class Connector:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.state_dir = cfg["state_dir"]
        self.keys = wxdb.KeyStore(os.path.join(self.state_dir, "keys.json"))
        self.store = wxdb.WeChatStore(cfg["files_root"], self.keys)
        self.state = State(os.path.join(self.state_dir, "state.json"))
        self.whitelist = set(cfg["whitelist"])
        self.groups = {g for g in cfg["group_whitelist"] if g.endswith("@chatroom")}
        self.self_id = onebot.stable_id(self.store.self_wxid)
        self.ui = UiDriver(cfg["ui_python"], wechat_env())
        self.db_lock = asyncio.Lock()
        self.send_lock = asyncio.Lock()
        self.sent_times: deque[float] = deque()
        self.at_pattern = None  # init_marks 里按小号昵称生成
        self.face_names = {}
        if cfg.get("face_map"):
            with open(cfg["face_map"], encoding="utf-8") as f:
                self.face_names = {str(v): k for k, v in json.load(f).items()}
        self.ob = onebot.OneBotClient(cfg["astrbot_ws_url"], cfg["access_token"], self.self_id, self.on_action)

    # ---------- 收 ----------

    async def _refresh(self) -> bool:
        async with self.db_lock:
            return await asyncio.to_thread(self.store.refresh)

    async def init_marks(self) -> None:
        """启动时：已经存在的会话从最新一条开始，不回放历史；之后才出现的会话表从头读"""
        await self._refresh()
        for wxid in self.whitelist | self.groups:
            marks = self.state.marks.setdefault(wxid, {})
            for db, latest in self.store.latest_ids(wxid).items():
                marks.setdefault(db, latest)
            self.state.id_of(wxid)
        self.state.save()
        names = [self.store.self_name()] + self.cfg["bot_names"]
        names = sorted({n for n in names if n}, key=len, reverse=True)
        # 微信里 @ 某人是「@昵称」后面跟一个 U+2005 空格（手打的 @ 可能是普通空格或者在句尾）
        self.at_pattern = re.compile("@(?:" + "|".join(map(re.escape, names)) + r")(?:\u2005|\s|$)") if names else None

    async def poll_loop(self) -> None:
        warned = False
        while True:
            await asyncio.sleep(self.cfg["poll_interval"])
            try:
                if not await self._refresh():
                    continue
            except LookupError as e:
                if not warned:
                    log.error("%s：重新抓一次密钥（README 第 3 步）", e)
                    warned = True
                continue
            changed = False
            for wxid in self.whitelist | self.groups:
                marks = self.state.marks.setdefault(wxid, {})
                before = dict(marks)
                async with self.db_lock:
                    msgs = await asyncio.to_thread(self.store.new_messages, wxid, marks)
                changed |= marks != before
                for m in msgs:
                    try:
                        await self.deliver(wxid, m)
                    except Exception as e:  # 一条消息出错不能让整个轮询停掉
                        log.error("处理一条消息出错，已跳过：%s", type(e).__name__)
            if changed:
                self.state.save()

    async def deliver(self, chat: str, m: dict) -> None:
        """chat：私聊是对方 wxid，群是 xxx@chatroom；m["sender"] 是实际发言人的 wxid"""
        cid = self.state.id_of(chat)
        age = time.time() - (m["time"] or 0)
        if age > self.cfg["max_message_age"]:
            log.info("跳过 %s 的旧消息（%d 秒前）", cid, age)
            return
        if m["type"] != 1 or not m["text"]:
            if m["type"] != 10000:  # 10000 是「某某撤回了一条消息」这类系统提示
                log.info("跳过 %s 的非文字消息 type=%s（第一阶段只收文字）", cid, m["type"])
            return
        mid = onebot.stable_id(f"{chat}:{m['local_id']}:{m['time']}")
        if chat.endswith("@chatroom"):
            sender = m["sender"] or "unknown"
            uid = self.state.id_of(sender)
            text = m["text"]
            at_self = bool(self.at_pattern and self.at_pattern.search(text))
            if at_self:
                text = self.at_pattern.sub("", text, count=1).strip()
            log.info("群 %s 里 %s 发了文字 %d 字%s", cid, uid, len(text), "，@ 了小号" if at_self else "")
            event = onebot.group_message_event(self.self_id, cid, uid, self.store.display_name(sender), mid,
                                               text, at_self, int(m["time"]))
        else:
            log.info("收到 %s 的私聊文字 %d 字", cid, len(m["text"]))
            event = onebot.private_message_event(self.self_id, cid, self.store.display_name(chat), mid,
                                                 m["text"], int(m["time"]))
        if not self.cfg["receive_only"]:
            await self.ob.push(event)

    # ---------- 回 ----------

    async def on_action(self, action: str, params: dict):
        is_group = action == "send_group_msg" or (action == "send_msg" and (
            params.get("message_type") == "group" or ("group_id" in params and "user_id" not in params)))
        if is_group:
            return await self.send_to(str(params.get("group_id")), params, group=True)
        if action in ("send_private_msg", "send_msg"):
            return await self.send_to(str(params.get("user_id")), params, group=False)
        if action == "get_group_info":
            gid = self.state.ids.get(str(params.get("group_id")))
            name = self.store.display_name(gid) if gid else ""
            return "ok", 0, {"group_id": params.get("group_id"), "group_name": name,
                             "member_count": 0, "max_member_count": 0}
        if action == "get_group_member_info":
            uid = str(params.get("user_id"))
            wxid = self.state.ids.get(uid)
            nick = "微信" if uid == str(self.self_id) else (self.store.display_name(wxid) if wxid else uid)
            return "ok", 0, {"group_id": params.get("group_id"), "user_id": params.get("user_id"), "nickname": nick,
                             "card": nick, "role": "member"}
        if action == "get_group_member_list":
            return "ok", 0, []
        if action == "get_login_info":
            return "ok", 0, {"user_id": self.self_id, "nickname": "微信"}
        if action == "get_stranger_info":
            wxid = self.state.ids.get(str(params.get("user_id")))
            nick = self.store.display_name(wxid) if wxid else str(params.get("user_id"))
            return "ok", 0, {"user_id": params.get("user_id"), "nickname": nick, "sex": "unknown", "age": 0}
        if action == "get_status":
            return "ok", 0, {"online": True, "good": True}
        if action == "get_version_info":
            return "ok", 0, {"app_name": "wechat-connector", "app_version": VERSION, "protocol_version": "v11"}
        if action in ("can_send_image", "can_send_record"):
            return "ok", 0, {"yes": False}
        return "failed", 1404, {"message": f"微信连接器不支持 {action}"}

    async def _rate_limit(self) -> None:
        limit = self.cfg["max_sends_per_minute"]
        while True:
            now = time.time()
            while self.sent_times and now - self.sent_times[0] > 60:
                self.sent_times.popleft()
            if len(self.sent_times) < limit:
                self.sent_times.append(now)
                return
            await asyncio.sleep(60 - (now - self.sent_times[0]) + 0.1)

    def _at_name(self, qq: str) -> str:
        wxid = self.state.ids.get(qq)
        return self.store.display_name(wxid) if wxid else ""

    async def send_to(self, target_id: str, params: dict, group: bool):
        wxid = self.state.ids.get(target_id)
        allowed = self.groups if group else self.whitelist
        if not wxid or wxid not in allowed:
            log.warning("拒绝发送：%s 不在%s白名单", target_id, "群" if group else "")
            return "failed", 1403, {"message": "目标不在白名单"}
        uid = target_id
        text, dropped = segments_to_text(params.get("message"), self.face_names, self._at_name)
        text = text.strip()
        if dropped:
            log.info("丢掉不支持的消息段 %s（第一阶段只发文字）", dropped)
        if not text:
            return "failed", 1400, {"message": "第一阶段只支持文字"}
        if os.path.exists(os.path.join(self.state_dir, "STOP")):
            log.warning("有 STOP 文件，暂停发送")
            return "failed", 1503, {"message": "连接器已暂停发送"}
        async with self.send_lock:  # 界面只有一个：一条一条来
            await self._rate_limit()
            td = self.cfg["typing_delay"]
            delay = min(td["max"], td["base"] + td["per_char"] * len(text))
            since = int(time.time()) - 2
            req = {"op": "send", "name": self.store.display_name(wxid), "text": text,
                   "sections": ["群聊"] if group else ["联系人"],
                   "use_list": self.store.name_is_unique(wxid), "delay": delay}
            resp = await asyncio.to_thread(self.ui.request, req)
            if not resp.get("ok"):
                log.warning("发给 %s 失败：%s", uid, resp.get("reason"))
                return "failed", 1500, {"message": f"发送失败：{resp.get('reason')}"}
            confirmed = await self._confirm(wxid, since, text)
            log.info("发给 %s %d 字，路径 %s，数据库确认%s", uid, len(text), resp.get("path"),
                     "通过" if confirmed else "超时（界面显示已发出）")
        return "ok", 0, {"message_id": onebot.stable_id(f"out:{time.time_ns()}")}

    async def _confirm(self, wxid: str, since: int, text: str, seconds: float = 8.0) -> bool:
        deadline = time.time() + seconds
        while time.time() < deadline:
            await asyncio.sleep(0.5)
            async with self.db_lock:
                await asyncio.to_thread(self.store.refresh)
                if await asyncio.to_thread(self.store.sent_since, wxid, since, text):
                    return True
        return False

    async def run(self) -> None:
        log.info("微信连接器 %s 启动：白名单 %d 人、%d 个群，%s", VERSION, len(self.whitelist), len(self.groups),
                 "只收不发" if self.cfg["receive_only"] else f"连 {self.cfg['astrbot_ws_url']}")
        await self.init_marks()
        ping = await asyncio.to_thread(self.ui.request, {"op": "ping"}, 20.0)
        log.info("界面驱动：%s", "就绪" if ping.get("ok") else "没找到微信主窗口（检查无障碍组件）")
        tasks = [asyncio.create_task(self.poll_loop())]
        if not self.cfg["receive_only"]:
            tasks.append(asyncio.create_task(self.ob.run_forever()))
        await asyncio.gather(*tasks)


def cmd_check(cfg: dict) -> int:
    keys = wxdb.KeyStore(os.path.join(cfg["state_dir"], "keys.json"))
    print("密钥原料：", "有" if keys.passphrase else "没有（先运行 wxkey.py capture）")
    store = wxdb.WeChatStore(cfg["files_root"], keys)
    store.refresh()
    print("消息库：", ", ".join(f"{os.path.basename(p)}={'可读' if d.con else '不可读'}" for p, d in store.msg_dbs.items()))
    print("通讯录条目：", len(store.contacts()))
    listed = cfg["whitelist"] + cfg["group_whitelist"]
    missing = [w for w in listed if w not in store.contacts()]
    print("白名单：", len(cfg["whitelist"]), "人、", len(cfg["group_whitelist"]), "个群",
          f"（通讯录里找不到 {len(missing)} 个）" if missing else "")
    resp = UiDriver(cfg["ui_python"], wechat_env()).request({"op": "ping"}, 20.0)
    print("界面驱动：", "就绪" if resp.get("ok") else f"不可用 {resp}")
    return 0 if keys.passphrase and resp.get("ok") else 1


def cmd_lookup(cfg: dict, name: str) -> int:
    store = wxdb.WeChatStore(cfg["files_root"], wxdb.KeyStore(os.path.join(cfg["state_dir"], "keys.json")))
    hits = store.find_by_name(name)
    for wxid in hits:
        kind = "群" if wxid.endswith("@chatroom") else ("公众号" if wxid.startswith("gh_") else "联系人")
        print(f"{wxid}\t{kind}\tOneBot 号 {onebot.stable_id(wxid)}")
    if not hits:
        print("通讯录里没有这个备注名或昵称（要完全一致）")
    return 0 if hits else 1


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "check", "lookup"])
    ap.add_argument("name", nargs="?")
    ap.add_argument("--config", default="/config/wechat-connector/config.json")
    args = ap.parse_args()
    cfg = load_config(args.config)
    if args.cmd == "check":
        sys.exit(cmd_check(cfg))
    if args.cmd == "lookup":
        sys.exit(cmd_lookup(cfg, args.name or ""))
    setup_logging(cfg["state_dir"])
    asyncio.run(Connector(cfg).run())


if __name__ == "__main__":
    main()
