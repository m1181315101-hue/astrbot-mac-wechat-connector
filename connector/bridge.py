#!/usr/bin/env python3
"""微信 ↔ AstrBot 连接器（OneBot v11）

收：轮询本地数据库（wxdb），白名单里的人（私聊；写 "*" 就是所有好友）和白名单里的群发来的新文字 → OneBot 消息事件 → AstrBot
    群里有人 @ 小号时，事件前面加一个 at 段，AstrBot 按「被 @」处理
    引用回复：事件前面加一个 reply 段，被引用的那条记在内存里，AstrBot 用 get_msg 来取；
    引用的是小号的话，AstrBot 当成在叫它（和 QQ 上回复机器人的消息一样）
回：AstrBot 的 send_private_msg / send_group_msg → 发送队列 → 界面驱动（wxui）切换会话、核对、粘贴、回车
    → 回查数据库确认。文字、图片（GIF 是动画表情）、语音（原生语音条，不行就退回 mp3 文件）按原顺序一段一段发
    界面只有一个：所有要发的消息按到达顺序排成一队，一条（含它的所有段）发完再发下一条；
    界面没动过、一个字没发出去的失败（核对没过、没找到搜索框）等 2 秒重试一次

用法（在微信容器里，用和微信相同的用户运行）：
  python3 bridge.py run    [--config /config/wechat-connector/config.json]
  python3 bridge.py check  [--config ...]            检查密钥、数据库、界面驱动是否就绪
  python3 bridge.py lookup <备注名或昵称> [--config ...]  按名字查 wxid（配置白名单用）
  python3 bridge.py test-media [--config ...]         给「文件传输助手」发一组文字、图片、动图、语音，自测发送
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
from collections import OrderedDict, deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import media  # noqa: E402
import onebot  # noqa: E402
import wxdb  # noqa: E402

VERSION = "0.5.0"
HERE = os.path.dirname(os.path.abspath(__file__))
# 界面驱动的这些失败都发生在回车之前（回车前核对失败时已经清空输入框），一个字都没发出去，可以重试
RETRYABLE = ("粘贴前核对失败", "回车前核对失败", "没找到搜索框", "打开后输入框名字对不上", "找不到微信主窗口")
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
    "media_dir": "/tmp/wechat-connector-media",
    "group_voice_min_seconds": 20,
    "voice_mode": "native",
    "max_queue_wait": 150,  # 排队超过这么多秒还没轮到就不发了（AstrBot 等回执最多 180 秒，超时后插件可能已经改发文字）
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


WECHAT_ENV_KEYS = (b"DISPLAY", b"DBUS_SESSION_BUS_ADDRESS", b"PULSE_SERVER", b"PULSE_RUNTIME_PATH", b"XDG_RUNTIME_DIR")


def wechat_env() -> dict:
    """界面驱动要和微信用同一个 X 显示、会话总线和声音服务：从微信主进程的环境变量里读"""
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
        found = {k.decode(): v.decode() for k, v in pairs if k in WECHAT_ENV_KEYS}
        if b"DISPLAY" in dict(pairs) and b"DBUS_SESSION_BUS_ADDRESS" in dict(pairs):
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


def message_parts(message, face_names: dict[str, str], at_name=lambda qq: "") -> tuple[list[tuple[str, str]], list[str]]:
    """OneBot 消息 → 按顺序的 [(text, 文字) / (image, file) / (record, file)]，相邻的文字合成一段；
    另外返回丢掉的段类型。QQ 表情编号翻回 [名字]，微信自己认这种写法；
    at 段写成「@名字 」文字（只是显示，不会真的提醒对方）"""
    if isinstance(message, str):
        text = re.sub(r"\[CQ:[^\]]*\]", "", message)
        text = text.replace("&#91;", "[").replace("&#93;", "]").replace("&#44;", ",").replace("&amp;", "&")
        return ([("text", text.strip())] if text.strip() else []), []
    parts: list[tuple[str, str]] = []
    dropped = []

    def add_text(t: str) -> None:
        if parts and parts[-1][0] == "text":
            parts[-1] = ("text", parts[-1][1] + t)
        else:
            parts.append(("text", t))

    for seg in message or []:
        if not isinstance(seg, dict):
            continue
        kind, data = seg.get("type"), seg.get("data") or {}
        if kind == "text":
            add_text(str(data.get("text", "")))
        elif kind == "face" and str(data.get("id")) in face_names:
            add_text(f"[{face_names[str(data.get('id'))]}]")
        elif kind == "at":
            name = at_name(str(data.get("qq", "")))
            if name:
                add_text(f"@{name} ")
        elif kind in ("image", "record") and data.get("file"):
            parts.append((kind, str(data["file"])))
        elif kind != "reply":
            dropped.append(str(kind))
    parts = [(k, v.strip() if k == "text" else v) for k, v in parts]
    return [(k, v) for k, v in parts if v], dropped


class Connector:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.state_dir = cfg["state_dir"]
        self.keys = wxdb.KeyStore(os.path.join(self.state_dir, "keys.json"))
        self.store = wxdb.WeChatStore(cfg["files_root"], self.keys)
        self.state = State(os.path.join(self.state_dir, "state.json"))
        self.all_friends = "*" in cfg["whitelist"]  # "*"：通讯录里的所有好友都能私聊
        self.whitelist = {w for w in cfg["whitelist"] if w != "*"}
        self.groups = {g for g in cfg["group_whitelist"] if g.endswith("@chatroom")}
        self.self_id = onebot.stable_id(self.store.self_wxid)
        self.ui = UiDriver(cfg["ui_python"], wechat_env())
        self.db_lock = asyncio.Lock()
        self.send_queue: asyncio.Queue = asyncio.Queue()
        self.sending = False  # 发送队列正在发一条
        self.sent_times: deque[float] = deque()
        self.at_pattern = None  # init_marks 里按小号昵称生成
        self.quoted: OrderedDict[int, dict] = OrderedDict()  # 被引用的消息，给 get_msg 用
        self.media = media.MediaDir(cfg["media_dir"])
        self.face_names = {}
        if cfg.get("face_map"):
            with open(cfg["face_map"], encoding="utf-8") as f:
                self.face_names = {str(v): k for k, v in json.load(f).items()}
        self.ob = onebot.OneBotClient(cfg["astrbot_ws_url"], cfg["access_token"], self.self_id, self.on_action)

    # ---------- 收 ----------

    async def _refresh(self) -> bool:
        async with self.db_lock:
            return await asyncio.to_thread(self.store.refresh)

    def private_targets(self) -> set[str]:
        return self.whitelist | (self.store.friends() if self.all_friends else set())

    async def init_marks(self) -> None:
        """启动时：已经存在的会话从最新一条开始，不回放历史；之后才出现的会话表从头读"""
        await self._refresh()
        for wxid in self.private_targets() | self.groups:
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
            async with self.db_lock:
                targets = await asyncio.to_thread(self.private_targets)
            for wxid in targets | self.groups:
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
                log.info("跳过 %s 的非文字消息 type=%s（只收文字和引用）", cid, m["type"])
            return
        mid = onebot.stable_id(f"{chat}:{m['local_id']}:{m['time']}")
        quote = m.get("quote")
        reply_id = self.remember_quote(chat, quote) if quote else None
        quote_note = ("，引用了小号的话" if quote["sender"] == self.store.self_wxid else "，引用了一条消息") if quote else ""
        if chat.endswith("@chatroom"):
            sender = m["sender"] or "unknown"
            uid = self.state.id_of(sender)
            text = m["text"]
            at_self = bool(self.at_pattern and self.at_pattern.search(text))
            if at_self:
                text = self.at_pattern.sub("", text, count=1).strip()
            log.info("群 %s 里 %s 发了文字 %d 字%s%s", cid, uid, len(text), "，@ 了小号" if at_self else "", quote_note)
            event = onebot.group_message_event(self.self_id, cid, uid, self.store.display_name(sender), mid,
                                               text, at_self, int(m["time"]), reply_id)
        else:
            log.info("收到 %s 的私聊文字 %d 字%s", cid, len(m["text"]), quote_note)
            event = onebot.private_message_event(self.self_id, cid, self.store.display_name(chat), mid,
                                                 m["text"], int(m["time"]), reply_id)
        if not self.cfg["receive_only"]:
            await self.ob.push(event)

    def remember_quote(self, chat: str, quote: dict) -> int:
        """把被引用的那条做成一条消息记下来（最多 200 条），返回它的消息 ID；
        原作者是小号时 user_id 用小号自己的 ID，AstrBot 才知道引用的是它"""
        rid = onebot.stable_id(f"{chat}:quote:{quote['svrid'] or quote['text']}")
        author = quote["sender"] or chat
        if author == self.store.self_wxid:
            uid, name = self.self_id, self.store.self_name() or quote["name"]
        else:
            uid, name = self.state.id_of(author), quote["name"] or self.store.display_name(author)
        ts = quote["time"] or int(time.time())
        if chat.endswith("@chatroom"):
            msg = onebot.group_message_event(self.self_id, self.state.id_of(chat), uid, name, rid,
                                             quote["text"], False, ts)
        else:
            msg = onebot.private_message_event(self.self_id, uid, name, rid, quote["text"], ts)
        msg.pop("post_type")
        self.quoted[rid] = msg
        self.quoted.move_to_end(rid)
        while len(self.quoted) > 200:
            self.quoted.popitem(last=False)
        return rid

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
        if action == "get_msg":
            msg = self.quoted.get(int(params.get("message_id") or 0))
            return ("ok", 0, msg) if msg else ("failed", 1404, {"message": "只能取到别人引用过的消息"})
        if action == "get_status":
            return "ok", 0, {"online": True, "good": True}
        if action == "get_version_info":
            return "ok", 0, {"app_name": "wechat-connector", "app_version": VERSION, "protocol_version": "v11"}
        if action in ("can_send_image", "can_send_record"):
            return "ok", 0, {"yes": True}
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
        """先排队再检查：检查要读库（会让出控制权），放在排队前面的话后到的消息可能插队。
        等这条发完（或被拒）再回 AstrBot"""
        future = asyncio.get_running_loop().create_future()
        ahead = self.send_queue.qsize() + self.sending
        self.send_queue.put_nowait((time.time(), target_id, params, group, future))
        if ahead:
            log.info("发给 %s 的消息排队，前面还有 %d 条", target_id, ahead)
        return await future

    async def send_worker(self) -> None:
        while True:
            queued_at, target_id, params, group, future = await self.send_queue.get()
            self.sending = True
            waited = time.time() - queued_at
            try:
                if waited > self.cfg["max_queue_wait"]:
                    log.warning("发给 %s 的消息排队 %.0f 秒还没轮到，不发了", target_id, waited)
                    result = ("failed", 1504, {"message": "排队太久，没发"})
                else:
                    if waited >= 5:
                        log.info("发给 %s 的消息排队 %.0f 秒后开始发", target_id, waited)
                    result = await self._send_job(target_id, params, group)
            except Exception as e:  # 一条出错不能让整个队列停掉
                log.exception("发给 %s 时出错", target_id)
                result = ("failed", 1500, {"message": f"发送出错：{type(e).__name__}"})
            finally:
                self.sending = False
            if not future.done():
                future.set_result(result)

    async def _send_job(self, target_id: str, params: dict, group: bool):
        wxid = self.state.ids.get(target_id)
        if group:
            allowed = self.groups
        else:
            async with self.db_lock:  # 好友列表要读联系人库，别和轮询线程同时刷新
                allowed = await asyncio.to_thread(self.private_targets)
        if not wxid or wxid not in allowed:
            log.warning("拒绝发送：%s 不在%s白名单", target_id, "群" if group else "")
            return "failed", 1403, {"message": "目标不在白名单"}
        parts, dropped = message_parts(params.get("message"), self.face_names, self._at_name)
        if dropped:
            log.info("丢掉不支持的消息段 %s", dropped)
        if not parts:
            return "failed", 1400, {"message": "没有能发的内容（只支持文字、图片、语音）"}
        if os.path.exists(os.path.join(self.state_dir, "STOP")):  # 排队期间放的 STOP 也算
            log.warning("有 STOP 文件，暂停发送")
            return "failed", 1503, {"message": "连接器已暂停发送"}
        name = self.store.display_name(wxid)
        sections = ["群聊"] if group else ["联系人"]
        return await self.send_parts(target_id, wxid, name, sections, parts, group)

    async def send_parts(self, label: str, wxid: str, name: str, sections: list[str], parts, group: bool):
        """一段一段发；有一段发出去就算成功，全都没发出去才回失败（带上原因，插件好改用文字）。
        只能由发送队列（或单独跑的自测）调用：界面同一时间只能有一个人用"""
        sent, reasons = 0, []
        use_list = self.store.name_is_unique(wxid) if wxid != "filehelper" else True
        for kind, value in parts:
            if kind == "text":
                ok, why = await self._send_one(label, wxid, name, sections, use_list, text=value)
            else:
                items, why, data = await asyncio.to_thread(self._prepare_media, kind, value, group)
                ok = False
                for i, (how, path, secs) in enumerate(items):
                    if how == "voice":
                        ok, why = await self._send_one(label, wxid, name, sections, use_list, voice=path,
                                                       seconds=secs, types=(34,))
                        if not ok and i == 0:  # 第一段就没发出去：整段退回成音频文件
                            log.warning("语音条没发出去（%s），改发音频文件", why)
                            fallback, why, _ = await asyncio.to_thread(self._voice_as_file, data,
                                                                       sum(x[2] for x in items))
                            for _how, fpath, _secs in fallback:
                                ok, why = await self._send_one(label, wxid, name, sections, use_list,
                                                               file=fpath, types=(49,))
                            break
                    else:
                        ok, why = await self._send_one(label, wxid, name, sections, use_list, file=path,
                                                       types=(3, 47) if kind == "image" else (49,))
            if ok:
                sent += 1
            else:
                reasons.append(why)
        if sent:
            return "ok", 0, {"message_id": onebot.stable_id(f"out:{time.time_ns()}")}
        return "failed", 1500, {"message": "；".join(reasons) or "发送失败"}

    def _prepare_media(self, kind: str, value: str, group: bool) -> tuple[list[tuple[str, str, float]], str, bytes]:
        """返回 ([(file|voice, 路径, 秒数)], 失败原因, 原始数据)"""
        try:
            data = media.load_data(value)
        except Exception as e:
            return [], f"取不到{'图片' if kind == 'image' else '语音'}数据（{type(e).__name__}）", b""
        if not data:
            return [], "取不到媒体数据（可能是只在 AstrBot 容器里的文件路径）", b""
        if kind == "image":
            path = self.media.image_file(data)
            return ([("file", path, 0.0)], "", data) if path else ([], "不认识的图片格式", data)
        seconds = media.audio_seconds(data)
        min_seconds = self.cfg["group_voice_min_seconds"]
        if group and (min_seconds < 0 or seconds < min_seconds):
            log.info("群里不发 %.0f 秒的短语音（group_voice_min_seconds=%s）", seconds, min_seconds)
            return [], "群里不发短语音，请改用文字回复", data
        if self.cfg["voice_mode"] == "native":
            segments = self.media.voice_segments(data)
            if segments:
                return [("voice", path, secs) for path, secs in segments], "", data
        return self._voice_as_file(data, seconds)

    def _voice_as_file(self, data: bytes, seconds: float) -> tuple[list[tuple[str, str, float]], str, bytes]:
        path = self.media.voice_file(data, "翻唱.mp3" if seconds >= 20 else "语音.mp3")
        return ([("file", path, seconds)], "", data) if path else ([], "不认识的音频格式", data)

    async def _send_one(self, label: str, wxid: str, name: str, sections: list[str], use_list: bool,
                        text: str = "", file: str = "", voice: str = "", seconds: float = 0.0,
                        types: tuple[int, ...] = ()) -> tuple[bool, str]:
        await self._rate_limit()
        since = int(time.time()) - 2
        if text:
            td = self.cfg["typing_delay"]
            req = {"op": "send", "name": name, "text": text, "sections": sections, "use_list": use_list,
                   "delay": min(td["max"], td["base"] + td["per_char"] * len(text))}
        else:
            async with self.db_lock:
                await asyncio.to_thread(self.store.refresh)
                before = await asyncio.to_thread(self.store.self_count, wxid, since, types)
            req = {"op": "send", "name": name, "sections": sections, "use_list": use_list, "delay": 0.3}
            req.update({"voice": voice, "seconds": seconds} if voice else {"file": file})
        resp = await asyncio.to_thread(self.ui.request, req, 40.0 + seconds)
        if not resp.get("ok") and str(resp.get("reason", "")).startswith(RETRYABLE):
            log.info("发给 %s 没成（%s），2 秒后重试一次", label, resp.get("reason"))
            await asyncio.sleep(2.0)
            resp = await asyncio.to_thread(self.ui.request, req, 40.0 + seconds)
        what = f"{len(text)} 字" if text else (f"语音条 {seconds:.0f} 秒" if voice else os.path.basename(file))
        if not resp.get("ok"):
            log.warning("发给 %s 失败（%s）：%s", label, what, resp.get("reason"))
            return False, f"发送失败：{resp.get('reason')}"
        if text:
            confirmed = await self._confirm(lambda: self.store.sent_since(wxid, since, text))
        else:
            confirmed = await self._confirm(lambda: self.store.self_count(wxid, since, types) > before, 15.0)
        log.info("发给 %s %s，路径 %s，数据库确认%s", label, what, resp.get("path"),
                 "通过" if confirmed else "超时（界面显示已发出）")
        return True, ""

    async def _confirm(self, check, seconds: float = 8.0) -> bool:
        deadline = time.time() + seconds
        while time.time() < deadline:
            await asyncio.sleep(0.5)
            async with self.db_lock:
                await asyncio.to_thread(self.store.refresh)
                if await asyncio.to_thread(check):
                    return True
        return False

    async def run(self) -> None:
        people = f"所有好友（{len(self.private_targets())} 人）" if self.all_friends else f"{len(self.whitelist)} 人"
        log.info("微信连接器 %s 启动：私聊白名单 %s、%d 个群，%s", VERSION, people, len(self.groups),
                 "只收不发" if self.cfg["receive_only"] else f"连 {self.cfg['astrbot_ws_url']}")
        await self.init_marks()
        ping = await asyncio.to_thread(self.ui.request, {"op": "ping"}, 20.0)
        log.info("界面驱动：%s", "就绪" if ping.get("ok") else "没找到微信主窗口（检查无障碍组件）")
        tasks = [asyncio.create_task(self.poll_loop())]
        if not self.cfg["receive_only"]:
            tasks.append(asyncio.create_task(self.send_worker()))
            tasks.append(asyncio.create_task(self.ob.run_forever()))
        await asyncio.gather(*tasks)


def cmd_check(cfg: dict) -> int:
    keys = wxdb.KeyStore(os.path.join(cfg["state_dir"], "keys.json"))
    print("密钥原料：", "有" if keys.passphrase else "没有（先运行 wxkey.py capture）")
    store = wxdb.WeChatStore(cfg["files_root"], keys)
    store.refresh()
    print("消息库：", ", ".join(f"{os.path.basename(p)}={'可读' if d.con else '不可读'}" for p, d in store.msg_dbs.items()))
    print("通讯录条目：", len(store.contacts()))
    listed = [w for w in cfg["whitelist"] if w != "*"] + cfg["group_whitelist"]
    missing = [w for w in listed if w not in store.contacts()]
    people = f"所有好友（{len(store.friends())} 人）" if "*" in cfg["whitelist"] else f"{len(cfg['whitelist'])} 人"
    print("白名单：", people, "、", len(cfg["group_whitelist"]), "个群",
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


def cmd_test_media(cfg: dict) -> int:
    """不经过 AstrBot，直接把一组文字、图片、动图、语音发给「文件传输助手」，走和正式发送一样的路径"""
    import base64
    import io
    import math
    import struct
    import wave
    from PIL import Image, ImageDraw

    def b64(data: bytes) -> str:
        return "base64://" + base64.b64encode(data).decode()

    png = io.BytesIO()
    im = Image.new("RGB", (240, 160), (90, 160, 230))
    ImageDraw.Draw(im).ellipse((80, 40, 160, 120), fill=(255, 220, 80))
    im.save(png, "PNG")
    frames = []
    for i in range(4):
        f = Image.new("RGB", (160, 160), (255, 230, 120))
        ImageDraw.Draw(f).ellipse((20 + i * 20, 50, 80 + i * 20, 110), fill=(80, 140, 230))
        frames.append(f)
    gif = io.BytesIO()
    frames[0].save(gif, "GIF", save_all=True, append_images=frames[1:], duration=200, loop=0)
    wav = io.BytesIO()
    with wave.open(wav, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(b"".join(struct.pack("<h", int(6000 * math.sin(2 * math.pi * 523 * i / 16000)))
                               for i in range(32000)))
    parts = [("text", f"微信连接器 {VERSION} 自测：下面是一张图、一个动图、一段 2 秒的提示音"),
             ("image", b64(png.getvalue())), ("image", b64(gif.getvalue())), ("record", b64(wav.getvalue()))]

    async def go():
        c = Connector(cfg)
        await c._refresh()
        return await c.send_parts("文件传输助手", "filehelper", "文件传输助手", ["功能"], parts, group=False)

    status, _code, data = asyncio.run(go())
    print("结果：", status, data)
    return 0 if status == "ok" else 1


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "check", "lookup", "test-media"])
    ap.add_argument("name", nargs="?")
    ap.add_argument("--config", default="/config/wechat-connector/config.json")
    args = ap.parse_args()
    cfg = load_config(args.config)
    if args.cmd == "check":
        sys.exit(cmd_check(cfg))
    if args.cmd == "lookup":
        sys.exit(cmd_lookup(cfg, args.name or ""))
    setup_logging(cfg["state_dir"])
    if args.cmd == "test-media":
        sys.exit(cmd_test_media(cfg))
    asyncio.run(Connector(cfg).run())


if __name__ == "__main__":
    main()
