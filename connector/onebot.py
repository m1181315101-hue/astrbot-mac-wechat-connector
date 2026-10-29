"""OneBot v11 反向 WebSocket 客户端（Universal 角色），对接 AstrBot 的 aiocqhttp 适配器

连接方式和 NapCat 接 AstrBot 一样：连 ws://astrbot:<端口>/ws，
请求头带 Authorization: Bearer <口令>、X-Self-ID、X-Client-Role: Universal。
事件（post_type）由这边推过去；AstrBot 发来的 {"action", "params", "echo"} 交给回调处理后原样带 echo 回复。
连接部分参考 jessiongod/wechat-onebot-bridge（MIT）的 ob_client.py。
"""
import asyncio
import hashlib
import json
import logging
import re
import time
from collections import deque

import websockets

log = logging.getLogger("wechat-connector")


def stable_id(text: str) -> int:
    """wxid → 正的 32 位整数，每次重启都一样（不能用 Python 的 hash()，它每次进程启动都会变）"""
    return int.from_bytes(hashlib.sha256(text.encode()).digest()[:4], "big") & 0x7FFFFFFF or 1


class OneBotClient:
    def __init__(self, url: str, token: str, self_id: int, on_action, buffer_seconds: int = 300):
        self.url = url
        self.token = token
        self.self_id = self_id
        self.on_action = on_action  # async (action, params) -> (status, retcode, data)
        self.ws = None
        self.buffer: deque[tuple[float, dict]] = deque(maxlen=50)  # AstrBot 重启期间先攒着
        self.buffer_seconds = buffer_seconds
        self.connected_at = 0.0  # 给状态页看：这次连上的时间、断开过几次、最近一次断开的原因
        self.disconnects = 0
        self.last_error = ""

    async def push(self, event: dict) -> None:
        if self.ws is not None:
            try:
                await self.ws.send(json.dumps(event, ensure_ascii=False))
                return
            except websockets.ConnectionClosed:
                pass
        self.buffer.append((time.time(), event))
        log.warning("AstrBot 没连上，消息先暂存（%d 条）", len(self.buffer))

    def meta(self, meta_event_type: str, **extra) -> dict:
        return {"time": int(time.time()), "self_id": self.self_id, "post_type": "meta_event",
                "meta_event_type": meta_event_type, **extra}

    async def _flush(self) -> None:
        cutoff = time.time() - self.buffer_seconds
        while self.buffer:
            ts, event = self.buffer.popleft()
            if ts >= cutoff:
                await self.ws.send(json.dumps(event, ensure_ascii=False))

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(30)
            status = {"online": True, "good": True}
            await self.ws.send(json.dumps(self.meta("heartbeat", status=status, interval=30000)))

    async def _answer(self, req: dict) -> None:
        action, params, echo = req.get("action", ""), req.get("params") or {}, req.get("echo")
        try:
            status, retcode, data = await self.on_action(action, params)
        except Exception as e:
            log.exception("处理 %s 出错", action)
            status, retcode, data = "failed", 1400, {"message": str(e)}
        resp = {"status": status, "retcode": retcode, "data": data, "echo": echo}
        if status != "ok":
            resp["message"] = resp["wording"] = (data or {}).get("message", "") if isinstance(data, dict) else ""
        if self.ws is not None:
            await self.ws.send(json.dumps(resp, ensure_ascii=False))

    async def run_forever(self) -> None:
        headers = {"X-Self-ID": str(self.self_id), "X-Client-Role": "Universal", "User-Agent": "OneBot/11"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        while True:
            try:
                async with websockets.connect(self.url, additional_headers=headers, max_size=32 * 1024 * 1024) as ws:
                    self.ws = ws
                    self.connected_at = time.time()
                    log.info("已连上 AstrBot：%s", self.url)
                    await ws.send(json.dumps(self.meta("lifecycle", sub_type="connect")))
                    await self._flush()
                    hb = asyncio.create_task(self._heartbeat())
                    try:
                        async for raw in ws:
                            try:
                                req = json.loads(raw)
                            except ValueError:
                                continue
                            if isinstance(req, dict) and "action" in req:
                                asyncio.create_task(self._answer(req))
                    finally:
                        hb.cancel()
            except (OSError, websockets.WebSocketException) as e:
                log.warning("连接 AstrBot 失败或断开（%s），5 秒后重连", type(e).__name__)
                self.last_error = type(e).__name__
            if self.ws is not None:
                self.disconnects += 1
            self.ws = None
            await asyncio.sleep(5)


def _reply_seg(reply_id: int | None) -> tuple[list[dict], str]:
    """引用回复：最前面放一个 reply 段，AstrBot 会用 get_msg 回来取被引用的那条"""
    if not reply_id:
        return [], ""
    return [{"type": "reply", "data": {"id": str(reply_id)}}], f"[CQ:reply,id={reply_id}]"


def _image_seg(image: dict | None) -> tuple[list[dict], str]:
    """收到的表情包：放一个 image 段，格式和 NapCat 一样（file 是文件名，url 是下载地址）。
    表情包不用 base64://：AstrBot 本身认，但有的插件（比如陪伴插件的群聊看图）先按 file 读本地文件、读不到再用 url。
    照片没有能下载的地址，只能 file 放 base64://，不带 url"""
    if not image:
        return [], ""
    data = {"file": image["file"]}
    if image.get("url"):
        data["url"] = image["url"]
    if image.get("image_data_url"):  # 只有引用里的图片带，给只认公网地址的插件用（陪伴插件按字段名里有没有 image 认图）
        data["image_data_url"] = image["image_data_url"]
    return [{"type": "image", "data": data}], "[图片]"


# 陪伴插件（astrbot_plugin_private_companion）看到消息文字里有这几个词，会先拿这个人最近一次的聊天记录，
# 连着转两份时第二份就读成了第一份；给模型看的标签里避开它们，插件才会按这条消息自己的 forward 段去取
_LATEST_HINTS = ("转发", "合并消息", "聊天记录")


def record_label(title: str) -> str:
    """「[对话记录] 小红和小明」：微信的标题多是「…的聊天记录」，去掉这个尾巴；剩下的还带那几个词就不写标题"""
    short = re.sub(r"的?聊天记录$", "", title or "").strip()
    if any(w in short for w in _LATEST_HINTS):
        short = ""
    return f"[对话记录] {short}".strip()


def _forward_seg(forward: dict | None) -> tuple[list[dict], str]:
    """收到的聊天记录：一段文字「[对话记录] 标题」加一个 forward 段（和 NapCat 一样只带 id），内容用 get_forward_msg 取。
    要带那段文字：消息里只有 forward 段时 AstrBot 当成空消息，不交给模型"""
    if not forward:
        return [], ""
    label = record_label(forward["title"])
    return ([{"type": "text", "data": {"text": label}}, {"type": "forward", "data": {"id": forward["id"]}}],
            f"{label}[CQ:forward,id={forward['id']}]")


def forward_nodes(self_id: int, items: list[dict], message_type: str) -> dict:
    """get_forward_msg 的返回，和 NapCat 一样是 {"messages": [节点]}，每个节点像一条消息（sender、time、message 段）。
    items 是聊天记录里的每一条（name、time、text）；只有昵称，所以 sender 不带 user_id"""
    nodes = []
    for i, item in enumerate(items):
        text = item["text"] or "[消息]"
        nodes.append({"self_id": self_id, "time": item["time"], "message_type": message_type,
                      "message_id": i + 1, "sender": {"nickname": item["name"], "card": item["name"]},
                      "message": [{"type": "text", "data": {"text": text}}], "raw_message": text,
                      "message_format": "array", "post_type": "message"})
    return {"messages": nodes}


def private_message_event(self_id: int, user_id: int, nickname: str, message_id: int, text: str, ts: int,
                          reply_id: int | None = None, image: dict | None = None,
                          forward: dict | None = None) -> dict:
    reply, raw = _reply_seg(reply_id)
    img, img_raw = _image_seg(image)
    fwd, fwd_raw = _forward_seg(forward)
    return {
        "time": ts, "self_id": self_id, "post_type": "message", "message_type": "private", "sub_type": "friend",
        "message_id": message_id, "user_id": user_id, "font": 0,
        "message": reply + img + fwd + ([{"type": "text", "data": {"text": text}}] if text else []),
        "raw_message": raw + img_raw + fwd_raw + text, "message_format": "array",
        "sender": {"user_id": user_id, "nickname": nickname, "sex": "unknown", "age": 0},
    }


def group_message_event(self_id: int, group_id: int, user_id: int, nickname: str, message_id: int,
                        text: str, at_self: bool, ts: int, reply_id: int | None = None,
                        image: dict | None = None, forward: dict | None = None) -> dict:
    """at_self：微信里有人 @ 了小号，就在前面放一个 at 段，AstrBot 按「被 @」处理"""
    reply, raw = _reply_seg(reply_id)
    img, img_raw = _image_seg(image)
    fwd, fwd_raw = _forward_seg(forward)
    message = reply + ([{"type": "at", "data": {"qq": str(self_id)}}] if at_self else []) + img + fwd + \
        ([{"type": "text", "data": {"text": text}}] if text else [])  # 只 @ 不说话时正文为空
    raw += (f"[CQ:at,qq={self_id}] " if at_self else "") + img_raw + fwd_raw + text
    return {
        "time": ts, "self_id": self_id, "post_type": "message", "message_type": "group", "sub_type": "normal",
        "message_id": message_id, "group_id": group_id, "user_id": user_id, "font": 0, "anonymous": None,
        "message": message, "raw_message": raw, "message_format": "array",
        "sender": {"user_id": user_id, "nickname": nickname, "card": nickname, "role": "member"},
    }
