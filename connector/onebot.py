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
            self.ws = None
            await asyncio.sleep(5)


def private_message_event(self_id: int, user_id: int, nickname: str, message_id: int, text: str, ts: int) -> dict:
    return {
        "time": ts, "self_id": self_id, "post_type": "message", "message_type": "private", "sub_type": "friend",
        "message_id": message_id, "user_id": user_id, "font": 0,
        "message": [{"type": "text", "data": {"text": text}}], "raw_message": text, "message_format": "array",
        "sender": {"user_id": user_id, "nickname": nickname, "sex": "unknown", "age": 0},
    }


def group_message_event(self_id: int, group_id: int, user_id: int, nickname: str, message_id: int,
                        text: str, at_self: bool, ts: int) -> dict:
    """at_self：微信里有人 @ 了小号，就在前面放一个 at 段，AstrBot 按「被 @」处理"""
    message = ([{"type": "at", "data": {"qq": str(self_id)}}] if at_self else []) + \
        ([{"type": "text", "data": {"text": text}}] if text else [])  # 只 @ 不说话时正文为空
    raw = (f"[CQ:at,qq={self_id}] " if at_self else "") + text
    return {
        "time": ts, "self_id": self_id, "post_type": "message", "message_type": "group", "sub_type": "normal",
        "message_id": message_id, "group_id": group_id, "user_id": user_id, "font": 0, "anonymous": None,
        "message": message, "raw_message": raw, "message_format": "array",
        "sender": {"user_id": user_id, "nickname": nickname, "card": nickname, "role": "member"},
    }
