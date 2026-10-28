# -*- coding: utf-8 -*-
"""告诉她这条消息来自哪个平台（QQ 还是微信）。

AstrBot 给模型的发送者信息只有 User ID 和昵称，不分平台，接上微信以后她会在微信里说自己在 QQ，
认主人时也分不清一个 User ID 是 QQ 号还是微信连接器的号。这里按事件的平台 ID 在系统提示词末尾加一行：
- 平台 ID → 名字的对照在 config.json 的 platforms 里（默认 wechat → 微信、webchat → 网页面板），没列出的按 default_name（默认 QQ）；
- 人格里再写清楚：说自己在哪按【当前平台】来，认主人时 User ID 要和平台对上。
"""
import json
import os

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
MARKER = "【当前平台】"
DEFAULTS = {"platforms": {"wechat": "微信", "webchat": "网页面板"}, "default_name": "QQ"}


@register(
    "astrbot_plugin_platform_hint",
    "dafeiyu-local",
    "在系统提示词末尾标明这条消息来自 QQ 还是微信",
    "1.0.0",
)
class PlatformHintPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.cfg = dict(DEFAULTS)
        path = os.path.join(PLUGIN_DIR, "config.json")
        if os.path.isfile(path):
            try:
                with open(path, encoding="utf-8") as f:
                    self.cfg.update(json.load(f))
            except Exception as exc:  # noqa: BLE001 - 配置坏了就用默认值
                logger.warning(f"[platform_hint] 读 config.json 失败，用默认值: {exc}")

    @filter.on_llm_request()
    async def inject_platform(self, event: AstrMessageEvent, req) -> None:
        try:
            name = self.cfg["platforms"].get(event.get_platform_id(), self.cfg["default_name"])
            base = req.system_prompt or ""
            # 同一请求被重复处理时只留一行
            cut = base.find(MARKER)
            if cut != -1:
                end = base.find("\n", cut)
                base = base[:cut] + (base[end + 1:] if end != -1 else "")
            req.system_prompt = (base.rstrip() + f"\n\n{MARKER}这条消息来自{name}。").strip()
        except Exception as exc:  # noqa: BLE001 - 注入失败不能影响聊天
            logger.warning(f"[platform_hint] 标平台失败: {exc}")
