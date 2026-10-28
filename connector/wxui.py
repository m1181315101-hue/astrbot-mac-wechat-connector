#!/usr/bin/env python3
"""微信界面驱动：按名字切换会话、核对后粘贴发送（无障碍接口 AT-SPI + xdotool）

Linux 微信是 Qt 写的，带无障碍桥：打开 org.a11y.Status.IsEnabled 以后，整棵界面树都能读到。
这个进程由 bridge.py 启动，常驻；用系统 Python 跑（要有 python3-gi 和 gir1.2-atspi-2.0）。
标准输入每行一个 JSON 请求，标准输出每行一个 JSON 结果：

  {"op": "ping"}
  {"op": "send", "name": "备注名", "text": "...", "sections": ["联系人"], "use_list": true, "delay": 1.2}
  → {"ok": true, "path": "list|search|already"} / {"ok": false, "reason": "..."}

安全规则：
- 会话列表只在 use_list=true（调用方确认这个名字在通讯录里独一无二）时用，按第一行精确匹配；
- 搜索结果只认 sections 指定分组里名字完全相同的那一项，「搜索网络结果」「聊天记录」永远不点；
- 粘贴前、回车前都核对底部输入框的无障碍名字 == 目标名字，对不上就放弃，一个字都不发。
不往标准输出写任何聊天内容；日志只写到标准错误，而且只有名字长度、坐标和结果。
"""
import json
import subprocess
import sys
import time

import gi

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi  # noqa: E402

SECTION_TITLES = {"联系人", "群聊", "功能", "最常使用", "最近使用", "搜索网络结果", "聊天记录",
                  "公众号", "小程序", "搜一搜", "视频号", "服务", "文章", "企业微信联系人"}


def log(msg: str) -> None:
    print(f"[wxui] {msg}", file=sys.stderr, flush=True)


def sh(*args: str) -> str:
    return subprocess.run(args, capture_output=True, check=False).stdout.decode().strip()


def set_clipboard(data: bytes) -> None:
    # xclip 会留在后台占着剪贴板，输出不能接管道，否则会一直等它退出
    subprocess.run(["xclip", "-selection", "clipboard"], input=data,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def enable_a11y() -> bool:
    out = subprocess.run(["gdbus", "call", "--session", "--dest", "org.a11y.Bus", "--object-path", "/org/a11y/bus",
                          "--method", "org.freedesktop.DBus.Properties.Set", "org.a11y.Status", "IsEnabled",
                          "<true>"], capture_output=True, check=False)
    return out.returncode == 0


def wechat_app():
    desk = Atspi.get_desktop(0)
    for i in range(desk.get_child_count()):
        app = desk.get_child_at_index(i)
        if app and app.get_name() == "wechat":
            return app
    return None


def main_frame():
    app = wechat_app()
    return app.get_child_at_index(0) if app and app.get_child_count() else None


def ext(node):
    e = node.get_extents(Atspi.CoordType.SCREEN)
    return e.x, e.y, e.width, e.height


def walk(node, depth=0):
    """深度优先；宽或高为 0 的（隐藏的）子树整个跳过，省掉一大半跟微信的来回"""
    if node is None or depth > 45:
        return
    if depth > 0:
        try:
            e = node.get_extents(Atspi.CoordType.SCREEN)
            if e.width <= 0 or e.height <= 0:
                return
        except Exception:
            return
    yield node
    try:
        n = node.get_child_count()
    except Exception:
        return
    for i in range(n):
        yield from walk(node.get_child_at_index(i), depth + 1)


def role(node) -> str:
    try:
        return node.get_role_name()
    except Exception:
        return ""


def first_line(node) -> str:
    try:
        return (node.get_name() or "").split("\n", 1)[0].strip()
    except Exception:
        return ""


_SCALE = None


def scale() -> float:
    """AT-SPI 给的是逻辑坐标，xdotool 用物理像素：比例 = X 窗口宽 / 无障碍主窗口宽"""
    global _SCALE
    if _SCALE is None:
        _SCALE = 1.0
        frame = main_frame()
        if frame is not None:
            w = ext(frame)[2]
            for wid in sh("xdotool", "search", "--onlyvisible", "--name", "^微信$").split():
                geo = dict(l.split("=", 1) for l in sh("xdotool", "getwindowgeometry", "--shell", wid).splitlines())
                if w and int(geo.get("WIDTH", 0)) > w:
                    _SCALE = int(geo["WIDTH"]) / w
    return _SCALE


def click(node) -> None:
    x, y, w, h = ext(node)
    s = scale()
    sh("xdotool", "mousemove", str(int((x + w / 2) * s)), str(int((y + h / 2) * s)), "click", "1")


def current_chat() -> str:
    """底部输入框（大文本框）的无障碍名字就是当前会话名"""
    frame = main_frame()
    for node in walk(frame):
        if role(node) == "text":
            _x, _y, w, h = ext(node)
            if w > 200 and h > 40:
                return node.get_name() or ""
    return ""


def wait_chat(target: str, seconds: float) -> bool:
    deadline = time.time() + seconds
    while True:
        if current_chat() == target:
            return True
        if time.time() >= deadline:
            return False
        time.sleep(0.3)


def nav_button(name: str):
    for node in walk(main_frame()):
        if role(node) == "push button" and node.get_name() == name:
            return node
    return None


def from_session_list(target: str) -> bool:
    btn = nav_button("微信")
    if btn is not None:
        click(btn)
        time.sleep(0.6)
    hits = [n for n in walk(main_frame()) if role(n) == "list item" and first_line(n) == target]
    if len(hits) != 1:
        return False
    click(hits[0])
    return True


def search_box():
    for node in walk(main_frame()):
        if role(node) == "text":
            _x, _y, w, h = ext(node)
            if h < 40 and w < 300:
                return node
    return None


def from_search(target: str, sections: set[str]) -> tuple[bool, str]:
    box = search_box()
    if box is None:
        return False, "没找到搜索框"
    click(box)
    time.sleep(0.5)
    set_clipboard(target.encode())
    sh("xdotool", "key", "--clearmodifiers", "ctrl+a", "ctrl+v")
    app = wechat_app()
    hits, seen = [], []
    for _ in range(12):  # 结果出来要一两秒，最多等 6 秒
        time.sleep(0.5)
        hits, seen, section = [], [], ""
        for j in range(1, app.get_child_count()):
            for node in walk(app.get_child_at_index(j)):
                if role(node) != "list item":
                    continue
                name = first_line(node)
                if name in SECTION_TITLES:
                    section = name
                    seen.append(name)
                elif name == target and section in sections:
                    hits.append(node)
        if seen:
            break
    if len(hits) != 1:
        sh("xdotool", "key", "Escape")
        return False, f"搜索结果分组 {seen}，可点的精确匹配 {len(hits)} 个"
    click(hits[0])
    return True, ""


def open_chat(target: str, sections: set[str], use_list: bool) -> tuple[bool, str]:
    if use_list and current_chat() == target:
        return True, "already"
    # 会话列表里点「已经选中」的那项不会重新打开聊天（从别的页切回来时常见），1.5 秒内没打开就改走搜索。
    # 不靠先点别的会话来重置，免得把别人的消息标成已读。
    if use_list and from_session_list(target) and wait_chat(target, 1.5):
        return True, "list"
    ok, why = from_search(target, sections)
    if ok and wait_chat(target, 3.0):
        return True, "search"
    return False, why or "打开后输入框名字对不上"


def handle_send(req: dict) -> dict:
    target = req["name"]
    text = req.get("text", "")
    if not target or not text:
        return {"ok": False, "reason": "名字或内容为空"}
    if main_frame() is None:
        return {"ok": False, "reason": "无障碍树里没有微信主窗口"}
    wid = sh("xdotool", "search", "--onlyvisible", "--name", "^微信$").split()
    if wid:
        sh("xdotool", "windowactivate", "--sync", wid[0])
    ok, path = open_chat(target, set(req.get("sections") or ["联系人"]), bool(req.get("use_list")))
    if not ok:
        return {"ok": False, "reason": path}
    time.sleep(max(0.0, float(req.get("delay", 0))))  # 像人在打字
    set_clipboard(text.encode())
    if current_chat() != target:  # 等待期间有弹窗或有人动了界面
        return {"ok": False, "reason": "粘贴前核对失败"}
    sh("xdotool", "key", "--clearmodifiers", "ctrl+v")
    time.sleep(0.3)
    if current_chat() != target:
        sh("xdotool", "key", "--clearmodifiers", "ctrl+a", "Delete")
        return {"ok": False, "reason": "回车前核对失败，已清空输入框"}
    sh("xdotool", "key", "--clearmodifiers", "Return")
    log(f"已发送：名字 {len(target)} 字，内容 {len(text)} 字，路径 {path}")
    return {"ok": True, "path": path}


def main() -> None:
    if not enable_a11y():
        log("打开无障碍开关失败（会话总线地址不对？）")
    time.sleep(1.0)
    for line in sys.stdin:
        try:
            req = json.loads(line)
            op = req.get("op")
            if op == "ping":
                resp = {"ok": main_frame() is not None}
            elif op == "send":
                resp = handle_send(req)
            else:
                resp = {"ok": False, "reason": f"未知操作 {op}"}
        except Exception as e:  # 界面驱动出错不能把整个连接器拖垮
            resp = {"ok": False, "reason": f"{type(e).__name__}: {e}"}
        print(json.dumps(resp, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
