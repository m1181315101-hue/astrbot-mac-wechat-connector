#!/usr/bin/env python3
"""微信界面驱动：按名字切换会话、核对后粘贴发送（无障碍接口 AT-SPI + xdotool）

Linux 微信是 Qt 写的，带无障碍桥：打开 org.a11y.Status.IsEnabled 以后，整棵界面树都能读到。
这个进程由 bridge.py 启动，常驻；用系统 Python 跑（要有 python3-gi 和 gir1.2-atspi-2.0）。
标准输入每行一个 JSON 请求，标准输出每行一个 JSON 结果：

  {"op": "ping"}
  {"op": "status"}  → {"ok": true, "frame": true, "width": 880, "height": 640, "login": false}（只读，不动窗口）
  {"op": "login"}   → {"ok": true, "login": true, "state": "qr", "text": "用小号扫码登录", "others": 0}（只读）
  {"op": "login", "grab": true} → 再加 "png": 登录窗口截图（base64）；不是登录窗口就 {"ok": false, "reason": ...}
  {"op": "send", "name": "备注名", "text": "...", "sections": ["联系人"], "use_list": true, "delay": 1.2}
  {"op": "send", "name": "备注名", "file": "/tmp/xx/语音.mp3", "sections": ["群聊"], "use_list": true}
  {"op": "send", "name": "备注名", "voice": "/tmp/xx/voice.wav", "sections": ["联系人"], "use_list": true}
  → {"ok": true, "path": "list|search|already"} / {"ok": false, "reason": "...", "recording": false}

发文件是用 text/uri-list 粘贴：GIF 会变成动画表情，PNG/JPG 变成图片，其他（比如 mp3）变成文件。
发语音条用微信自带的「发语音」：点它开始录音 → 把音频播进虚拟麦克风 → 点「发送语音」。
虚拟麦克风 = PulseAudio 的 null sink（wxvmic_sink）+ 把它的监听源包装成普通输入设备（wxvmic）；
微信不把「监听源」当麦克风，而且只在启动时找麦克风，所以要在微信启动前建好、设成默认输入，
微信的环境里还要有 PULSE_SERVER（只有 PULSE_RUNTIME_PATH 时它连不上声音服务）。

安全规则：
- 会话列表只在 use_list=true（调用方确认这个名字在通讯录里独一无二）时用，按第一行精确匹配；
- 搜索结果只认 sections 指定分组里名字完全相同的那一项，「搜索网络结果」「聊天记录」永远不点；
- 粘贴前、回车前都核对底部输入框的无障碍名字 == 目标名字，对不上就放弃，一个字都不发。
- 截图只截登录窗口：按里面的控件认出来是登录窗口、旁边没开别的微信窗口，截前截后各认一次，主窗口永远不截。
不往标准输出写任何聊天内容；日志只写到标准错误，而且只有名字长度、坐标和结果。
"""
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse

import gi

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi  # noqa: E402

SECTION_TITLES = {"联系人", "群聊", "功能", "最常使用", "最近使用", "搜索网络结果", "聊天记录",
                  "公众号", "小程序", "搜一搜", "视频号", "服务", "文章", "企业微信联系人"}


def log(msg: str) -> None:
    print(f"[wxui] {msg}", file=sys.stderr, flush=True)


def sh(*args: str) -> str:
    return subprocess.run(args, capture_output=True, check=False).stdout.decode().strip()


def set_clipboard(data: bytes, mime: str | None = None) -> None:
    # xclip 会留在后台占着剪贴板，输出不能接管道，否则会一直等它退出
    cmd = ["xclip", "-selection", "clipboard"] + (["-t", mime] if mime else [])
    subprocess.run(cmd, input=data, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


VMIC_SINK, VMIC_SOURCE = "wxvmic_sink", "wxvmic"


def pactl_json(*args: str):
    out = subprocess.run(["pactl", "-f", "json", *args], capture_output=True, text=True, check=False).stdout
    try:
        return json.loads(out) if out.strip() else []
    except ValueError:
        return []


def ensure_virtual_mic() -> bool:
    """没有就建虚拟麦克风并设成默认输入；原来跟着默认输入走的其他录音（比如网页桌面的推流）挪回原设备"""
    sources = [x.get("name") for x in pactl_json("list", "short", "sources")]
    if VMIC_SOURCE not in sources:
        subprocess.run(["pactl", "load-module", "module-null-sink", f"sink_name={VMIC_SINK}",
                        "sink_properties=device.description=WxVirtualMicSink"], capture_output=True, check=False)
        subprocess.run(["pactl", "load-module", "module-remap-source", f"master={VMIC_SINK}.monitor",
                        f"source_name={VMIC_SOURCE}", "source_properties=device.description=WxVirtualMic"],
                       capture_output=True, check=False)
    info = pactl_json("info")
    previous = info.get("default_source_name", "") if isinstance(info, dict) else ""
    if previous != VMIC_SOURCE:
        subprocess.run(["pactl", "set-default-source", VMIC_SOURCE], capture_output=True, check=False)
    # 网页桌面的推流（pcmflux）该录桌面声音 output.monitor；跟着默认输入跑到虚拟麦克风上的挪回去
    names = {x.get("index"): x.get("name") for x in pactl_json("list", "short", "sources")}
    home = "output.monitor" if "output.monitor" in names.values() else (previous if previous != VMIC_SOURCE else "")
    for out in pactl_json("list", "source-outputs"):
        binary = (out.get("properties") or {}).get("application.process.binary", "")
        if names.get(out.get("source")) == VMIC_SOURCE and binary != "wechat" and home:
            subprocess.run(["pactl", "move-source-output", str(out.get("index")), home],
                           capture_output=True, check=False)
    return VMIC_SOURCE in [x.get("name") for x in pactl_json("list", "short", "sources")]


def wechat_recording() -> bool:
    return any((x.get("properties") or {}).get("application.process.binary") == "wechat"
               for x in pactl_json("list", "source-outputs"))


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


def scale() -> float:
    """AT-SPI 给的是逻辑坐标，xdotool 用物理像素：比例 = X 窗口宽 / 无障碍主窗口宽。
    每次现算：窗口隐藏时无障碍宽度是 0，缓存下来会一直错"""
    frame = main_frame()
    w = ext(frame)[2] if frame is not None else 0
    wid = big_window()
    if w and wid:
        geo = dict(l.split("=", 1) for l in sh("xdotool", "getwindowgeometry", "--shell", wid).splitlines())
        if int(geo.get("WIDTH", 0)) > w:
            return int(geo["WIDTH"]) / w
    return 1.0


def click(node) -> None:
    x, y, w, h = ext(node)
    s = scale()
    sh("xdotool", "mousemove", str(int((x + w / 2) * s)), str(int((y + h / 2) * s)), "click", "1")


# 微信连上麦克风以后，输入框名字后面会多一句提示：「会话名 按住 Ctrl + Super 使用语音输入文字」
VOICE_INPUT_HINT = re.compile(r"\s+按住\s*Ctrl\s*\+\s*Super\s*使用语音输入文字$")


def current_chat() -> str:
    """底部输入框（大文本框）的无障碍名字就是当前会话名（去掉语音输入提示）"""
    frame = main_frame()
    for node in walk(frame):
        if role(node) == "text":
            _x, _y, w, h = ext(node)
            if w > 200 and h > 40:
                return VOICE_INPUT_HINT.sub("", node.get_name() or "")
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


def button(name: str, near=None):
    """按无障碍名字找按钮；near 给了就只要和它同一行（y 相差 12 以内）的"""
    for node in walk(main_frame()):
        if role(node) == "push button" and node.get_name() == name:
            if near is None or abs(ext(node)[1] - ext(near)[1]) <= 12:
                return node
    return None


def cancel_recording() -> None:
    send = button("发送语音")
    cancel = button("取消", near=send) if send is not None else None
    if cancel is not None:
        click(cancel)
    else:
        sh("xdotool", "key", "--clearmodifiers", "Escape")
    time.sleep(0.5)


def send_voice(target: str, path: str, seconds: float, how: str) -> dict:
    start = button("发语音")
    if start is None:
        return {"ok": False, "reason": "没找到「发语音」按钮", "recording": False}
    click(start)
    deadline = time.time() + 3.0
    while time.time() < deadline and not (wechat_recording() and button("发送语音") is not None):
        time.sleep(0.2)
    if not wechat_recording() or button("发送语音") is None:
        sh("xdotool", "key", "--clearmodifiers", "Escape")
        return {"ok": False, "reason": "微信没有开始录音（检查虚拟麦克风和微信的 PULSE_SERVER）", "recording": False}
    if current_chat() != target:
        cancel_recording()
        return {"ok": False, "reason": "开始录音后会话对不上，已取消", "recording": True}
    played = subprocess.run(["paplay", f"--device={VMIC_SINK}", path], capture_output=True,
                            timeout=seconds + 15, check=False)
    time.sleep(0.4)
    send = button("发送语音")
    if played.returncode != 0 or send is None or current_chat() != target:
        cancel_recording()
        return {"ok": False, "reason": "播放失败或发送前核对失败，已取消录音", "recording": True}
    click(send)
    time.sleep(0.8)
    log(f"已发送：名字 {len(target)} 字，语音条 {seconds:.0f} 秒，路径 {how}")
    return {"ok": True, "path": how}


def frame_visible() -> bool:
    frame = main_frame()
    return frame is not None and ext(frame)[2] > 0


def big_window() -> str:
    for wid in sh("xdotool", "search", "--onlyvisible", "--name", "^微信$").split():
        geo = dict(l.split("=", 1) for l in sh("xdotool", "getwindowgeometry", "--shell", wid).splitlines())
        if int(geo.get("WIDTH", 0)) > 400:  # 托盘图标和小浮窗也叫这个名字，只要大窗口
            return wid
    return ""


def show_main_window() -> bool:
    """主窗口被关到托盘（点了右上角 ×）就点托盘图标让微信自己打开（用 xdotool 强行映射的话，
    微信仍认为窗口是隐藏的，无障碍树里大小一直是 0）；窗口太窄时微信会切成单栏、藏起会话列表和搜索框，
    调到 1760×1280。返回主窗口最后是否可用"""
    if not frame_visible():
        log("主窗口被收进了托盘，点托盘图标打开")
        for tray in sh("xdotool", "search", "--name", "^stalonetray$").split():
            geo = dict(l.split("=", 1) for l in sh("xdotool", "getwindowgeometry", "--shell", tray).splitlines())
            sh("xdotool", "mousemove", str(int(geo["X"]) + 12), str(int(geo["Y"]) + 12), "click", "1")
            break
        deadline = time.time() + 5
        while time.time() < deadline and not frame_visible():
            time.sleep(0.3)
    wid = big_window()
    if wid:
        geo = dict(l.split("=", 1) for l in sh("xdotool", "getwindowgeometry", "--shell", wid).splitlines())
        if int(geo.get("WIDTH", 0)) < 1600 or int(geo.get("HEIGHT", 0)) < 1100:
            sh("xdotool", "windowsize", wid, "1760", "1280")
            time.sleep(1.0)
        sh("xdotool", "windowactivate", "--sync", wid)
    return frame_visible()


def handle_send(req: dict) -> dict:
    target = req["name"]
    text, path, voice = req.get("text", ""), req.get("file", ""), req.get("voice", "")
    if not target or not (text or path or voice):
        return {"ok": False, "reason": "名字或内容为空"}
    for f in (path, voice):
        if f and not os.path.isfile(f):
            return {"ok": False, "reason": "要发的文件不存在"}
    if not show_main_window() or main_frame() is None:
        return {"ok": False, "reason": "找不到微信主窗口（没登录，或者被关掉了）"}
    ok, how = open_chat(target, set(req.get("sections") or ["联系人"]), bool(req.get("use_list")))
    if not ok:
        return {"ok": False, "reason": how}
    if voice:
        return send_voice(target, voice, float(req.get("seconds") or 0), how)
    time.sleep(max(0.0, float(req.get("delay", 0))))  # 像人在打字
    if path:
        set_clipboard(f"file://{urllib.parse.quote(path)}\r\n".encode(), "text/uri-list")
    else:
        set_clipboard(text.encode())
    if current_chat() != target:  # 等待期间有弹窗或有人动了界面
        return {"ok": False, "reason": "粘贴前核对失败"}
    sh("xdotool", "key", "--clearmodifiers", "ctrl+v")
    time.sleep(1.0 if path else 0.3)  # 文件要等微信把附件放进输入框
    if current_chat() != target:
        sh("xdotool", "key", "--clearmodifiers", "ctrl+a", "Delete")
        return {"ok": False, "reason": "回车前核对失败，已清空输入框"}
    sh("xdotool", "key", "--clearmodifiers", "Return")
    log(f"已发送：名字 {len(target)} 字，{'文件' if path else f'文字 {len(text)} 字'}，路径 {how}")
    return {"ok": True, "path": how}


# 登录窗口才有的控件：扫码、确认、断网这些状态的控件一直都在树里，只是没显示
LOGIN_MARKS = {"二维码", "扫码登录", "进入微信", "切换账号", "仅传输文件", "网络代理设置"}
# 显示着哪个控件 → 登录窗口现在是什么状态（按顺序认第一个）
LOGIN_STATES = (("网络连接已断开", "offline", "网络连接已断开"), ("无法加载", "offline", "二维码加载不出来"),
                ("需在手机上完成登录", "confirm", "需在手机上完成登录"),
                ("进入微信", "confirm", "要点「进入微信」，再在手机上确认（去网页桌面点）"),
                ("正在进入", "loading", "正在进入"), ("正在加载", "loading", "正在加载"),
                ("二维码", "qr", "用小号扫码登录"))


def tree_names(node, limit: int = 800) -> tuple[set[str], set[str]]:
    """一个窗口里控件的名字（包括没显示的），以及其中显示着（宽高不为 0）的那些；最多看 limit 个控件"""
    names, shown, stack = set(), set(), [(node, 0)]
    while stack and limit > 0:
        n, depth = stack.pop()
        limit -= 1
        try:
            name = n.get_name() or ""
            e = n.get_extents(Atspi.CoordType.SCREEN)
            count = n.get_child_count()
        except Exception:
            continue
        if name:
            names.add(name)
            if e.width > 0 and e.height > 0:
                shown.add(name)
        if depth < 30:
            stack.extend((n.get_child_at_index(i), depth + 1) for i in range(count))
    return names, shown


def login_window():
    """找登录窗口（只读）：显示着、不大于 600×800（无障碍坐标）、里面至少有 4 个登录窗口才有的控件。
    返回 (登录窗口或 None, 它里面显示着的控件名字, 别的显示着的微信窗口个数)"""
    app = wechat_app()
    login, shown, others = None, set(), 0
    for i in range(app.get_child_count() if app else 0):
        top = app.get_child_at_index(i)
        try:
            _x, _y, w, h = ext(top)
        except Exception:
            continue
        if w <= 0 or h <= 0:  # 收在托盘里、没显示的窗口
            continue
        if login is None and w <= 600 and h <= 800:
            names, seen = tree_names(top)
            if len(names & LOGIN_MARKS) >= 4:
                login, shown = top, seen
                continue
        others += 1
    return login, shown, others


def login_state(shown: set[str]) -> tuple[str, str]:
    for name, state, text in LOGIN_STATES:
        if name in shown:
            return state, text
    return "unknown", "停在登录窗口（看截图里的提示）"


def login_info() -> dict:
    login, shown, others = login_window()
    if login is None:
        return {"ok": True, "login": False}
    state, text = login_state(shown)
    return {"ok": True, "login": True, "state": state, "text": text, "others": others}


def png_size(data: bytes) -> tuple[int, int]:
    if data[:8] != b"\x89PNG\r\n\x1a\n" or data[12:16] != b"IHDR":
        return 0, 0
    return int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")


def login_x_window(frame) -> tuple[str, int, int]:
    """登录窗口对应的 X 窗口：微信进程的窗口里，位置、大小和无障碍坐标成同一个比例（缩放）的那个。返回 (窗口号, 宽, 高)"""
    x, y, w, h = ext(frame)
    for wid in sh("xdotool", "search", "--onlyvisible", "--pid", str(wechat_app().get_process_id())).split():
        geo = dict(l.split("=", 1) for l in sh("xdotool", "getwindowgeometry", "--shell", wid).splitlines())
        gx, gy, gw, gh = (int(geo.get(k, 0)) for k in ("X", "Y", "WIDTH", "HEIGHT"))
        s = gw / w
        if 0.9 <= s <= 4 and all(abs(a - b * s) <= 2 + s for a, b in ((gh, h), (gx, x), (gy, y))):
            return wid, gw, gh
    return "", 0, 0


def grab_login() -> dict:
    """截登录窗口：截前截后都要认得出是登录窗口、位置没变、旁边没开别的微信窗口，图的大小和窗口对得上，否则不给图"""
    login, shown, others = login_window()
    if login is None:
        return {"ok": False, "reason": "现在不是登录窗口，不截图"}
    if others:
        return {"ok": False, "reason": "还开着别的微信窗口，不截图（去网页桌面看）"}
    before = ext(login)
    wid, gw, gh = login_x_window(login)
    if not wid:
        return {"ok": False, "reason": "没找到登录窗口对应的 X 窗口"}
    png = subprocess.run(["ffmpeg", "-v", "error", "-f", "x11grab", "-draw_mouse", "0", "-window_id", wid,
                          "-i", os.environ.get("DISPLAY", ":1"), "-frames:v", "1", "-f", "image2pipe", "-c:v", "png",
                          "-"], capture_output=True, timeout=15, check=False).stdout
    after, shown, others = login_window()
    if after is None or others or ext(after) != before or png_size(png) != (gw, gh):
        return {"ok": False, "reason": "截图时窗口变了，这张不要"}
    state, text = login_state(shown)
    return {"ok": True, "login": True, "state": state, "text": text, "png": base64.b64encode(png).decode()}


def main() -> None:
    if not enable_a11y():
        log("打开无障碍开关失败（会话总线地址不对？）")
    if os.environ.get("PULSE_SERVER") and not ensure_virtual_mic():
        log("虚拟麦克风没建起来，语音条会退回成音频文件")
    time.sleep(1.0)
    for line in sys.stdin:
        try:
            req = json.loads(line)
            op = req.get("op")
            if op == "ping":
                resp = {"ok": show_main_window() and main_frame() is not None}
            elif op == "status":  # 状态页用：只看不动（ping 会把窗口从托盘里拉出来、调大小）
                frame = main_frame()
                w, h = ext(frame)[2:] if frame is not None else (0, 0)
                resp = {"ok": True, "frame": frame is not None, "width": w, "height": h, "login": False}
                if 0 < w <= 600:  # 小得像登录窗口：认一下
                    info = login_info()
                    resp.update(login=info["login"], login_text=info.get("text", ""))
            elif op == "login":  # 状态页的登录页用：只读；grab 时截登录窗口
                resp = grab_login() if req.get("grab") else login_info()
            elif op == "send":
                resp = handle_send(req)
            else:
                resp = {"ok": False, "reason": f"未知操作 {op}"}
        except Exception as e:  # 界面驱动出错不能把整个连接器拖垮
            resp = {"ok": False, "reason": f"{type(e).__name__}: {e}"}
        print(json.dumps(resp, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
