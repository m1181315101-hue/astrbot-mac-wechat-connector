"""看门狗：微信界面不对时的检测、自动唤醒和状态页告警

管三种情况（每 30 秒看一次进程；界面正常时每 3 分钟看一次，有问题时每分钟一次，正在发消息就下一轮再看）：
- 卡住（标红），会自动唤醒：连续 3 次发送因为界面失败（没找到搜索框 / 找不到微信主窗口 / 界面驱动无响应），
  或者连续两次看到主窗口开着、却找不到搜索框；
- 疑似卡住（标黄），会自动唤醒：这次启动后见过 ILink 服务（WeChatAppEx 里命令行带
  --utility-sub-type=flue.mojom.ILinkServiceHost 的进程），它没了超过 5 分钟；
- 无障碍总线不可用，或者微信没接上无障碍总线（标红），不唤醒：连接器是靠无障碍总线读微信界面的，
  这条总线没起来、或者微信是在它坏着的时候启动的，鼠标怎么晃都没用，直接在状态页写清楚要重启容器。
  11-10 见过一次：容器重启后总线没起来，一整天发不出消息，当时的看门狗白白唤醒了 25 次。
停在登录窗口、微信没在运行都不算卡住（概览里别的检查项会标出来），也不唤醒。
凌晨见过多次的「收不到消息」其实是微信掉线退回了登录窗口（辅助进程和 ILink 服务同一刻没了），属于这一类，
要有人重新登录；看门狗把那一刻记进日志。手动结束 WeChatAppEx 主进程不会卡：微信一秒内把它拉起来（11-02 试过）。
自愈：卡住或疑似时马上唤醒一次，没好就 2、5、15、30 分钟后再试，之后每 30 分钟一次，1 级、2 级交替：
1 级重置屏保、鼠标在微信主窗口上晃一下再放回去（不点）；2 级再叫出主窗口、点一下左边的「微信」按钮。
启动时关掉 X 屏保（watchdog_screensaver_off）。
日志和状态页只有时间、次数和有没有，没有消息内容和名字。
"""
import asyncio
import logging
import os
import time

log = logging.getLogger("wechat-connector")

PROC_EVERY = 30         # 每 30 秒看一次进程
UI_EVERY = 180          # 正常时每 3 分钟看一次界面
UI_EVERY_STALLED = 60   # 卡住或疑似时每分钟看一次
ILINK_GRACE = 300       # ILink 服务没了这么久才算疑似卡住
SEND_FAILS = 3          # 连续这么多次发送因为界面失败就算卡住
UI_BAD = 2              # 连续这么多次看到主窗口开着却找不到搜索框就算卡住
NUDGE_GAPS = (120, 180, 600, 900, 1800)  # 唤醒：马上一次，之后隔 2、3、10、15 分钟（即 2、5、15、30 分钟时），再往后每 30 分钟
A11Y_DOWN = "无障碍总线不可用"  # 界面驱动问不到无障碍总线的地址时回这个（wxui.py 里同名）
UI_REASONS = ("没找到搜索框", "找不到微信主窗口", "界面驱动无响应", A11Y_DOWN)  # 这些发送失败说明界面不对（不是名字对不上那类）
STALLED = ("suspect", "stuck")
NO_NUDGE = {"a11y", "noapp"}  # 这两种原因唤醒没用：要重启容器（或者修好总线后重启微信）
HELPERS = ("wxocr", "wxutility", "wxplayer")  # 微信主进程带起来的辅助进程；11-01 凌晨卡住后和 ILink 服务同一秒起来


def ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} 秒"
    if seconds < 3600:
        return f"{seconds // 60} 分钟"
    if seconds < 86400:
        return f"{seconds // 3600} 小时 {seconds % 3600 // 60} 分钟"
    return f"{seconds // 86400} 天 {seconds % 86400 // 3600} 小时"


def clock(ts: float) -> str:
    """几点几分；不是今天的带上日期"""
    t = time.localtime(ts)
    return time.strftime("%H:%M" if t[:3] == time.localtime()[:3] else "%m-%d %H:%M", t)


def radium_state() -> dict:
    """看 /proc（僵尸进程不算）：微信主进程、WeChatAppEx 主进程（微信主进程的子进程，命令行里没有 --type=）、
    ILink 服务进程各自几点起来的，没在跑是 0；微信主进程下的辅助进程有几个"""
    try:
        with open("/proc/stat") as f:
            boot = next(int(line.split()[1]) for line in f if line.startswith("btime "))
    except (OSError, StopIteration, ValueError):
        return {}
    hz = os.sysconf("SC_CLK_TCK")
    procs = []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open(f"/proc/{d}/stat", "rb") as f:
                raw = f.read().decode(errors="replace")
            comm, rest = raw[raw.index("(") + 1:raw.rindex(")")], raw[raw.rindex(")") + 2:].split()
            if comm not in ("wechat", "WeChatAppEx") + HELPERS or rest[0] == "Z":
                continue
            with open(f"/proc/{d}/cmdline", "rb") as f:
                cmd = f.read().replace(b"\0", b" ").decode(errors="replace")
        except (OSError, ValueError, IndexError):
            continue
        procs.append((comm, int(rest[1]), boot + int(rest[19]) / hz, cmd, int(d)))
    mains = {p[4] for p in procs if p[0] == "wechat"}
    return {"main": min((p[2] for p in procs if p[0] == "wechat"), default=0.0),
            "browser": max((p[2] for p in procs if p[0] == "WeChatAppEx" and p[1] in mains and "--type=" not in p[3]),
                           default=0.0),
            "ilink": max((p[2] for p in procs if p[0] == "WeChatAppEx" and "ILinkServiceHost" in p[3]), default=0.0),
            "helpers": sum(1 for p in procs if p[0] in HELPERS and p[1] in mains)}


class Watchdog:
    def __init__(self, c):
        self.c = c
        self.enabled = bool(c.cfg.get("watchdog", True))
        self.state = "ok"          # ok / suspect（疑似）/ stuck（卡住）/ login（停在登录窗口）/ off（微信没在运行）
        self.keys: list[str] = []  # 为什么：fails（发送失败）/ ui（界面）/ ilink
        self.since = 0.0           # 这次卡住最早的迹象在几点
        self.proc: dict = {}
        self.last_browser = 0.0    # 上次见到的 WeChatAppEx 主进程几点起来的（中间有一轮没在跑也认得出重启）
        self.ilink_seen = False    # 这次启动后见过 ILink 服务
        self.ilink_gone = 0.0      # ILink 服务从几点起没在跑
        self.ui: dict = {}         # 最近一次看界面的结果（只有有没有和个数）
        self.ui_at = 0.0
        self.ui_bad = 0            # 连续几次看到主窗口开着却找不到搜索框
        self.ui_bad_since = 0.0
        self.a11y_bad = 0          # 连续几次界面驱动说无障碍总线不可用
        self.a11y_since = 0.0
        self.noapp_bad = 0         # 连续几次无障碍树里没有微信（微信明明在运行）
        self.noapp_since = 0.0
        self.fails = 0             # 连续几次发送因为界面失败
        self.fail_since = 0.0
        self.fail_reason = ""
        self.last_inbound = 0.0    # 最近一次白名单里的会话有新消息（收不收都算）
        self.nudges = 0            # 这次卡住以后唤醒了几次
        self.last_nudge = 0.0
        self.next_nudge = 0.0
        self.saver_tried = 0.0
        self.diag_note = ""        # 下次看完界面后记一行现场（WeChatAppEx 重启后）
        self.stalls = 0            # 这次启动后卡住（含疑似）过几次
        self.recovered: dict = {}  # 上一次卡住：几点开始、卡了多久、期间唤醒了几次

    # ---------- 发送和轮询报进来的 ----------

    def on_send(self, ok: bool, reason: str) -> None:
        if ok:
            self.fails, self.fail_since, self.fail_reason = 0, 0.0, ""
        elif str(reason).startswith(UI_REASONS):
            if not self.fails:
                self.fail_since = time.time()
            self.fails += 1
            self.fail_reason = str(reason).split("（", 1)[0][:30]

    def on_inbound(self) -> None:
        self.last_inbound = time.time()

    # ---------- 循环 ----------

    async def run(self) -> None:
        await asyncio.sleep(5)
        await self.saver_off()
        while True:
            try:
                await self.tick()
            except Exception as e:  # 看门狗出错不能把连接器拖垮
                log.warning("看门狗出错：%s", type(e).__name__)
            await asyncio.sleep(PROC_EVERY)

    async def saver_off(self) -> None:
        """关掉 X 屏保；之后看到它又开了（比如 X 重启过）就再关一次，最多一小时一次"""
        if not self.c.cfg.get("watchdog_screensaver_off", True) or time.time() - self.saver_tried < 3600:
            return
        self.saver_tried = time.time()
        resp = await asyncio.to_thread(self.c.ui.request, {"op": "saver_off"}, 20.0, 10.0)
        if resp.get("ok"):
            log.info("看门狗：关掉了 X 屏保（原来的状态：%s，现在：%s）", resp.get("before") or "读不到", resp.get("after") or "读不到")
        elif not resp.get("busy"):
            log.info("看门狗：X 屏保没关掉（%s）", resp.get("reason", ""))

    async def tick(self) -> None:
        self.note_proc(await asyncio.to_thread(radium_state), time.time())
        soon = self.state in STALLED or self.a11y_bad or self.noapp_bad or self.fail_reason == A11Y_DOWN
        if time.time() - self.ui_at >= (UI_EVERY_STALLED if soon else UI_EVERY):
            await self.look()
        self.judge(time.time())
        if self.state in STALLED and not NO_NUDGE & set(self.keys) and time.time() >= self.next_nudge:
            await self.nudge()

    def note_proc(self, proc: dict, now: float) -> None:
        """记下 WeChatAppEx 重启、ILink 服务和辅助进程没了、起来的时间（第一次看只记不比）"""
        old = self.proc
        il, old_il = proc.get("ilink", 0.0), old.get("ilink", 0.0)
        br, hp, old_hp = proc.get("browser", 0.0), proc.get("helpers", 0), old.get("helpers", 0)
        if old:
            if old.get("browser") and not br:
                log.warning("看门狗：WeChatAppEx 没在跑了")
            if br and self.last_browser and abs(br - self.last_browser) > 1:
                log.info("看门狗：WeChatAppEx 重启了（%s 起来的）", clock(br))
                self.ui_at, self.diag_note = 0.0, "WeChatAppEx 重启后"  # 马上看一眼界面，记下现场
            if old_hp and not hp:
                log.warning("看门狗：微信的辅助进程（%s）都没了", "、".join(HELPERS))
            elif hp and not old_hp:
                log.info("看门狗：微信的辅助进程起来了（%d 个）", hp)
            if old_il and not il:
                self.ilink_gone = now
                log.warning("看门狗：WeChatAppEx 里的 ILink 服务没了")
            elif il and not old_il:
                gone = f"，之前 {ago(now - self.ilink_gone)}没在跑" if self.ilink_gone else ""
                log.info("看门狗：ILink 服务起来了（%s）%s", clock(il), gone)
                self.ilink_gone = 0.0
            elif il and abs(il - old_il) > 1:
                log.info("看门狗：ILink 服务重启了（%s）", clock(il))
        elif not il and proc.get("main"):
            log.info("看门狗：启动时 ILink 服务没在跑（见到它之前不按它判断）")
        self.ilink_seen = self.ilink_seen or bool(il)
        self.last_browser = br or self.last_browser
        self.proc = proc

    async def look(self) -> None:
        """看一眼界面（只读）；正在发消息就下一轮再看"""
        c = self.c
        if c.sending or c.send_queue.qsize():
            return
        resp = await asyncio.to_thread(c.ui.request, {"op": "health"}, 20.0, 2.0)
        if resp.get("busy"):
            return
        now = self.ui_at = time.time()
        if not resp.get("ok"):
            self.ui = {"ok": False, "reason": str(resp.get("reason", ""))[:60]}
            if resp.get("a11y") is False:  # 界面驱动问过了：无障碍总线不可用
                if not self.a11y_bad:
                    self.a11y_since = now
                self.a11y_bad += 1
            return
        self.a11y_bad, self.a11y_since = 0, 0.0
        if self.fail_reason == A11Y_DOWN:  # 总线回来了：之前因为它没发出去的不再算数
            self.fails, self.fail_since, self.fail_reason = 0, 0.0, ""
        self.ui = resp
        if resp.get("app") is False and self.proc.get("main"):  # 总线在，但无障碍树里没有微信
            if not self.noapp_bad:
                self.noapp_since = now
            self.noapp_bad += 1
            return
        self.noapp_bad, self.noapp_since = 0, 0.0
        if self.diag_note:
            log.info("看门狗：%s的现场：%s", self.diag_note, self.diag())
            self.diag_note = ""
        if resp.get("saver") in ("on", "off"):  # 屏保又开着（关屏保后应该是 disabled）
            await self.saver_off()
        if resp.get("width", 0) > 600 and not resp.get("login") and not resp.get("search"):
            if not self.ui_bad:
                self.ui_bad_since = now
            self.ui_bad += 1
            return
        self.ui_bad, self.ui_bad_since = 0, 0.0
        if resp.get("search"):  # 搜索框找得到了：之前因为界面没发出去的不再算数
            self.fails, self.fail_since, self.fail_reason = 0, 0.0, ""

    def judge(self, now: float) -> None:
        if not self.proc.get("main"):
            state, keys = "off", []
        elif self.ui.get("login"):
            state, keys = "login", []
        else:
            hits = (("a11y", self.a11y_bad >= UI_BAD or (self.a11y_bad and self.fail_reason == A11Y_DOWN)),
                    # 紧接着总线故障的「没接上」，看到一次就算，免得中间漏出一段去唤醒
                    ("noapp", self.noapp_bad >= UI_BAD or (self.noapp_bad and NO_NUDGE & set(self.keys))),
                    ("fails", self.fails >= SEND_FAILS), ("ui", self.ui_bad >= UI_BAD),
                    ("ilink", self.ilink_seen and bool(self.ilink_gone) and now - self.ilink_gone >= ILINK_GRACE))
            keys = [k for k, hit in hits if hit]
            state = "stuck" if {"fails", "ui"} & set(keys) or NO_NUDGE & set(keys) else "suspect" if keys else "ok"
        if state not in STALLED:
            if self.state in STALLED and state == "ok":
                self.recovered = {"start": self.since, "end": now, "nudges": self.nudges}
                log.info("看门狗：恢复了（%s 起卡了 %s，期间自动唤醒 %d 次）；%s", clock(self.since), ago(now - self.since),
                         self.nudges, self.diag())
            elif self.state in STALLED:
                log.info("看门狗：%s，先不判断卡没卡", "停在登录窗口" if state == "login" else "微信没在运行")
            self.state, self.keys, self.since, self.nudges, self.next_nudge = state, [], 0.0, 0, 0.0
            return
        start = min(t for t in ((self.fail_since if "fails" in keys else 0), (self.ui_bad_since if "ui" in keys else 0),
                                (self.a11y_since if "a11y" in keys else 0), (self.noapp_since if "noapp" in keys else 0),
                                (self.ilink_gone if "ilink" in keys else 0), now) if t)
        if self.state not in STALLED:
            self.since, self.next_nudge = start, now
            self.stalls += 1
            log.warning("看门狗：%s：%s；%s", "卡住了" if state == "stuck" else "疑似卡住",
                        "；".join(self.reasons(now, keys)), self.diag())
        elif state != self.state or keys != self.keys:
            word = ("卡住了" if state == "stuck" else "疑似卡住") if state != self.state else "还卡着" if state == "stuck" else "还疑似卡住"
            log.warning("看门狗：%s：%s", word, "；".join(self.reasons(now, keys)))
        if NO_NUDGE & set(keys) and not NO_NUDGE & set(self.keys if self.state in STALLED else []):
            log.warning("看门狗：%s", self.advice(keys))
        self.since = min(self.since, start)
        self.state, self.keys = state, keys

    async def nudge(self) -> None:
        if self.c.sending:  # 界面正被发消息用着：下一轮再说
            return
        level = 1 if self.nudges % 2 == 0 else 2
        resp = await asyncio.to_thread(self.c.ui.request, {"op": "nudge", "level": level}, 30.0, 10.0)
        if resp.get("busy"):  # 正在发消息：下一轮再说
            return
        self.nudges += 1
        self.last_nudge = time.time()
        self.next_nudge = self.last_nudge + NUDGE_GAPS[min(self.nudges - 1, len(NUDGE_GAPS) - 1)]
        self.ui_at = 0.0  # 下一轮马上再看一次界面
        done = "、".join(resp.get("done") or []) if resp.get("ok") else f"没做成（{resp.get('reason', '')}）"
        log.info("看门狗：第 %d 次自动唤醒（%d 级）：%s", self.nudges, level, done)

    # ---------- 给日志和状态页看的 ----------

    def reasons(self, now: float, keys: list[str] | None = None) -> list[str]:
        keys = self.keys if keys is None else keys
        out = []
        if "a11y" in keys:
            out.append(f"{A11Y_DOWN}，连接器读不到微信的界面（连续 {self.a11y_bad} 次）")
        if "noapp" in keys:
            out.append(f"微信在运行，但没接上无障碍总线（连续 {self.noapp_bad} 次）")
        if "fails" in keys:
            out.append(f"连续 {self.fails} 次发送因为界面没发出去（{self.fail_reason}）")
        if "ui" in keys:
            out.append(f"主窗口开着却找不到搜索框（连续 {self.ui_bad} 次）")
        if "ilink" in keys:
            out.append(f"WeChatAppEx 里的 ILink 服务 {ago(now - self.ilink_gone)}没在跑")
        return out

    def advice(self, keys: list[str] | None = None) -> str:
        """卡住时给人看的下一步"""
        keys = self.keys if keys is None else keys
        if NO_NUDGE & set(keys):
            return ("这种情况自动唤醒没用，看门狗不会去唤醒。在电脑的终端里运行 docker restart wechat"
                    "（重启容器，不是重建），等微信自己起来、登录回来，手机上可能要点一下确认")
        return "一直不好的话：打开网页桌面，在微信窗口上动一下鼠标，一两分钟后多半就恢复了"

    def diag(self) -> str:
        """一行现场：界面、X、进程、最后一次入站（只有个数、时间和有没有）"""
        u, p, parts = self.ui, self.proc, []
        if u.get("ok"):
            parts.append(f"主窗口 {u.get('width', 0)}×{u.get('height', 0)}" + ("（登录窗口）" if u.get("login") else ""))
            if "search" in u:
                parts.append(f"搜索框{'有' if u['search'] else '没有'}、「微信」按钮{'有' if u.get('nav') else '没有'}、"
                             f"按钮 {u.get('buttons', 0)} 个、列表项 {u.get('items', 0)} 个")
            parts.append(f"微信窗口 {u.get('windows', 0)} 个，X {u.get('idle', -1)} 秒没有输入，屏保 {u.get('saver') or '读不到'}"
                         f"（{ago(time.time() - self.ui_at)}前看的）")
        elif u:
            parts.append(f"界面驱动：{u.get('reason', '')}")
        parts.append("WeChatAppEx " + (f"{clock(p['browser'])} 起来的" if p.get("browser") else "没在跑"))
        parts.append("ILink 服务 " + (f"{clock(p['ilink'])} 起来的" if p.get("ilink") else "没在跑"))
        parts.append(f"辅助进程 {p.get('helpers', 0)} 个")
        parts.append("最后一次入站 " + (clock(self.last_inbound) if self.last_inbound else "启动后还没有"))
        return "，".join(parts)

    def snapshot(self, now: float) -> dict:
        state = self.state if self.enabled else "disabled"
        rec = self.recovered
        return {
            "state": state, "since": self.since, "since_text": clock(self.since) if self.since else "",
            "for": ago(now - self.since) if self.since else "", "reasons": self.reasons(now) if state in STALLED else [],
            "last_inbound": self.last_inbound, "last_inbound_text": clock(self.last_inbound) if self.last_inbound else "",
            "last_inbound_ago": ago(now - self.last_inbound) if self.last_inbound else "",
            "advice": self.advice() if state in STALLED else "", "nudging": state in STALLED and not NO_NUDGE & set(self.keys),
            "fails": self.fails, "fail_reason": self.fail_reason, "nudges": self.nudges,
            "last_nudge_ago": ago(now - self.last_nudge) if self.nudges else "",
            "ilink": bool(self.proc.get("ilink")), "stalls": self.stalls,
            "recovered": {"start_text": clock(rec["start"]), "for": ago(rec["end"] - rec["start"]),
                          "ago": ago(now - rec["end"]), "nudges": rec["nudges"]} if rec else None,
        }
