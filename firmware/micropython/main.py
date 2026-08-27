# main.py — Arcs-Mini 事件通知设备端 (MicroPython 备用方案)
# 用法：连上 Wi-Fi 后运行本文件即可。屏幕/LED/喇叭函数按你的驱动改。
#
# 依赖：urequests（MicroPython 自带 / 可安装）
# 关键：since 断点 —— 收到事件后记录最大 ts，掉线/重启后补拉不掉事件。

import json
import time

try:
    import urequests
except ImportError:
    raise SystemExit("缺少 urequests 库")

# ===== 桥配置（改成你自己的部署）=====
BRIDGE = "http://YOUR_BRIDGE_HOST:8788"
TOKEN = "YOUR_BRIDGE_TOKEN"          # 与 bridge.py --token 一致
HDR = {"X-Bridge-Token": TOKEN}
POLL_TIMEOUT = 25  # 长轮询挂起秒数

# ===== 提示效果（示例：按你的板子驱动替换）=====
def ui_screen(line1, line2=""):
    """写两行文字到 ST7789V 屏。中文需字库，英文/数字开箱即用。"""
    print("[屏]", line1, "|", line2)  # 替换为你的 LCD 驱动调用

def ui_led(times, color="green"):
    print("[LED]", times, "x", color)

def ui_led_blink(on):
    print("[LED]", "blink-start" if on else "blink-stop")

def ui_audio(kind):
    print("[音]", kind)

# ===== 事件处理 =====
def handle(ev):
    kind = ev.get("event", "")
    if kind == "session.end":
        st = ev.get("status", "?")
        if st == "completed":
            ui_screen("OK 任务完成", ev.get("session_id", "")[:10])
            ui_led(2, "green"); ui_audio("up-beep")
        elif st == "failed":
            ui_screen("!! 任务失败", ev.get("session_id", "")[:10])
            ui_led(5, "red"); ui_audio("low-beep x2")
        else:
            ui_screen("- 会话结束 -", st); ui_led(1, "blue")
    elif kind == "approval.requested":
        ui_screen("需要授权!", (ev.get("description") or ev.get("command") or "")[:10])
        ui_led_blink(True); ui_audio("triple-beep")
    elif kind == "approval.decided":
        ch = ev.get("choice", "")
        ui_led_blink(False)
        ui_screen("已批准" if ch == "approved" else "已拒绝", "")
        ui_led(1, "green" if ch == "approved" else "red"); ui_audio("click")

# ===== 主循环：长轮询 =====
def run():
    since = 0.0  # 可持久化到文件，重启后恢复：open('/since','r').read()
    while True:
        try:
            url = "%s/poll?since=%.3f&timeout=%d" % (BRIDGE, since, POLL_TIMEOUT)
            r = urequests.get(url, headers=HDR, timeout=POLL_TIMEOUT + 5)
            data = r.json()
            r.close()
            for ev in data.get("events", []):
                if ev.get("ts", 0) > since:
                    since = ev["ts"]
                handle(ev)
        except Exception as e:
            print("[net] 错误:", e)
            time.sleep(5)  # 退避；桥保留 12h 事件，恢复后按 since 补拉

if __name__ == "__main__":
    print("Hermes 事件通知设备端启动，长轮询 %s ..." % BRIDGE)
    run()