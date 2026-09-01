#!/usr/bin/env python3
"""方案A: bridge.py 智能逻辑单测 (不依赖真实 flask, 用 stub)."""
import sys
import types
import json
import io

# ---- Flask stub ----
def _jsonify(*args, **kwargs):
    class R:
        status_code = 200
        def __init__(self, obj):
            self.obj = obj
        def get_json(self, silent=False):
            return self.obj
        @property
        def data(self):
            return json.dumps(self.obj, ensure_ascii=False).encode()
    if args:
        return R(args[0])
    return R(kwargs)

flask_stub = types.ModuleType("flask")
flask_stub.Flask = lambda *a, **k: types.SimpleNamespace(
    json=types.SimpleNamespace(ensure_ascii=False),
    config={}, add_url_rule=lambda *a, **k: None, route=lambda *a, **k: (lambda f: f),
)
flask_stub.request = types.SimpleNamespace(
    get_json=lambda silent=False: {},
    args={}, headers={}, method="GET",
)
flask_stub.jsonify = _jsonify
sys.modules["flask"] = flask_stub

cors_stub = types.ModuleType("flask_cors")
cors_stub.CORS = lambda app: None
sys.modules["flask_cors"] = cors_stub

# ---- stub outbound_webhooks import (bridge.py 不 import, 但保险) ----
import importlib.util

spec = importlib.util.spec_from_file_location("bridge_mod", r"E:\hermes\hermes-event-bridge\bridge.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

pass_count = 0
fail_count = 0

def check(name, cond, detail=""):
    global pass_count, fail_count
    if cond:
        pass_count += 1
        print(f"  ✓ {name}")
    else:
        fail_count += 1
        print(f"  ✗ {name} {detail}")

print("== _city_set_intent ==")
for text, expect in [
    ("把天气城市设为广州", "广州"),
    ("天气城市改成上海", "上海"),
    ("换成厦门天气", "厦门"),
    ("换成成都", "成都"),
    ("帮我查天气", None),
    ("把天气城市设为天气", None),
    ("今天深圳天气如何", None),
]:
    got = mod._city_set_intent(text)
    check(f"{text!r} -> {got!r}", got == expect, f"got={got!r} expect={expect!r}")

print("== _handle_hermes_hook 事件翻译 ==")
# 清空事件队列便于检查
with mod._lock:
    mod._events.clear()

# 1. on_session_end completed -> session.end completed
r, code = mod._handle_hermes_hook("on_session_end", "s1", {"completed": True, "failed": False, "interrupted": False, "turn_exit_reason": "text_response(stop)"})
check("completed -> 200 + session.end", code == 200 and r.obj.get("note") == "session.end-completed", f"{r.obj}")
ev = list(mod._events)[-1]
check("  事件类型 session.end completed", ev["event"] == "session.end" and ev["status"] == "completed", f"{ev}")

# 2. on_session_end interrupted -> 静默 (不产生事件)
n_before = len(mod._events)
r, code = mod._handle_hermes_hook("on_session_end", "s1", {"completed": False, "failed": False, "interrupted": True, "turn_exit_reason": "interrupted_during_api_call"})
check("interrupted -> 静默无新事件", code == 200 and len(mod._events) == n_before and "silent" in r.obj.get("note"), f"{r.obj}")

# 3. on_session_end failed -> session.end failed
mod._handle_hermes_hook("on_session_end", "s2", {"completed": False, "failed": True, "interrupted": False, "turn_exit_reason": "api_error"})
ev = list(mod._events)[-1]
check("failed -> 红色 session.end", ev["event"] == "session.end" and ev["status"] == "failed", f"{ev}")

# 4. pre_llm_call 第一轮 -> 记住标题
mod._handle_hermes_hook("pre_llm_call", "s3", {"user_message": "帮我看看杭州明天天气", "is_first_turn": True})
check("pre_llm_call 记住标题", mod._get_title("s3") == "帮我看看杭州明天天气", f"{mod._get_title('s3')}")

# 5. pre_llm_call 城市指令 -> 设置城市 + 事件
mod._handle_hermes_hook("pre_llm_call", "s4", {"user_message": "把天气城市设为广州", "is_first_turn": True})
cfg = mod._load_config()
check("城市指令 -> config 更新", cfg.get("city") == "广州", f"{cfg}")
ev = list(mod._events)[-1]
check("城市指令 -> 完成事件通知", ev.get("title") == "天气已设为广州", f"{ev}")

# 6. on_session_start -> session.start
mod._handle_hermes_hook("on_session_start", "s5", {})
ev = list(mod._events)[-1]
check("on_session_start -> session.start", ev["event"] == "session.start", f"{ev}")

# 7. pre_approval_request -> approval.requested
mod._handle_hermes_hook("pre_approval_request", "s6", {"command": "rm -rf /tmp"})
ev = list(mod._events)[-1]
check("approval.requested 黄", ev["event"] == "approval.requested" and ev["title"] == "需要授权", f"{ev}")

# 8. post_approval_response denied -> approval.decided denied
mod._handle_hermes_hook("post_approval_response", "s6", {"choice": "deny"})
ev = list(mod._events)[-1]
check("approval.decided denied 红", ev["event"] == "approval.decided" and ev["status"] == "denied", f"{ev}")

# 恢复杭州
mod._set_weather_city("杭州")

print(f"\n== 结果: {pass_count} 通过, {fail_count} 失败 ==")
sys.exit(1 if fail_count else 0)