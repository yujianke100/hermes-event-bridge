#!/usr/bin/env python3
"""插件自测：模拟 Hermes 的 ctx，调用 register()，触发各 hook，验证事件送达桥服务。

用法:
    python test_plugin.py [--bridge http://127.0.0.1:8788] [--token <t>]

说明:
    register.py 的 hook 回调是 **kwargs 签名（Hermes 0.17.0 invoke_hook 以
    cb(**kwargs) 调用），且配置通过环境变量 / config.yaml event_bridge 块读取。
    本自测设置环境变量后再 register，触发 hook 时按真实运行时方式调用。
"""
import argparse
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import register as plugin  # noqa: E402


class FakeCtx:
    """最小化模拟 Hermes 0.17.0 的 PluginContext：register_hook。"""

    def __init__(self):
        self.hooks = {}

    def register_hook(self, name, fn):
        self.hooks[name] = fn


def fetch_events(bridge, since=0.0, token=""):
    req = urllib.request.Request(f"{bridge}/events?since={since}")
    if token:
        req.add_header("X-Bridge-Token", token)
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode()).get("events", [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bridge", default="http://127.0.0.1:8788")
    ap.add_argument("--token", default="")
    args = ap.parse_args()
    bridge = args.bridge
    token = args.token

    # 配置由环境变量注入（生产环境等价于 config.yaml event_bridge 块）
    os.environ["HERMES_BRIDGE_URL"] = bridge
    if token:
        os.environ["HERMES_BRIDGE_TOKEN"] = token

    before = fetch_events(bridge, token=token)
    since = max((e["ts"] for e in before), default=0.0)
    print(f"[test] 基线: 桥已有 {len(before)} 个事件, since={since:.3f}")

    ctx = FakeCtx()
    plugin.register(ctx)
    print(f"[test] register() 注册的 hooks: {sorted(ctx.hooks.keys())}")
    assert set(ctx.hooks) >= {"on_session_end", "pre_approval_request", "post_approval_response"}, "hooks 注册不完整!"
    print(f"[test] 插件配置 bridge_url={plugin._SETTINGS.get('bridge_url')}")

    # 触发三个 hook —— 按真实运行时方式调用：cb(**kwargs)，不传 ctx
    ctx.hooks["on_session_end"](
        session_id="sess-test-1", task_id="task-1", turn_id="turn-1",
        completed=True, failed=False, interrupted=False,
        turn_exit_reason="completed", model="deepseek-v4-flash", platform="cli",
    )
    time.sleep(0.3)
    ctx.hooks["pre_approval_request"](
        command="write_file", description="需要授权写入文件",
        pattern_key="write", session_key="sess-test-1",
        surface="cli", turn_id="turn-2",
    )
    time.sleep(0.3)
    ctx.hooks["post_approval_response"](
        command="write_file", description="写入文件",
        choice="approved", decided_by="user",
        session_key="sess-test-1", surface="cli",
    )
    time.sleep(0.5)

    after = fetch_events(bridge, since=since, token=token)
    kinds = [(e["event"], e.get("session_id") or e.get("session_key")) for e in after]
    print(f"[test] 桥新增事件: {kinds}")

    events_by_kind = {e["event"]: e for e in after}
    assert "session.end" in events_by_kind, "session.end 未送达!"
    assert events_by_kind["session.end"].get("status") == "completed", "status 错误"
    assert "approval.requested" in events_by_kind, "approval.requested 未送达!"
    assert events_by_kind["approval.requested"].get("description") == "需要授权写入文件", "授权描述错误"
    assert "approval.decided" in events_by_kind, "approval.decided 未送达!"
    assert events_by_kind["approval.decided"].get("choice") == "approved", "授权结果错误"

    print("\n✅ 插件自测通过: 3 类事件全部由插件 hook 正确外发到桥服务")
    print("   - session.end         (status=completed)")
    print("   - approval.requested  (需要授权)")
    print("   - approval.decided    (choice=approved)")


if __name__ == "__main__":
    main()