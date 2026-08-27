"""Hermes Event Bridge 通知插件 —— register() 入口。

监听 Hermes 生命周期 hook，将「session 完成任务 / 需要授权 / 授权已决定」
主动 POST 到事件桥（bridge.py），由桥推送给 Arcs-Mini 开发板等设备。

配置获取顺序（兼容 Hermes 0.17.x ~ 0.20.x，无需 ctx.get_config）：
    1. 环境变量  HERMES_BRIDGE_URL / HERMES_BRIDGE_TOKEN
    2. config.yaml 顶层 event_bridge 块:
         event_bridge:
           url: http://<你的桥服务器>:8788
           token: <你的token>
    3. 默认值 http://127.0.0.1:8788

注意：
- 本插件只做「外发通知」，不阻塞任何流程（observer 型 hook）。
- 网络错误会被记录但绝不影响 Hermes 主流程。
- 回调签名必须是 **kwargs（0.17.0 的 invoke_hook 以 cb(**kwargs) 调用）。
"""

import json
import logging
import os
import time
import urllib.request

log = logging.getLogger("hermes-event-bridge")

# 允许外发的事件 → 映射到桥的事件类型
_EVENTS = {
    "session_end": "session.end",
    "approval_request": "approval.requested",
    "approval_response": "approval.decided",
}

_DEFAULT_URL = "http://127.0.0.1:8788"
_SETTINGS = None  # 模块级缓存，register() 时加载一次


def _load_settings() -> dict:
    """按 环境变量 -> config.yaml event_bridge 块 -> 默认值 的顺序读取配置。"""
    s = {
        "bridge_url": os.environ.get("HERMES_BRIDGE_URL", "").strip(),
        "bridge_token": os.environ.get("HERMES_BRIDGE_TOKEN", "").strip(),
    }
    try:  # config.yaml 顶层块（任意 Hermes 版本都走的通用路径）
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        eb = (cfg.get("event_bridge") or {}) if isinstance(cfg, dict) else {}
        if isinstance(eb, dict):
            if not s["bridge_url"]:
                s["bridge_url"] = str(eb.get("url", "")).strip()
            if not s["bridge_token"]:
                s["bridge_token"] = str(eb.get("token", "")).strip()
    except Exception as e:  # noqa: BLE001 - 配置读取失败不致命
        log.debug("config.yaml 读取失败，使用默认配置: %s", e)
    if not s["bridge_url"]:
        s["bridge_url"] = _DEFAULT_URL
    return s


def _post(settings: dict, event_type: str, payload: dict):
    """POST 事件到事件桥。失败仅记录日志，绝不抛异常影响 Hermes。"""
    url = (settings.get("bridge_url") or _DEFAULT_URL).rstrip("/") + "/event"
    body = {"event": event_type, **payload}
    req = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    token = settings.get("bridge_token") or ""
    if token:
        req.add_header("X-Bridge-Token", token)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            status = r.status
        log.info("bridge 事件 %s -> %s (HTTP %s)", event_type, url, status)
    except Exception as e:  # noqa: BLE001 - 通知失败不影响主流程
        log.warning("bridge 事件 %s 发送失败: %s", event_type, e)


def _notify(settings: dict, ev_key: str, **fields):
    if ev_key not in _EVENTS:
        return
    notify_on = settings.get("notify_on")
    if notify_on is None:
        notify_on = list(_EVENTS.keys())
    if not isinstance(notify_on, list) or ev_key in notify_on:
        _post(settings, _EVENTS[ev_key], fields)


# ---------------------------------------------------------------- hooks
# 签名统一为 **kwargs：Hermes 0.17.0 invoke_hook 以 cb(**kwargs) 调用，
# 回调里只取关心的字段，任何异常都被 invoke_hook 捕获（observer 型）。


def on_session_end(**kwargs):
    """会话一轮完成时触发：session.end"""
    settings = _SETTINGS or _load_settings()
    status = "ended"
    if kwargs.get("completed"):
        status = "completed"
    elif kwargs.get("failed"):
        status = "failed"
    elif kwargs.get("interrupted"):
        status = "interrupted"
    else:
        status = kwargs.get("turn_exit_reason") or "ended"

    _notify(
        settings,
        "session_end",
        session_id=kwargs.get("session_id", ""),
        task_id=kwargs.get("task_id", ""),
        turn_id=kwargs.get("turn_id", ""),
        status=status,
        turn_exit_reason=kwargs.get("turn_exit_reason", ""),
        model=kwargs.get("model", ""),
        platform=kwargs.get("platform", ""),
        ts=time.time(),
    )


def pre_approval_request(**kwargs):
    """Hermes 需要用户授权时触发：approval.requested"""
    settings = _SETTINGS or _load_settings()
    _notify(
        settings,
        "approval_request",
        command=kwargs.get("command", ""),
        description=kwargs.get("description", ""),
        pattern_key=kwargs.get("pattern_key", ""),
        session_key=kwargs.get("session_key", ""),
        surface=kwargs.get("surface", ""),
        turn_id=kwargs.get("turn_id", ""),
        ts=time.time(),
    )


def post_approval_response(**kwargs):
    """用户对授权做出决定后触发：approval.decided"""
    settings = _SETTINGS or _load_settings()
    _notify(
        settings,
        "approval_response",
        command=kwargs.get("command", ""),
        description=kwargs.get("description", ""),
        choice=kwargs.get("choice", ""),
        decided_by=kwargs.get("decided_by", ""),
        session_key=kwargs.get("session_key", ""),
        surface=kwargs.get("surface", ""),
        ts=time.time(),
    )


def register(ctx):
    """插件注册入口 —— Hermes 在每个进程启动时调用。"""
    global _SETTINGS
    _SETTINGS = _load_settings()
    ctx.register_hook("on_session_end", on_session_end)
    ctx.register_hook("pre_approval_request", pre_approval_request)
    ctx.register_hook("post_approval_response", post_approval_response)
    url = _SETTINGS.get("bridge_url")
    token = "（带鉴权）" if _SETTINGS.get("bridge_token") else "（无鉴权）"
    log.info("⭐ hermes-event-bridge 插件已注册，事件将外发到 %s %s", url, token)