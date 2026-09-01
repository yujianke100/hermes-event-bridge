"""Hermes Event Bridge — 导管版插件 (v1.3.0)

Hermes 0.20.6 引入 worker 池架构: 对话跑在独立 worker 进程
(hermes_bridge.py --worker-profile), 原生 outbound webhook 只注册在
gateway 主进程 → worker 进程里触发的 hook 事件发不出去。

本插件是极简「导管」: 在 worker 进程内把 hook kwargs 转发到桥端,
格式与原生 outbound webhook 完全一致:
    {"hook_event_name": "...", "session_id": "...", "extra": {...}}
所有智能 (会话标题 / 城市指令 / 打断静默 / 授权翻译) 都在桥端
_handle_hermes_hook() 完成, 插件不做任何业务判断。

配置 (config.yaml 顶层 event_bridge 块, 或环境变量):
    event_bridge:
      url: http://<桥服务器>:8788
      token: <token>
"""

import json
import logging
import os
import urllib.request

log = logging.getLogger("hermes-event-bridge")

_DEFAULT_URL = "http://127.0.0.1:8788"
_SETTINGS = None

# 桥端 _handle_hermes_hook 会处理的 hook 白名单
_EVENTS = (
    "on_session_start",
    "on_session_end",
    "pre_llm_call",
    "pre_approval_request",
    "post_approval_response",
)

# 每个 hook 只转发桥端需要的字段, 避免 conversation_history 等大对象浪费带宽
_FIELD_WHITELIST = {
    "on_session_start": ("session_id",),
    "pre_llm_call": ("user_message", "is_first_turn"),
    "on_session_end": ("completed", "failed", "interrupted", "turn_exit_reason"),
    "pre_approval_request": ("command", "description", "pattern_key", "session_key"),
    "post_approval_response": ("command", "description", "choice", "decided_by", "session_key"),
}


def _load_settings() -> dict:
    """读取 event_bridge 配置 (环境变量优先, 其次 config.yaml, 最后默认值)。"""
    url = os.environ.get("HERMES_BRIDGE_URL", "").strip()
    token = os.environ.get("HERMES_BRIDGE_TOKEN", "").strip()

    if not url or not token:
        try:
            from hermes_cli.config import load_config

            cfg = load_config()
            eb = (cfg.get("event_bridge") or {}) if isinstance(cfg, dict) else {}
            if not url:
                url = str(eb.get("url", "")).strip()
            if not token:
                token = str(eb.get("token", "")).strip()
        except Exception:  # noqa: BLE001
            pass

    if not url:
        url = _DEFAULT_URL
    return {"url": url, "token": token}


def _forward(hook_name: str, kwargs: dict):
    """把 hook kwargs 原样转发到桥端 (与 outbound webhook 同格式)。"""
    settings = _SETTINGS or _load_settings()
    url = settings.get("url", "").rstrip("/")
    if not url:
        return

    session_id = str(
        kwargs.get("session_id") or kwargs.get("session_key") or kwargs.get("parent_session_id") or ""
    )
    keys = _FIELD_WHITELIST.get(hook_name, ())
    extra = {k: v for k, v in kwargs.items() if k in keys}

    body = json.dumps(
        {"hook_event_name": hook_name, "session_id": session_id, "extra": extra},
        ensure_ascii=False,
    ).encode("utf-8")

    headers = {"Content-Type": "application/json"}
    token = settings.get("token", "")
    if token:
        headers["X-Bridge-Token"] = token

    try:
        req = urllib.request.Request(url + "/event", data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            log.info("forward %s -> %s (HTTP %s)", hook_name, url, resp.status)
    except Exception as e:  # noqa: BLE001
        log.warning("forward %s failed: %s", hook_name, e)


# ---------------- hooks (observer 型, **kwargs 签名) ----------------


def on_session_start(**kwargs):
    _forward("on_session_start", kwargs)


def on_session_end(**kwargs):
    _forward("on_session_end", kwargs)


def pre_llm_call(**kwargs):
    _forward("pre_llm_call", kwargs)


def pre_approval_request(**kwargs):
    _forward("pre_approval_request", kwargs)


def post_approval_response(**kwargs):
    _forward("post_approval_response", kwargs)


def register(ctx):
    """插件注册入口 —— Hermes 在 worker/gateway 进程启动时调用。"""
    global _SETTINGS
    _SETTINGS = _load_settings()

    try:
        ctx.register_hook("on_session_start", on_session_start)
        ctx.register_hook("on_session_end", on_session_end)
        ctx.register_hook("pre_llm_call", pre_llm_call)
        ctx.register_hook("pre_approval_request", pre_approval_request)
        ctx.register_hook("post_approval_response", post_approval_response)
        log.info(
            "⭐ hermes-event-bridge 导管插件已注册 (url=%s)",
            _SETTINGS.get("url", _DEFAULT_URL),
        )
    except Exception as e:  # noqa: BLE001
        log.warning("register failed: %s", e)