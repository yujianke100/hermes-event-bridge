"""Hermes Event Bridge 通知插件 —— register() 入口。

监听 Hermes 生命周期 hook，将「会话空闲(任务完成) / 需要授权 / 授权已决定」
主动 POST 到事件桥（bridge.py），由桥推送给 Arcs-Mini 开发板等设备。

配置获取顺序（兼容 Hermes 0.17.x ~ 0.20.x，无需 ctx.get_config）：
    1. 环境变量  HERMES_BRIDGE_URL / HERMES_BRIDGE_TOKEN
    2. config.yaml 顶层 event_bridge 块:
         event_bridge:
           url: http://<你的桥服务器>:8788
           token: <你的token>
           tone_enabled: true        # 是否播放提示音 (默认 true)
           duration_ms: 3000         # 显示时长 ms; 0=默认3000; -1=常驻直到下一轮对话
           idle_seconds: 120         # 空闲多少秒后视为任务完成
    3. 默认值 http://127.0.0.1:8788

通知策略（重要!）:
    Hermes 的 on_session_end hook 是【每一轮对话结束】都触发（turn 级），
    不是整个任务完成才触发 —— 若每轮都外发, 设备会频繁响铃。
    本插件使用「空闲检测」: 每个 on_session_end 只刷新活动时间戳并重置
    定时器; 会话空闲连续 IDLE_SECONDS 秒无新轮次, 才外发一次
    "session.end(completed, idle)" 通知。连续交互(用户还在聊天/干活)完全静默。

硬件端交互:
    - 每个通知可携带 title(自定义标题)/subtitle/tone(0|1)/duration_ms。
    - 常驻模式: duration_ms=65535(-1) 时, toast 不自动消失, 直到
      Hermes 开始新一轮对话 (on_session_start) 才清除 —— 通过外发
      session.start 事件实现。
    - 多会话堆叠: 设备端最多同时显示 3 条通知, 上下堆叠。

注意：
- 本插件只做「外发通知」，不阻塞任何流程（observer 型 hook）。
- 网络错误会被记录但绝不影响 Hermes 主流程。
- 回调签名必须是 **kwargs（0.17.0 的 invoke_hook 以 cb(**kwargs) 调用）。
"""

import json
import logging
import os
import socket
import threading
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

# ---------------- 配置（环境变量/event_bridge 块均可覆盖） ----------------
_IDLE_SECONDS = 120.0                 # 空闲多少秒视为「任务完成」
_NOTIFY_COOLDOWN_S = 300.0            # 同一 session 两次通知最小间隔
_TONE_ENABLED = True                  # 是否播放提示音
_DURATION_MS = 3000                   # 显示时长; -1 = 常驻直到下一轮
_STICKY_DURATION = 65535              # 设备端常驻标记 (0xFFFF)

_lock = threading.Lock()
_last_activity = {}   # session_id -> 最近一次 on_session_end 时间戳
_last_notified = {}   # session_id -> 最近一次外发通知的时间戳
_timers = {}          # session_id -> threading.Timer
_titles = {}          # session_id -> 最近一次用户消息开头 (session 标题)


def _get_host_ip() -> str:
    """获取本机在局域网中的 IP（用于设备端显示通知来源）。
    通过 UDP 连一个不可达地址来触发系统路由选择，不真正发包。
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.1)
        try:
            s.connect(("10.255.255.255", 1))
            ip = s.getsockname()[0]
        finally:
            s.close()
        if ip and ip != "0.0.0.0":
            return ip
    except Exception:  # noqa: BLE001
        pass
    return ""


def _load_settings() -> dict:
    """按 环境变量 -> config.yaml event_bridge 块 -> 默认值 的顺序读取配置。"""
    global _IDLE_SECONDS, _TONE_ENABLED, _DURATION_MS

    s = {
        "bridge_url": os.environ.get("HERMES_BRIDGE_URL", "").strip(),
        "bridge_token": os.environ.get("HERMES_BRIDGE_TOKEN", "").strip(),
        "tone_enabled": True,
        "duration_ms": 3000,
        "idle_seconds": 120,
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
            if "tone_enabled" in eb:
                s["tone_enabled"] = bool(eb.get("tone_enabled"))
            if "duration_ms" in eb:
                s["duration_ms"] = int(eb.get("duration_ms"))
            if "idle_seconds" in eb:
                s["idle_seconds"] = float(eb.get("idle_seconds"))
    except Exception as e:  # noqa: BLE001 - 配置读取失败不致命
        log.debug("config.yaml 读取失败，使用默认配置: %s", e)
    if not s["bridge_url"]:
        s["bridge_url"] = _DEFAULT_URL

    _IDLE_SECONDS = float(s.get("idle_seconds", 120))
    _TONE_ENABLED = bool(s.get("tone_enabled", True))
    _DURATION_MS = int(s.get("duration_ms", 3000))
    return s


def _post(settings: dict, event_type: str, payload: dict):
    """POST 事件到事件桥。失败仅记录日志，绝不抛异常影响 Hermes。"""
    url = (settings.get("bridge_url") or _DEFAULT_URL).rstrip("/") + "/event"
    # 统一注入 Hermes 本机 IP + 通知偏好（音效/时长）
    body = {"event": event_type, **payload}
    if not body.get("host"):
        host_ip = _get_host_ip()
        if host_ip:
            body["host"] = host_ip
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


def _notify_common(session_key: str, **extra):
    """组装公共通知字段 (音效开关 + 显示时长 + session 标题)。"""
    settings = _SETTINGS or _load_settings()
    title = extra.pop("title", None) or "任务完成"
    payload = {
        "session_id": session_key,
        "tone": "1" if (_TONE_ENABLED and extra.pop("force_tone", True)) else "0",
        "duration_ms": _DURATION_MS if _DURATION_MS >= 0 else _STICKY_DURATION,
        "ts": time.time(),
        **extra,
    }
    if title:
        payload["title"] = title
    return settings, payload


# ---------------- 空闲检测核心 ----------------

def _reset_idle_timer(session_id: str, anchor_ts: float):
    """(持锁调用) 为 session 重置空闲定时器。anchor_ts = 期望的活动时间戳,
    定时器触发时若活动时间戳已前进(又有新轮次), 则静默取消。"""
    old = _timers.pop(session_id, None)
    if old:
        try:
            old.cancel()
        except Exception:  # noqa: BLE001
            pass

    t = threading.Timer(_IDLE_SECONDS, _on_idle, args=(session_id, anchor_ts))
    t.daemon = True
    _timers[session_id] = t
    t.start()


def _on_idle(session_id: str, anchor_ts: float):
    """空闲定时器触发: 若期间无新活动且冷却期已过, 外发「任务完成」通知。"""
    settings = _SETTINGS or _load_settings()
    with _lock:
        if _last_activity.get(session_id) != anchor_ts:
            # 期间又有新轮次(用户继续交互), 静默放弃
            _timers.pop(session_id, None)
            return
        _timers.pop(session_id, None)

        now = time.time()
        last_n = _last_notified.get(session_id, 0.0)
        if now - last_n < _NOTIFY_COOLDOWN_S:
            log.info("session %s idle but within notify cooldown, skip", session_id)
            return
        _last_notified[session_id] = now

    title = str(_titles.get(session_id, "") or "").strip()
    if title:
        title = title[:20]  # 取标题前几个字
    else:
        title = "任务完成"

    _post(settings, "session.end",
          {
              "session_id": session_id,
              "task_id": session_id,
              "status": "completed",
              "turn_exit_reason": "idle(%.0fs)" % _IDLE_SECONDS,
              "model": "",
              "platform": "",
              "title": title,
              "subtitle": "任务完成 · Hermes",
              "tone": "1" if _TONE_ENABLED else "0",
              "duration_ms": _DURATION_MS if _DURATION_MS >= 0 else _STICKY_DURATION,
              "ts": time.time(),
          })
    log.info("session %s idle reached, notified once", session_id)


def _send_session_start(session_id: str, title: str):
    """Hermes 开始新一轮对话: 通知设备清除常驻 toast (上一轮的通知)。
    title 同时用于后续 session.end 显示。"""
    settings = _SETTINGS or _load_settings()
    with _lock:
        _titles[session_id] = title
    _post(settings, "session.start",
          {
              "session_id": session_id,
              "title": (title or "")[:20],
              "ts": time.time(),
          })
    log.info("session %s started (title=%s), sent clear", session_id, title)


# ---------------- 天气城市设置 ----------------
# 用户直接在 Hermes 说「把天气城市设为广州」等，插件识别后 POST 桥 /config
# 更新城市，设备端下次拉 /weather 自动生效。不拦截原 LLM 对话——事件照常发，
# 只是附带一次配置更新。


def _city_set_intent(text: str):
    """从用户消息里提取「设城市」意图，返回城市名或 None。

    支持: 把天气城市设为XX / 天气城市改成XX / 城市设为XX / 换成XX天气等
    """
    if not text:
        return None
    import re

    patterns = [
        r"(?:把)?(?:天气)?城市(?:设为|改成|换为|改为|设置成|设成|变成)\s*([\u4e00-\u9fa5A-Za-z]{2,10})",
        r"(?:天气)?换成\s*([\u4e00-\u9fa5A-Za-z]{2,10})(?:天气)?",
        r"城市(?:设为|改成|换为|设置成|设成|改为)\s*([\u4e00-\u9fa5A-Za-z]{2,10})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            city = m.group(1).strip()
            # 过滤掉 "天气" "城市" 等词尾 (如 "设为天气" 误匹配)
            if city in ("天气", "城市", "天气预报"):
                return None
            # "换成厦门天气" 这类会把尾部"天气"吃进捕获组, 裁掉
            if city.endswith("天气"):
                city = city[:-2].strip()
            if not city:
                return None
            return city
    return None


def _update_weather_city(city: str):
    """POST 桥 /config 更新城市。成功返回 True。"""
    settings = _SETTINGS or _load_settings()
    url = settings.get("url", "").rstrip("/")
    token = settings.get("token", "")
    if not url:
        log.warning("bridge url not configured, cannot set weather city")
        return False
    try:
        body = json.dumps({"city": city}).encode("utf-8")
        req = urllib.request.Request(
            url + "/config", data=body, method="POST",
            headers={"Content-Type": "application/json", "X-Bridge-Token": token})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            ok = resp.status == 200 and data.get("ok")
            if ok:
                log.info("weather city updated to %s (resp=%s)", city, data.get("city"))
            else:
                log.warning("weather city update rejected: %s", data)
            return bool(ok)
    except Exception as e:  # noqa: BLE001
        log.warning("weather city update failed: %s", e)
        return False


# ---------------- hooks ----------------
# 签名统一为 **kwargs：Hermes 0.17.0 invoke_hook 以 cb(**kwargs) 调用。
# 任何异常都由 invoke_hook 捕获（observer 型）。


def on_session_start(**kwargs):
    """新一轮对话开始: 抓标题 + 发 session.start 清除常驻 toast。"""
    session_id = str(kwargs.get("session_id") or "")
    if not session_id:
        return
    _send_session_start(session_id, "")


def pre_llm_call(**kwargs):
    """LLM 调用前: 抓取用户消息开头作为 session 标题 (第一轮才有文案)。
    同时检测「设天气城市」指令 → 更新桥端城市配置 (不阻塞原对话)。"""
    session_id = str(kwargs.get("session_id") or "")
    if not session_id:
        return
    is_first = kwargs.get("is_first_turn")
    user_msg = str(kwargs.get("user_message") or "").strip()

    # 天气城市设置指令 (第一轮消息检测, 非阻塞)
    if is_first and user_msg:
        city = _city_set_intent(user_msg)
        if city:
            log.info("weather city intent detected: %s", city)
            _update_weather_city(city)

    if is_first and user_msg:
        with _lock:
            _titles[session_id] = user_msg
        title = user_msg[:20]
        _post(_SETTINGS or _load_settings(), "session.start",
              {"session_id": session_id, "title": title, "ts": time.time()})
        log.info("session %s title captured: %s", session_id, title)


def on_session_end(**kwargs):
    """每一轮对话/任务结束都触发（turn 级）。

    不做实际外发 —— 只刷新空闲检测状态。会话空闲累计 IDLE_SECONDS 秒
    无新轮次后, 由 _on_idle 统一外发一次「任务完成」通知。
    """
    session_id = str(kwargs.get("session_id") or "")
    if not session_id:
        return
    now = time.time()
    with _lock:
        _last_activity[session_id] = now
        _reset_idle_timer(session_id, now)


def pre_approval_request(**kwargs):
    """Hermes 需要用户授权时触发：approval.requested（立即外发, 不等空闲）"""
    settings = _SETTINGS or _load_settings()
    _post(
        settings,
        "approval.requested",
        command=kwargs.get("command", ""),
        description=kwargs.get("description", ""),
        pattern_key=kwargs.get("pattern_key", ""),
        session_key=kwargs.get("session_key", ""),
        surface=kwargs.get("surface", ""),
        turn_id=kwargs.get("turn_id", ""),
        title="需要授权",
        subtitle="Hermes 请求批准操作",
        tone="1" if _TONE_ENABLED else "0",
        duration_ms=_DURATION_MS if _DURATION_MS >= 0 else _STICKY_DURATION,
        ts=time.time(),
    )


def post_approval_response(**kwargs):
    """用户对授权做出决定后触发：approval.decided（立即外发）"""
    settings = _SETTINGS or _load_settings()
    _post(
        settings,
        "approval.decided",
        command=kwargs.get("command", ""),
        description=kwargs.get("description", ""),
        choice=kwargs.get("choice", ""),
        decided_by=kwargs.get("decided_by", ""),
        session_key=kwargs.get("session_key", ""),
        surface=kwargs.get("surface", ""),
        title="授权结果",
        tone="1" if _TONE_ENABLED else "0",
        duration_ms=_DURATION_MS if _DURATION_MS >= 0 else _STICKY_DURATION,
        ts=time.time(),
    )


def register(ctx):
    """插件注册入口 —— Hermes 在每个进程启动时调用。"""
    global _SETTINGS
    _SETTINGS = _load_settings()
    ctx.register_hook("on_session_start", on_session_start)
    ctx.register_hook("pre_llm_call", pre_llm_call)
    ctx.register_hook("on_session_end", on_session_end)
    ctx.register_hook("pre_approval_request", pre_approval_request)
    ctx.register_hook("post_approval_response", post_approval_response)
    url = _SETTINGS.get("bridge_url")
    token = "（带鉴权）" if _SETTINGS.get("bridge_token") else "（无鉴权）"
    log.info("⭐ hermes-event-bridge 插件已注册 (idle=%ss, tone=%s, dur=%sms)，事件将外发到 %s %s",
             _IDLE_SECONDS, _TONE_ENABLED, _DURATION_MS, url, token)