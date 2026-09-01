#!/usr/bin/env python3
"""
Hermes Event Bridge — 事件桥服务
=================================
接收 Hermes 插件主动外发的事件（session 完成 / 授权请求等），
并通过 WebSocket / 长轮询推送给物联网设备（如 LiSenAI Arcs-Mini 开发板）。

架构：
    [任意环境的 Hermes Studio]
        │  plugin:  on_session_end / pre_approval_request / ...
        │  POST http://<bridge>/event
        ▼
    [本服务 bridge.py]
        │  事件队列（内存，TTL 12h）
        │  ▲ WS 订阅 (ws://<bridge>/ws)   ▲ 长轮询 (GET /poll)
        ▼
    [Arcs-Mini 开发板 / 任何设备]

事件格式（JSON）：
    {
      "event": "session.end" | "approval.requested" | "approval.decided" | ...,
      "session_id": "...", "title": "...",
      "status": "completed" | "failed" | "interrupted",   # session.end
      "command": "...", "description": "...", "choice": "approved"|"denied",  # approval.*
      "summary": "...", "ts": 1787752659.123
    }

运行：
    python bridge.py [--host 0.0.0.0] [--port 8788] [--token your-secret]
依赖：
    pip install flask flask-cors
    （可选）pip install flask-sock   # 启用 WebSocket /ws
"""

import argparse
import json
import queue
import threading
import time
from collections import deque

try:
    from flask import Flask, request, jsonify
    from flask_cors import CORS
except ImportError:
    raise SystemExit("缺少依赖：请先运行  pip install flask flask-cors")

# ---------------------------------------------------------------- 事件队列
MAX_EVENTS = 2000
_lock = threading.Lock()
_events = deque()              # deque of dict (oldest first)
_ws_clients = set()            # set of queue.Queue  (每个 WS 连接一个队列)
_client_lock = threading.Lock()


def _now():
    return time.time()


def push_event(event_type: str, **fields):
    """入队一个事件，并广播给所有 WS 订阅者。"""
    ev = {"event": event_type, "ts": _now(), **fields}
    with _lock:
        _events.append(ev)
        while len(_events) > MAX_EVENTS:
            _events.popleft()
    with _client_lock:
        clients = list(_ws_clients)
    for q in clients:
        try:
            q.put_nowait(ev)
        except queue.Full:
            pass
    return ev


def list_events(since_ts: float = None, limit: int = 100, newest_first: bool = False):
    """按时间返回事件。

    - newest_first=True: 最近的在最前（/events 展示用）
    - newest_first=False: 最旧在前（FIFO 消费顺序，/poll 用——设备端一次只解析
      第一个事件，若一次返回多个+最新在前，设备处理最新后游标就跳过其余，
      造成事件永久丢失。故 /poll 必须按最旧优先单发。）
    """
    with _lock:
        items = list(_events)
    if since_ts is not None:
        items = [e for e in items if e["ts"] > since_ts]
    items = list(reversed(items))[:limit] if newest_first else items[:limit]
    return items


# ---------------------------------------------------------------- 天气代理 (Open-Meteo)
# 设备端无 TLS 能力 → 由桥代拉 HTTPS 天气，缓存后供设备 GET /weather 使用。
import os
import re
import urllib.request
import urllib.parse

WEATHER_CACHE_TTL = 600          # 天气缓存 10 分钟
GEOCODE_CACHE_TTL = 7 * 86400    # 地理编码缓存 7 天
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

# WMO 天气代码 -> 中文文案 (Open-Meteo weather_code)
WMO_TEXT = {
    0: "晴", 1: "大致晴朗", 2: "多云", 3: "阴",
    45: "雾", 48: "雾凇",
    51: "毛毛雨", 53: "毛毛雨", 55: "毛毛雨",
    56: "冻毛毛雨", 57: "冻毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨",
    66: "冻雨", 67: "冻雨",
    71: "小雪", 73: "中雪", 75: "大雪", 77: "雪粒",
    80: "阵雨", 81: "强阵雨", 82: "强阵雨",
    85: "阵雪", 86: "强阵雪",
    95: "雷暴", 96: "雷暴伴冰雹", 99: "强雷暴伴冰雹",
}

_config_lock = threading.Lock()
_geo_cache = {}      # city -> {"lat","lon","name","tz","ts"}
_weather_cache = {}  # city -> {"ts": ..., "data": {...}}

def _load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"city": "杭州"}

def _save_config(cfg):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

def _http_get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "hermes-event-bridge/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))

def _geocode(city):
    """按城市名查坐标，带缓存。返回 dict 或 None。"""
    now = time.time()
    with _config_lock:
        cached = _geo_cache.get(city)
        if cached and now - cached["ts"] < GEOCODE_CACHE_TTL:
            return cached

    try:
        q = urllib.parse.quote(city)
        data = _http_get_json(
            f"https://geocoding-api.open-meteo.com/v1/search?name={q}&count=1&language=zh&format=json")
        if not data.get("results"):
            return None
        r = data["results"][0]
        geo = {
            "city": r.get("name") or city,
            "lat": r["latitude"],
            "lon": r["longitude"],
            "region": r.get("admin1") or r.get("country") or "",
            "tz": r.get("timezone") or "Asia/Shanghai",
            "ts": now,
        }
        with _config_lock:
            _geo_cache[city] = geo
        return geo
    except Exception as e:
        print(f"[weather] geocode {city} failed: {e}")
        return None

def _fetch_weather(city):
    """取某城市当前天气 + 未来8小时 + 未来4天，带缓存。返回供设备显示的精简 dict。

    为降低设备端解析负担，hourly/daily 直接在桥端拼成紧凑字符串，
    设备端整串存下 → UI 换行显示即可。
    """
    now = time.time()
    with _config_lock:
        cached = _weather_cache.get(city)
        if cached and now - cached["ts"] < WEATHER_CACHE_TTL:
            return cached["data"]

    geo = _geocode(city)
    if not geo:
        return None
    try:
        tz = urllib.parse.quote(geo["tz"])
        data = _http_get_json(
            f"https://api.open-meteo.com/v1/forecast?latitude={geo['lat']}&longitude={geo['lon']}"
            f"&current=temperature_2m,weather_code"
            f"&hourly=temperature_2m,weather_code&forecast_hours=8"
            f"&daily=temperature_2m_max,temperature_2m_min,weather_code&forecast_days=4"
            f"&timezone={tz}")
        cur = data.get("current", {})
        wcode = cur.get("weather_code", 0)
        result = {
            "city": geo["city"],
            "region": geo["region"],
            "temp": round(cur.get("temperature_2m") or 0),
            "code": wcode,
            "text": WMO_TEXT.get(wcode, f"代码{wcode}"),
            "ts": now,
        }

        # 日期 (Asia/Shanghai) 如 "9月1日"
        try:
            from datetime import datetime, timedelta, timezone
            cn_now = datetime.now(timezone(timedelta(hours=8)))
            result["date"] = f"{cn_now.month}月{cn_now.day}日"
        except Exception:
            pass

        # 未来 8 小时: "15时26°多云;16时27°晴" (跳过第一个=当前整点? 保留全部)
        try:
            hh = data.get("hourly", {})
            h_times = hh.get("time", [])[:8]
            h_temps = hh.get("temperature_2m", [])[:8]
            h_codes = hh.get("weather_code", [])[:8]
            parts = []
            for i in range(min(len(h_times), len(h_temps), len(h_codes))):
                t = h_times[i]
                hour = int(t[11:13]) if len(t) >= 13 else i
                parts.append(f"{hour}时{round(h_temps[i])}°{WMO_TEXT.get(h_codes[i], '')}")
            result["hourly"] = ";".join(parts)
        except Exception:
            pass

        # 未来 4 天: "9/1 26-30° 多云;9/2 25-29° 晴"
        try:
            dd = data.get("daily", {})
            d_times = dd.get("time", [])[:4]
            d_max = dd.get("temperature_2m_max", [])[:4]
            d_min = dd.get("temperature_2m_min", [])[:4]
            d_codes = dd.get("weather_code", [])[:4]
            parts = []
            for i in range(min(len(d_times), len(d_max), len(d_min), len(d_codes))):
                t = d_times[i]
                md = f"{int(t[5:7])}/{int(t[8:10])}" if len(t) >= 10 else f"+{i + 1}"
                parts.append(f"{md} {round(d_min[i])}-{round(d_max[i])}° {WMO_TEXT.get(d_codes[i], '')}")
            result["daily"] = ";".join(parts)
        except Exception:
            pass

        with _config_lock:
            _weather_cache[city] = {"ts": now, "data": result}
        return result
    except Exception as e:
        print(f"[weather] fetch {city} failed: {e}")
        return None

# ================================================================
# Hermes 事件智能 (方案A: 桥端是唯一智能点, 插件/webhook 只当导管)
# ================================================================
# 收到 Hermes 生命周期 hook → 翻译成设备可显示的通知事件。
#
# 关键语义 (Hermes 0.20.4):
#   - on_session_end 是【turn 级】, 每轮都触发:
#       completed=True / failed=False / interrupted=False  → 正常完成 → 立即通知
#       interrupted=True (新任务打断旧 turn)                → 静默 (等新任务的完成)
#       failed=True                                         → 立即通知(红色)
#   - on_session_start / pre_llm_call: 会话标题 + 城市指令
#   - pre_approval_request / post_approval_response: 授权黄/绿红
#
# 状态: 只保留「每个 session 的标题」, 不保留 idle 定时器 —
#        「不需要 120 等待」= completed 立即通知, interrupted 静默。

# 会话标题缓存: session_id -> 标题(用户首条消息前几个字)
_session_titles: dict = {}
_session_lock = threading.Lock()

TITLE_MAX = 20  # 设备 toast 标题最多显示的字数


def _remember_title(session_id: str, title: str):
    with _session_lock:
        _session_titles[session_id] = (title or "").strip()[:TITLE_MAX]


def _get_title(session_id: str) -> str:
    with _session_lock:
        return _session_titles.get(session_id, "")


def _city_set_intent(text: str):
    """从用户消息提取设城市意图, 返回城市名或 None。"""
    if not text:
        return None
    patterns = [
        r"(?:把)?(?:天气)?城市(?:设为|改成|换为|改为|设置成|设成|变成)\s*([\u4e00-\u9fa5A-Za-z]{2,10})",
        r"(?:天气)?换成\s*([\u4e00-\u9fa5A-Za-z]{2,10})(?:天气)?",
        r"城市(?:设为|改成|换为|设置成|设成|改为)\s*([\u4e00-\u9fa5A-Za-z]{2,10})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            city = m.group(1).strip()
            if city in ("天气", "城市", "天气预报"):
                return None
            if city.endswith("天气"):
                city = city[:-2].strip()
            if not city:
                return None
            return city
    return None


def _set_weather_city(city: str):
    """验证城市并持久化到 config.json。成功返回城市名, 失败返回 None。"""
    geo = _geocode(city)
    if not geo:
        print(f"[hermes] city '{city}' not found")
        return None
    cfg = _load_config()
    cfg["city"] = geo["city"]
    _save_config(cfg)
    print(f"[hermes] weather city set to {geo['city']}")
    return geo["city"]


def _handle_hermes_hook(hook_name: str, session_id: str, extra: dict):
    """处理一个 Hermes outbound webhook 事件, 返回 (status_code, body)。"""
    now = time.time()

    if hook_name == "pre_llm_call":
        # 第一轮: 记录标题 + 检测城市指令
        user_msg = str(extra.get("user_message") or "").strip()
        if extra.get("is_first_turn") and user_msg:
            _remember_title(session_id, user_msg)
            city = _city_set_intent(user_msg)
            if city:
                _set_weather_city(city)
                push_event("session.end", status="completed",
                           session_id=session_id,
                           title=f"天气已设为{city}", subtitle="下次刷新天气生效",
                           tone="1", duration_ms=3000)
                return jsonify({"ok": True, "note": f"city->{city}"}), 200
        return jsonify({"ok": True, "note": "title remembered"}), 200

    if hook_name == "on_session_start":
        push_event("session.start", session_id=session_id, title=_get_title(session_id),
                   ts=now)
        return jsonify({"ok": True, "note": "session.start"}), 200

    if hook_name == "on_session_end":
        completed = bool(extra.get("completed"))
        failed = bool(extra.get("failed"))
        interrupted = bool(extra.get("interrupted"))
        reason = str(extra.get("turn_exit_reason") or "")

        # 打断(新任务开始) → 静默
        if interrupted or "interrupted" in reason:
            print(f"[hermes] session {session_id} interrupted — silent")
            return jsonify({"ok": True, "note": "interrupted-silent"}), 200

        if failed:
            ev = {
                "status": "failed",
                "title": _get_title(session_id) or "任务失败",
                "subtitle": "会话执行出错",
            }
        elif completed or not reason:
            ev = {
                "status": "completed",
                "title": _get_title(session_id) or "任务完成",
                "subtitle": "会话已结束",
            }
        else:
            ev = {
                "status": "interrupted",
                "title": _get_title(session_id) or "会话中断",
                "subtitle": "任务被用户打断",
            }
        push_event("session.end", session_id=session_id, ts=now, tone="1",
                   duration_ms=3000, **ev)
        print(f"[hermes] session.end {ev['status']} title={ev['title']}")
        return jsonify({"ok": True, "note": f"session.end-{ev['status']}"}), 200

    if hook_name == "pre_approval_request":
        command = str(extra.get("command") or "")[:60]
        push_event("approval.requested", session_id=session_id, title="需要授权",
                   subtitle=command or "Hermes 请求批准操作", ts=now, tone="1",
                   duration_ms=3000)
        print(f"[hermes] approval.requested cmd={command}")
        return jsonify({"ok": True, "note": "approval.requested"}), 200

    if hook_name == "post_approval_response":
        choice = str(extra.get("choice") or "")
        if choice in ("deny", "timeout"):
            ev = {"status": "denied", "title": "授权被拒",
                  "subtitle": "操作未获批准"}
        else:
            ev = {"status": "approved", "title": "授权通过",
                  "subtitle": "操作已获批准"}
        push_event("approval.decided", session_id=session_id, ts=now, tone="1",
                   duration_ms=3000, **ev)
        print(f"[hermes] approval.decided choice={choice}")
        return jsonify({"ok": True, "note": "approval.decided"}), 200

    # 其他 hook (观察用)
    print(f"[hermes] hook {hook_name} (unhandled)")
    return jsonify({"ok": True, "note": "unhandled"}), 200

# ---------------------------------------------------------------- Flask app
app = Flask(__name__)
CORS(app)  # 允许开发板/浏览器跨域

# 🔧 关键：Flask jsonify 默认 ensure_ascii=True，会把中文序列化成 \uXXXX 转义
# 设备端（Arcs-Mini）用 strstr 直接取原始字节上屏，无法反转义 → 乱码。
# 关闭转义，中文以 UTF-8 原文输出。Flask>=2.2 用 app.json.ensure_ascii，
# 旧版回退 JSON_AS_ASCII 配置。
try:
    app.json.ensure_ascii = False
except Exception:
    try:
        app.config["JSON_AS_ASCII"] = False
    except Exception:
        pass


def _authed():
    token = app.config.get("TOKEN")
    if not token:
        return True
    return request.headers.get("X-Bridge-Token") == token


@app.route("/healthz")
def healthz():
    return jsonify({
        "status": "ok",
        "events_buffered": len(_events),
        "ws_clients": len(_ws_clients),
        "time": _now(),
    })


@app.route("/event", methods=["POST"])
def receive_event():
    """接收 Hermes 生命周期事件。支持两种来源:

    1. 插件 (旧): {"event":"session.end", "status":..., "title":..., ...}
    2. 原生 outbound webhook (新): {"hook_event_name":"on_session_end",
       "session_id":"...", "extra":{...}, ...}

    桥端是唯一智能点: 把 Hermes 的 turn 级事件翻译成设备通知。
    - on_session_end: completed→立即通知; interrupted→静默(新任务打断);
      failed→立即红 ×
    - on_session_start → session.start (设备清除常驻 toast)
    - pre_llm_call → 缓存会话标题 + 检测城市指令
    - pre_approval_request → approval.requested (黄 !)
    - post_approval_response → approval.decided (绿√/红×)
    """
    if not _authed():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}

    # ---- 原生 outbound webhook 格式 ----
    hook_name = data.get("hook_event_name")
    if hook_name:
        sid = str(data.get("session_id") or "")
        extra = data.get("extra") or {}
        if not isinstance(extra, dict):
            extra = {}
        return _handle_hermes_hook(hook_name, sid, extra)

    # ---- 旧插件格式 (保持兼容) ----
    ev_type = data.get("event") or data.get("type") or data.get("event_type")
    if not ev_type:
        return jsonify({"error": "missing 'event' field"}), 400
    fields = {k: v for k, v in data.items() if k not in ("event", "type", "event_type")}
    ev = push_event(ev_type, **fields)
    return jsonify({"ok": True, "event": ev_type, "ts": ev["ts"]}), 200


@app.route("/events", methods=["GET"])
def get_events():
    """获取事件列表（按时间倒序）。?limit=50&since=1787750000"""
    if not _authed():
        return jsonify({"error": "unauthorized"}), 401
    try:
        since = float(request.args.get("since", 0) or 0)
        limit = min(int(request.args.get("limit", 100)), 500)
    except ValueError:
        return jsonify({"error": "bad since/limit"}), 400
    return jsonify({"events": list_events(since, limit, newest_first=True)})


@app.route("/poll", methods=["GET"])
def poll():
    """长轮询：?since=<ts>&timeout=25 —— 设备端最简单的方式。

    有新事件立即返回；没有则挂起最多 timeout 秒。掉线重连后把上次
    收到的最大 ts 传回来即可补拉错过的通知。
    """
    if not _authed():
        return jsonify({"error": "unauthorized"}), 401
    try:
        since = float(request.args.get("since", 0) or 0)
        timeout = min(float(request.args.get("timeout", 25) or 25), 55)
    except ValueError:
        return jsonify({"error": "bad since/timeout"}), 400

    deadline = time.time() + timeout
    while True:
        new = list_events(since)
        if new:
            # 一次只发一个事件（最旧优先 FIFO）：设备端只解析第一个事件，
            # 一次给多个会让它处理最新的、游标跳过其余 → 事件丢失。
            return jsonify({"events": new[:1]})
        if time.time() >= deadline:
            return jsonify({"events": []})
        time.sleep(0.5)


def _ws_handler(ws):
    """WS 消息循环（flask-sock 风格 handler）。"""
    q = queue.Queue(maxsize=256)
    with _client_lock:
        _ws_clients.add(q)
    try:
        ws.send(json.dumps({"event": "connected", "ts": _now()}))
        while True:
            try:
                ev = q.get(timeout=1)
                ws.send(json.dumps(ev, ensure_ascii=False))
            except queue.Empty:
                pass
    except Exception:
        pass
    finally:
        with _client_lock:
            _ws_clients.discard(q)


@app.route("/weather", methods=["GET"])
def get_weather():
    """设备端天气接口：GET /weather?city=杭州 → 精简 JSON。

    城市缺省时读 config.json（默认杭州）。设备无 TLS，由本桥代理 HTTPS
    拉 Open-Meteo 并缓存 10 分钟。同时附带 Asia/Shanghai 本地时间
    (time=HH:MM, wday=0周一..6周日) —— 设备端无需 RTC/时区换算。
    """
    if not _authed():
        return jsonify({"error": "unauthorized"}), 401
    city = (request.args.get("city") or "").strip() or _load_config().get("city", "杭州")
    w = _fetch_weather(city)
    if not w:
        return jsonify({"error": "weather unavailable"}), 502
    try:
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone(timedelta(hours=8)))
        w = dict(w)
        w["time"] = now.strftime("%H:%M")
        w["wday"] = now.weekday()  # 0=周一 .. 6=周日
    except Exception:
        pass
    return jsonify(w)


@app.route("/config", methods=["GET", "POST"])
def bridge_config():
    """城市配置：GET 返回当前; POST {"city":"广州"} 更新并持久化。"""
    if not _authed():
        return jsonify({"error": "unauthorized"}), 401
    if request.method == "GET":
        return jsonify(_load_config())
    data = request.get_json(silent=True) or {}
    new_city = (data.get("city") or "").strip()
    if not new_city:
        return jsonify({"error": "missing 'city'"}), 400
    # 先验证城市可解析，再保存
    geo = _geocode(new_city)
    if not geo:
        return jsonify({"error": f"city '{new_city}' not found"}), 404
    cfg = _load_config()
    cfg["city"] = geo["city"]
    _save_config(cfg)
    return jsonify({"ok": True, "city": geo["city"], "lat": geo["lat"], "lon": geo["lon"]}), 200


def main():
    ap = argparse.ArgumentParser(description="Hermes Event Bridge")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8788)
    ap.add_argument("--token", default="", help="可选：要求每个请求带 X-Bridge-Token")
    args = ap.parse_args()

    app.config["TOKEN"] = args.token

    # 尝试启用 WebSocket（flask-sock）
    ws_enabled = False
    try:
        from flask_sock import Sock
        Sock(app)
        app.add_url_rule("/ws", view_func=_ws_handler, methods=["GET", "POST"])
        ws_enabled = True
    except Exception as e:
        print(f"[bridge] flask-sock 不可用（{e}），仅提供 /poll 长轮询；pip install flask-sock 可启用 WS")

    print(f"[bridge] Hermes Event Bridge 监听 http://{args.host}:{args.port}")
    print(f"[bridge]   POST /event    <- Hermes 插件投递事件")
    print(f"[bridge]   GET  /poll     <- 设备长轮询 (since=<ts>&timeout=25)")
    print(f"[bridge]   GET  /events   <- 查询最近事件")
    if ws_enabled:
        print(f"[bridge]   GET  /ws       <- WebSocket 订阅 (已启用)")
    print(f"[bridge]   GET  /healthz  <- 健康检查")
    if args.token:
        print(f"[bridge] 已启用鉴权，请求需携带 X-Bridge-Token")
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()