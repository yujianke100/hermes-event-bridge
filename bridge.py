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
    """Hermes 插件 POST 事件到这里。支持 JSON 或表单。"""
    if not _authed():
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
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