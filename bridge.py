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


def list_events(since_ts: float = None, limit: int = 100):
    """按时间倒序返回事件（供长轮询/补拉）。"""
    with _lock:
        items = list(_events)
    if since_ts is not None:
        items = [e for e in items if e["ts"] > since_ts]
    return list(reversed(items))[:limit]


# ---------------------------------------------------------------- Flask app
app = Flask(__name__)
CORS(app)  # 允许开发板/浏览器跨域


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
    return jsonify({"events": list_events(since, limit)})


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
            return jsonify({"events": new})
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