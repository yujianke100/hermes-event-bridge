#!/usr/bin/env python3
"""设备端客户端模拟器：长轮询订阅 Hermes Event Bridge 事件并打印。

用法:  python device_sim.py [--bridge http://127.0.0.1:8788] [--token x]
"""
import argparse
import json
import time
import urllib.request

def poll_once(bridge, since, token, timeout=25):
    url = f"{bridge}/poll?since={since}&timeout={timeout}"
    req = urllib.request.Request(url)
    if token:
        req.add_header("X-Bridge-Token", token)
    with urllib.request.urlopen(req, timeout=timeout + 5) as r:
        return json.loads(r.read().decode())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bridge", default="http://127.0.0.1:8788")
    ap.add_argument("--token", default="")
    args = ap.parse_args()

    since = 0.0
    print(f"[device-sim] 连接 {args.bridge}，长轮询订阅中... (Ctrl+C 退出)")
    while True:
        try:
            data = poll_once(args.bridge, since, args.token)
            for ev in data.get("events", []):
                ts = ev.get("ts", 0)
                if ts > since:
                    since = ts
                kind = ev.get("event")
                if kind == "session.end":
                    print(f"\n>>> [完成] session {ev.get('session_id')} | {ev.get('title')} | status={ev.get('status', '?')}")
                elif kind == "approval.requested":
                    print(f"\n>>> [授权请求] {ev.get('description', ev.get('command', '?'))}")
                elif kind == "approval.decided":
                    print(f"\n>>> [授权结果] choice={ev.get('choice')} | {ev.get('description', '')}")
                else:
                    print(f"\n>>> [{kind}] {json.dumps(ev, ensure_ascii=False)}")
        except Exception as e:
            print(f"[device-sim] 轮询错误: {e}，2s 后重试")
            time.sleep(2)

if __name__ == "__main__":
    main()