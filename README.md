# Hermes Event Bridge — 让随身小设备"看见"Hermes

当 **任意环境的 Hermes Studio** 完成一个 session 任务、或需要你授权时，
通过本系统把事件推送到你的小设备（屏幕 / LED / 喇叭提示）。

```
[任意 Hermes Studio]            [事件桥 bridge.py]                [小设备]
  ┌─────────────────┐   POST    ┌────────────────────┐  长轮询   ┌──────────────┐
  │ 原生 outbound    │──/event──▶│ 事件队列 (内存)     │◀──/poll──▶│ Wi-Fi + 屏幕  │
  │ webhook (config) │           │ TTL 12h, 上限2000  │           │ LED + 喇叭    │
  │ hooks.outbound  │           │ 可选 X-Bridge-Token│           │ 时钟+天气     │
  └─────────────────┘           └────────────────────┘           └──────────────┘
```

**核心设计**：Hermes 永远是**主动外发**的那一侧（原生 outbound webhook，零代码、纯配置），
设备/任何机器**主动连出**到桥服务。因此桥只要一台有公网 IP 的服务器即可，
Hermes 跑在内网/远程机也毫无影响——不需要任何入站连到 Hermes 的端口。

**ALL 智能都在桥端**：Hermes 只负责把生命周期 hook 原始 JSON 喷到桥，
桥负责翻译成设备通知（会话标题、城市指令、"新任务打断旧任务不响"等）。

---

## 1. 事件桥服务

轻量 Flask 服务，内存事件队列，支持长轮询 / WebSocket。

**运行**（任意有公网 IP 的服务器，或内网机器）：

```bash
pip install flask flask-cors flask-sock
python bridge.py --host 0.0.0.0 --port 8788 --token <你的token>
```

**生产部署**（systemd，随附 `hermes-event-bridge.service` 模板）：

```bash
# 上传 bridge.py 到 /opt/hermes-event-bridge/，建 venv 装依赖
python3 -m venv venv && ./venv/bin/pip install flask flask-cors flask-sock
# 编辑 .service 文件里的 --token，放入 systemd 目录
sudo cp hermes-event-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now hermes-event-bridge
# 云服务器记得在防火墙放行端口（ufw allow 8788/tcp）
```

**API 速查**：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/event` | Hermes 插件投递事件（body: `{"event":"...", ...}`） |
| GET | `/poll?since=&timeout=` | 设备长轮询（推荐） |
| GET | `/events?since=&limit=` | 查询/补拉事件（调试用） |
| GET | `/ws` | WebSocket 订阅（可选，需 flask-sock） |
| GET | `/healthz` | 健康检查（无需鉴权） |

除 `/healthz` 外都要求 `X-Bridge-Token` 请求头（以 `--token` 启动时）。

---

## 2. Hermes 侧 — 原生 outbound webhook（零代码）

**不需要插件了**。Hermes 0.20.x 内置出站 webhook（`agent/outbound_webhooks.py`），
启动时自动从 `config.yaml` 注册。只要在 Hermes 的 `~/.hermes/config.yaml` 顶部加：

```yaml
hooks:
  outbound:
    - url: http://<你的服务器>:8788/event
      events:
        - on_session_start
        - on_session_end
        - pre_llm_call
        - pre_approval_request
        - post_approval_response
      timeout: 10
      name: arcs-mini
```

然后**重启 Hermes Studio / 网关**。`gateway/run.py` 启动时调用
`register_outbound_webhooks()`，纯配置即生效。若你的 Hermes 里启用了旧的
`hermes-event-bridge` 插件，记得禁用（`plugins.enabled: []`），避免双发。

**收到的事件 POST 格式**（`hook_event_name` + `session_id` + `extra`）：

```json
{
  "hook_event_name": "on_session_end",
  "session_id": "sess_abc123",
  "cwd": "/home/user/project",
  "extra": {"completed": true, "failed": false, "interrupted": false, "turn_exit_reason": "text_response(stop)"},
  "delivery_id": "3f2c...",
  "timestamp": "2026-09-01T12:00:00Z"
}
```

可选 `secret_env`/`secret` 开启 HMAC-SHA256 签名（`X-Hermes-Signature-256`），
防止伪造事件。

**桥端智能判定**（`_handle_hermes_hook`）：

| Hermes hook | 桥端行为 | 设备看到 |
|---|---|---|
| `pre_llm_call` (首轮) | 记住标题; 检测"把天气城市设为XX"→ 改城市 | 无（或完成通知"天气已设"） |
| `on_session_start` | 转发 session.start | 清除常驻 toast |
| `on_session_end` completed | **立即**通知（零延迟） | 绿 √ + 标题 |
| `on_session_end` interrupted | **静默**（新任务打断旧 turn，不打扰） | 无 |
| `on_session_end` failed | 立即通知 | 红 × |
| `pre_approval_request` | 转发 | 黄 ! + 命令 |
| `post_approval_response` | 转发 | 授权通过绿√ / 被拒红× |

> 旧版插件（`plugin/` 目录）保留作为参考/回退，但方案 A 之后不再需要。

---

## 3. 设备端接入

### 通信协议 —— 最简单的长轮询

```
GET /poll?since=<上次收到的事件ts>&timeout=25
Header: X-Bridge-Token: <token>
```

- 有新事件 → 立刻返回 `{"events":[{...}]}`
- 没新事件 → 挂起最多 `timeout` 秒（默认25，上限55），之后返回空列表
- 收到事件后，把其中最大的 `ts` 存起来作为下一次的 `since`，**断线重连不漏事件**

事件 JSON 示例：

```json
{
  "event": "session.end",
  "session_id": "20260827_181903_74b396",
  "status": "completed",
  "model": "deepseek-v4-flash",
  "platform": "cli",
  "ts": 1787825948.48
}
```

### 提示行为建议

| 事件 | 屏幕显示 | LED | 喇叭 |
|---|---|---|---|
| `session.end` status=completed | 「✅ 任务完成」 | 绿闪 2 次 | 短促上行音 |
| `session.end` status=failed | 「❌ 任务失败」 | 红闪 5 次 | 低音两声 |
| `approval.requested` | 「⚠️ 需要授权」+ 命令描述 | 快闪（持续） | 三连音 |
| `approval.decided` | 「👌 已批准/❌ 已拒绝」 | 单闪 | 单击 |

### 固件骨架（`firmware/`）

- `bridge_client.c/.h` — 长轮询客户端（HTTP GET /poll，含鉴权）
- `main_net_task.c` — 主任务骨架：轮询 → 解析 → 驱动 屏/LED/喇叭
- `micropython/main.py` — MicroPython 备用方案

把 `BRIDGE_HOST` / `BRIDGE_TOKEN` 宏改成你的部署即可。

### PC 模拟器（零硬件先看效果）

```bash
python device_sim.py --bridge http://127.0.0.1:8788 --token <token>
```

---

## 4. 项目文件

```
hermes-event-bridge/
├── bridge.py            # 事件桥服务（Flask）
├── device_sim.py        # 设备模拟器（长轮询）
├── plugin/
│   ├── plugin.yaml      # 插件清单（hooks 声明）
│   ├── register.py      # 插件注册逻辑（0.17.x 兼容）
│   ├── __init__.py
│   └── test_plugin.py   # 离线自测
├── firmware/
│   ├── bridge_client.h  # 设备侧长轮询客户端（C）
│   ├── bridge_client.c
│   ├── main_net_task.c  # 设备主任务骨架
│   ├── micropython/main.py
│   └── README.md
├── hermes-event-bridge.service   # systemd unit 模板
└── README.md
```