# 固件骨架说明 — 把事件通知功能写进小设备

本目录是**可移植骨架**，不是完整可编译工程。适配进你的设备 SDK 工程即可
（以 ListenAI Arcs-Mini / CSK6 SDK 为例）。

## 目录结构

```
firmware/
├── bridge_client.h      # 桥客户端接口（长轮询 /poll）
├── bridge_client.c      # HTTP 长轮询实现（socket 直拼 HTTP，含鉴权）
├── main_net_task.c      # 设备端主任务：轮询 → 解析 → 驱动 屏/LED/喇叭
└── micropython/
    └── main.py          # 备用方案：MicroPython 版（若板子刷了 MPY）
```

## 三种接法（按你的实际情况选）

### A. 标准：CSK6 C 工程（推荐，Arcs-Mini 官方 SDK）
1. 把 `bridge_client.c/.h` 拷进你的工程 `src/` 或 `components/`
2. `main_net_task.c` 里的网络头文件换成你 SDK 的（CSK6 用
   `<zephyr/posix/netdb.h>` 或 FreeRTOS socket；也可换成 SDK 的
   `lisa_http` 组件调用 —— SDK 里 `voice_player_tts.c`、`alarm_api.c`
   有 `lisa_http_request_t` 完整示例）
3. 把 `ui_screen_show / ui_led_* / ui_play_tone` 换成你工程里真实的
   屏幕/GPIO/音频驱动调用（Arcs-Mini SDK 现成有 `service_led.h`、
   `tone/` 模块）
4. 在任务创建处（Wi-Fi 连上后）创建 `device_net_task` 任务

### B. MicroPython（若板子刷了 MPY 固件）
1. 拷 `micropython/main.py` 到板子
2. 改 `ui_screen / ui_led / ui_audio` 为你的驱动
3. `import main; main.run()`

### C. 先在 PC 上看效果（零硬件）
```bash
python device_sim.py --bridge http://127.0.0.1:8788 --token <你的token>
```
跑一次 Hermes 会话，模拟器立刻打印事件。

## 关键设计点（移植时别改错）

1. **长轮询**：`GET /poll?since=<上次ts>&timeout=25`，有事件立即返回，
   没有挂起 25s。设备**主动连出**到桥服务，无需公网 IP / 端口映射。
2. **since 断点**：收到事件后取最大 `ts` 存下来（掉电可存 kv/flash）。
   掉线/重启后从断点继续，桥内存 12h 事件，**永不漏事件**。
3. **鉴权**：每个请求带 `X-Bridge-Token` 头；不带会被 401 拒。
4. **事件类型**：`session.end`（status: completed/failed/interrupted）、
   `approval.requested`、`approval.decided`（choice: approved/denied）。
5. **提示映射**：
   - 完成 → 绿闪2次 + 上行音
   - 失败 → 红闪5次 + 低音
   - 需授权 → 持续快闪 + 三连音（直到 decided）
   - 已决定 → 单击 + 单闪（绿=批准/红=拒绝）