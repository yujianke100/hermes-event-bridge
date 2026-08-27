/*
 * bridge_client.h — Arcs-Mini (CSK6) 事件桥 HTTP 客户端
 *
 * 职责：长轮询 /poll，拿到事件 JSON，返回给上层。
 * 依赖你的工程已具备：
 *   - Wi-Fi STA 已联网
 *   - 一个 HTTP client（lwIP httpc / AT 指令 / CSK6 lisa_http 皆可）
 *   - 底层 socket 收发函数
 */
#ifndef HERMES_BRIDGE_CLIENT_H
#define HERMES_BRIDGE_CLIENT_H

/* ---- 桥配置：改成你自己的部署（或从 lisa_kv / menuconfig 读取）-------- */
#define BRIDGE_HOST     "YOUR_BRIDGE_HOST"   /* 事件桥公网 IP/域名 */
#define BRIDGE_PORT     8788
#define BRIDGE_TOKEN    "YOUR_BRIDGE_TOKEN"  /* 与 bridge.py --token 一致 */
#define BRIDGE_POLL_SEC 25                   /* 长轮询挂起上限（服务器上限 55） */

/*
 * 长轮询一次。返回值：
 *   1   -> 有新事件，json_out 里是 {"events":[...]}（含本次全部新事件）
 *   0   -> 正常超时，无新事件
 *  -1   -> 网络错误（上层应退避重试；桥内存 12h 事件，重连后用 since 补拉）
 */
int bridge_poll(double since, char *json_out, int json_len);

#endif /* HERMES_BRIDGE_CLIENT_H */