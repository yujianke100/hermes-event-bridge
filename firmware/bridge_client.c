/*
 * bridge_client.c — Arcs-Mini (CSK6) 事件桥长轮询客户端实现
 *
 * 用最朴素的 BSD socket 直拼 HTTP/1.1 GET（lwIP / CSK6 POSIX socket 均可）。
 * 如果你的 SDK 有更高级的 HTTP 客户端组件，替换 _http_poll() 内部即可，
 * bridge_poll() 的对外语义保持不变。
 */

#include <string.h>
#include <stdio.h>
#include "bridge_client.h"

/* 若你的 SDK 提供标准 socket 头文件则包含；CSK6 可用 <zephyr/posix/netdb.h> 等 */
#include "lwip/sockets.h"    /* 换成你工程实际的 socket 头 */
#include "lwip/netdb.h"

int bridge_poll(double since, char *json_out, int json_len)
{
    char req[512];
    int sock = -1, rc = -1;
    size_t total = 0;

    /* 1. 组 HTTP 请求：GET /poll?since=<ts>&timeout=<s> */
    snprintf(req, sizeof(req),
        "GET /poll?since=%.3f&timeout=%d HTTP/1.1\r\n"
        "Host: %s:%d\r\n"
        "X-Bridge-Token: %s\r\n"
        "Connection: close\r\n"
        "\r\n",
        since, BRIDGE_POLL_SEC, BRIDGE_HOST, BRIDGE_PORT, BRIDGE_TOKEN);

    /* 2. 连接 */
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%d", BRIDGE_PORT);
    if (getaddrinfo(BRIDGE_HOST, port_str, &hints, &res) != 0)
        return -1;

    sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0) {
        freeaddrinfo(res);
        return -1;
    }
    /* 设备端网络可能较慢，给足连接超时 */
    struct timeval tv = { .tv_sec = 15, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if (connect(sock, res->ai_addr, res->ai_addrlen) != 0)
        goto out;
    freeaddrinfo(res);
    res = NULL;

    /* 3. 发送请求 */
    if (send(sock, req, strlen(req), 0) != (int)strlen(req))
        goto out;

    /* 4. 读响应体（跳过 HTTP 头；仅取 JSON 部分，最多 json_len-1 字节） */
    char buf[256];
    int hdr_done = 0;
    while (total + sizeof(buf) - 1 < (size_t)json_len) {
        int n = recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0)
            break;
        buf[n] = '\0';

        if (!hdr_done) {
            /* 找到 "}\r\n\r\n" 或 "\r\n\r\n" 之前的 body 起点 */
            char *body = strstr(buf, "\r\n\r\n");
            if (body) {
                hdr_done = 1;
                body += 4;
                int len = n - (int)(body - buf);
                if (len > 0) {
                    memcpy(json_out + total, body, len);
                    total += len;
                }
            }
            /* 头尚未收完，继续读 */
        } else {
            memcpy(json_out + total, buf, n);
            total += n;
        }
        /* 服务器在空响应（超时）时返回 {"events":[]}，或新事件 JSON */
        if (total > 0 && json_out[total - 1] == '\n')
            break;
    }

    if (total > 0) {
        json_out[total] = '\0';
        /* 空事件列表 => 正常超时 */
        rc = (strstr(json_out, "\"events\":[]") != NULL) ? 0 : 1;
    } else {
        rc = -1;
    }

out:
    if (sock >= 0)
        close(sock);
    if (res)
        freeaddrinfo(res);
    return rc;
}