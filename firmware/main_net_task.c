/*
 * main_net_task.c — Arcs-Mini 事件桥设备端主任务骨架
 *
 * 流程：
 *   1. Wi-Fi 连网（你自己的代码）
 *   2. 启动本任务：长轮询 /poll，解析事件，按类型驱动 屏幕 / LED / 喇叭
 *   3. since 断点：每次成功收到事件后更新，掉线重连不漏
 *
 * 依赖 CSK6 硬件：
 *   - 屏幕 ST7789V 240x240 (SPI, GPIOA_21~27)  —— 已有驱动则复用
 *   - 用户 LED GPIOB_01                             —— led.c 驱动
 *   - 喇叭 8Ω/2W via NS4150B (I2S, GPIOA_01 PA_mute) —— audio.c 驱动
 * 以下 *_ui_* 函数为示例填充，按你的驱动 API 替换实现。
 */

#include <string.h>
#include "bridge_client.h"
#include "cJSON.h"                /* 若无 cJSON，用你工程里任意 JSON 解析 */

/* ---- 提示效果的三个驱动（示例声明，替换为你的驱动头文件） ---- */
void ui_screen_show(const char *line1, const char *line2);   /* 屏幕两行文字 */
void ui_led_flash(int times, int color);                     /* 0=绿 1=红 2=蓝 */
void ui_led_blink_start(void);   /* 授权请求：持续快闪 */
void ui_led_blink_stop(void);
void ui_play_tone(int kind);     /* 0=上行短音 1=低音两下 2=三连音 3=单击 */

static double g_since = 0.0;     /* 事件断点；建议掉电后存 flash，重启续上 */

static void handle_session_end(const cJSON *ev)
{
    const cJSON *st = cJSON_GetObjectItem(ev, "status");
    const cJSON *sid = cJSON_GetObjectItem(ev, "session_id");
    const char *status = st ? (st->valuestring ? st->valuestring : "") : "";
    const char *sess   = sid ? (sid->valuestring ? sid->valuestring : "") : "";

    if (strcmp(status, "completed") == 0) {
        ui_screen_show("OK 任务完成", sess);
        ui_led_flash(2, 0);          /* 绿闪 2 次 */
        ui_play_tone(0);             /* 短促上行音 */
    } else if (strcmp(status, "failed") == 0) {
        ui_screen_show("!! 任务失败", sess);
        ui_led_flash(5, 1);          /* 红闪 5 次 */
        ui_play_tone(1);             /* 低音两声 */
    } else {
        ui_screen_show("- 会话结束 -", status);
        ui_led_flash(1, 2);          /* 蓝闪 1 次 */
        ui_play_tone(3);
    }
}

static void handle_approval(const cJSON *ev, int requested)
{
    const cJSON *desc = cJSON_GetObjectItem(ev, "description");
    const cJSON *cmd  = cJSON_GetObjectItem(ev, "command");
    const char *text  = desc ? (desc->valuestring ? desc->valuestring : "")
                             : (cmd  ? (cmd->valuestring  ? cmd->valuestring  : "") : "");
    if (text && strlen(text) > 10)   /* 屏小，长文本截断 */
        text += 10;

    if (requested) {
        ui_screen_show("需要授权!", text);
        ui_led_blink_start();        /* 持续快闪直到被处理 */
        ui_play_tone(2);             /* 三连音 */
    } else {
        const cJSON *ch = cJSON_GetObjectItem(ev, "choice");
        const char *choice = ch && ch->valuestring ? ch->valuestring : "";
        ui_led_blink_stop();
        if (strcmp(choice, "approved") == 0)
            ui_screen_show("已批准", text);
        else
            ui_screen_show("已拒绝", text);
        ui_led_flash(1, choice[0] == 'a' ? 0 : 1);
        ui_play_tone(3);
    }
}

/* 网络主任务：Wi-Fi 就绪后创建 */
void device_net_task(void *arg)
{
    char json[2048];

    while (1) {
        int rc = bridge_poll(g_since, json, sizeof(json));
        if (rc > 0) {
            /* 有新事件：{"events":[{...}, ...]} */
            cJSON *root = cJSON_Parse(json);
            if (root) {
                cJSON *arr = cJSON_GetObjectItem(root, "events");
                int n = arr ? cJSON_GetArraySize(arr) : 0;
                double max_ts = g_since;
                for (int i = 0; i < n; i++) {
                    cJSON *ev = cJSON_GetArrayItem(arr, i);
                    cJSON *t  = cJSON_GetObjectItem(ev, "ts");
                    if (t && t->valuedouble > max_ts)
                        max_ts = t->valuedouble;

                    const cJSON *kind = cJSON_GetObjectItem(ev, "event");
                    const char *evt = kind && kind->valuestring
                                    ? kind->valuestring : "";
                    if (strcmp(evt, "session.end") == 0)
                        handle_session_end(ev);
                    else if (strcmp(evt, "approval.requested") == 0)
                        handle_approval(ev, 1);
                    else if (strcmp(evt, "approval.decided") == 0)
                        handle_approval(ev, 0);
                }
                g_since = max_ts;   /* 断点前移，下次轮询不漏不重 */
                cJSON_Delete(root);
            }
        } else if (rc < 0) {
            /* 网络错误：退避重试（桥保留 12h 事件，按 since 补拉） */
            /* 可加指示灯提示离线状态 */
            /* k_msleep(5000); */
        }
        /* rc==0 是正常超时，立即下一轮 */
    }
}