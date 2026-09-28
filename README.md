# ArcGate

ArcGate 是 [Obsidian Arc](https://github.com/) 站点（Go + Vue 自部署 AI 聊天站）的 QQ 群退群上报机器人：群里有成员退群或被踢时，NapCat 把事件 HTTP 上报给 ArcGate，ArcGate 过滤出退群事件、调用站点的 `/api/bot/departure` Webhook 记账（收回重置卡等），再按站点返回的 `status` 生成文案、播报回群。整个程序是 Go 单文件单二进制，只依赖标准库。

## 1. 简介

链路一览：

```text
┌──────────────────────────────────────────────┐
│                    QQ 群                      │
└──────────────────────────┬───────────────────┘
                           │ 1. 群成员退群 / 被踢（OneBot 11 群事件）
                           ▼
┌──────────────────────────────────────────────┐
│          NapCat（OneBot 11 协议实现）          │
│                                              │
│  HTTP 服务器 :3000（供 ArcGate 调用 API）      │
│    send_group_msg / get_group_member_info    │
│    / get_stranger_info                       │
│  HTTP 上报：把群事件 POST 给 ArcGate           │
└──────────────────────────┬───────────────────┘
                           │ 2. HTTP POST http://127.0.0.1:3002/onebot/event
                           ▼
┌──────────────────────────────────────────────┐
│   ArcGate：Go 单文件单二进制（纯标准库）        │
│   监听 :3002，过滤退群 / 被踢事件，查询昵称     │
└──────────────────────────┬───────────────────┘
                           │ 3. POST {OBSIDIAN_ARC_URL}/api/bot/departure
                           │    Authorization: Bearer <token>
                           │    {"qq":"<QQ号字符串>"}（不带 mode）
                           ▼
┌──────────────────────────────────────────────┐
│      Obsidian Arc 站点（Go + Vue 自部署）      │
│      校验令牌，返回 status / cards_revoked    │
└──────────────────────────┬───────────────────┘
                           │ 4. ArcGate 按 status 生成播报文案，
                           │    经 NapCat :3000 send_group_msg
                           ▼
                      播报回 QQ 群
```

## 2. 环境要求

- Go 1.25+（在 Go 1.27 上验证过）
- 一台已安装并登录的 NapCat（OneBot 11 协议实现），与 ArcGate 同机部署或内网可达

## 3. 配置

在仓库根目录复制示例配置并填写：

```bat
:: Windows
copy .env.example .env
```

```bash
# Linux / macOS
cp .env.example .env
```

| 变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `OBSIDIAN_ARC_URL` | 是 | 无 | 站点基址，不带末尾斜杠，如 `https://arc.example.com` |
| `OBSIDIAN_ARC_BOT_TOKEN` | 是 | 无 | 在站点后台「安全 → QQ 机器人 Webhook 令牌」处获取；不要硬编码进代码、不要提交进仓库（`.env` 已被 `.gitignore` 忽略） |
| `OBSIDIAN_ARC_WATCH_GROUPS` | 否 | 空 | 要监听的群号，逗号分隔；留空 = 监听所有群 |
| `ARC_LISTEN_ADDR` | 否 | `127.0.0.1:3002` | 接收 NapCat HTTP 上报的监听地址 |
| `ARC_ONEBOT_API_URL` | 否 | `http://127.0.0.1:3000` | NapCat HTTP API 地址 |
| `ARC_ONEBOT_ACCESS_TOKEN` | 否 | 空 | NapCat 侧配置了 access token 就填，两边保持一致 |
| `ARC_LOG_LEVEL` | 否 | `info` | 日志级别：`debug` / `info` / `warn` / `error` |

> 注意：进程环境变量优先于 `.env` 文件——`.env` 只补缺，启动前已 export 的变量不会被 `.env` 覆盖，便于用 Docker、systemd 等方式直接注入配置。

## 4. NapCat 配置

打开 NapCat WebUI → 网络配置，新建以下两项：

**① HTTP 服务器（供 ArcGate 调用 API）**

- 监听端口填 `3000`（与 `ARC_ONEBOT_API_URL` 对应）。
- ArcGate 会调用 `send_group_msg`（播报回群）、`get_group_member_info` / `get_stranger_info`（查昵称）。
- 可选设置 access token；若设置，须与 `ARC_ONEBOT_ACCESS_TOKEN` 一致。

**② HTTP 上报（把群事件推给 ArcGate）**

- 目标 URL 填 `http://127.0.0.1:3002/onebot/event`（与 `ARC_LISTEN_ADDR` 对应）。
- ArcGate 接收任意路径，这里的路径可以自定，与监听地址对得上即可。
- 可选统一 access token，与 HTTP 服务器一侧保持一致，并填入 `ARC_ONEBOT_ACCESS_TOKEN`。

> 若 NapCat 与 ArcGate 不在同一台机器，把 `127.0.0.1` 换成对方实际可达的 IP。

## 5. 构建与运行

在仓库根目录：

```bat
:: Windows
go build -o arcgate.exe .
.\arcgate.exe
```

```bash
# Linux / macOS
go build -o arcgate .
./arcgate
```

前台运行，`Ctrl+C` 退出。日志同时写控制台与 `logs/arcgate.log`，首次运行会自动创建 `logs/`、`data/` 目录。

## 6. 联调与自测

**离线全链路自测**——不需要 QQ 客户端，也不需要真实站点：

```bash
go run ./tools/mock
```

逐场景输出 `PASS` / `FAIL`，全部 `PASS` 即链路正常。

**向真实站点注入模拟退群事件**：

```bash
./arcgate.exe -simulate <群号> <QQ号> [leave|kick]
# 例：./arcgate.exe -simulate 123456789 10001 leave
```

- 第三个参数默认 `leave`（主动退群）；`kick` 表示被移出群。
- 被模拟的 QQ 必须真的不在群里，站点才会返回 `departed`（并收回卡片）。
- 只想看看会发出什么请求与播报文案时，加 `-dry-run`（放在最前）：

```bash
./arcgate.exe -dry-run -simulate 123456789 10001 leave
```

`-dry-run` 只打印将发出的请求与文案，不发任何 HTTP 请求。

## 7. 机器人行为

| 站点返回 status | 机器人动作 |
| --- | --- |
| `departed` | 播报「{昵称} 已退群」；若返回的 `cards_revoked > 0`，追加播报「已收回 N 张重置卡」 |
| `already_departed` | 静默，不播报 |
| `unknown` | 静默，不播报 |
| `refused` | 播报「该账号需管理员在站点后台处理」 |

补充说明：

- `kick`（被移出群）同样会上报，播报文案为「{昵称} 已被移出群」。
- 昵称取 QQ 侧的群名片 / 昵称，通过 NapCat 的 `get_group_member_info` / `get_stranger_info` 查询。

## 8. 重试与可靠性

- 站点返回 `429`：等待 5 秒后重试。
- 网络错误：按 2s → 5s → 10s 退避重试，最多 3 次。
- 重试耗尽：请求体写入 `data/pending.jsonl`，后台每 30 秒排水重试一次；站点接口幂等，不会重复收回卡片。
- 排水持续失败 5 轮：移入 `data/dead.jsonl`，不再自动重试，需人工排查后处理。
- 同群同人 15 分钟内去重，避免重复上报与重复播报。

## 9. 隐私与安全

- 播报内容只使用 QQ 侧昵称，绝不把站点返回的账号信息（`username` / `user_id` 等）发进群里。
- 令牌只从环境变量 / `.env` 读取；日志输出对令牌做了脱敏。
- 请勿把 `.env`、`logs/`、`data/` 提交进仓库，三者均已列入 `.gitignore`。

## 10. 故障排查

| 现象 | 排查 |
| --- | --- |
| 日志出现 `401 bot_unauthorized` | 检查 `OBSIDIAN_ARC_BOT_TOKEN` 是否与站点后台一致；站点后台的 QQ 机器人 Webhook 接口开关是否打开 |
| 群里收不到播报 | 检查 NapCat 的两个网络配置（HTTP 服务器 :3000、HTTP 上报指向 :3002）是否都启用且地址正确；检查 `OBSIDIAN_ARC_WATCH_GROUPS` 是否漏掉了该群 |
| 日志频繁出现 `429` | 属正常限流（站点令牌桶：突发 10、每 5 秒补充 1 个），机器人会自动退避，无需处理 |
| `data/pending.jsonl` 有积压 | 说明站点曾不可达；站点恢复后每 30 秒自动排水，无需人工干预 |
