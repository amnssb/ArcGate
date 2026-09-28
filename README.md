# ArcGate — 为 Obsidian Arc 设计的 QQ 群退群上报机器人

> **本项目专为 [Obsidian Arc](https://github.com/amnssb/ObsidianArc)（Go + Vue 自部署 AI 聊天站）设计**，对接其 `/api/bot/departure` 机器人 Webhook 协议（协议全文见 ObsidianArc 仓库 `docs/admin/departures.md` 第五节）。它不是通用的 QQ 机器人框架——不做聊天、不做命令，只做一件事：**监听 QQ 群成员退群/被踢 → 上报站点封禁/删除账号并收回邀请奖励 → 把结果播报回群**。

Go 单文件单二进制，纯标准库、零第三方依赖；自带 Web 控制台、离线验收模拟器与容器部署方案。

```text
┌──────────────────────────────────────────────┐
│                    QQ 群                      │
└──────────────────────────┬───────────────────┘
                           │ 1. 群成员退群 / 被踢（OneBot 11 群事件）
                           ▼
┌──────────────────────────────────────────────┐
│          NapCat（OneBot 11 协议实现）          │
│  HTTP 服务器 :3000（供 ArcGate 调用 API）      │
│    send_group_msg / get_group_member_info    │
│    / get_stranger_info                       │
│  HTTP 上报：把群事件 POST 给 ArcGate           │
└──────────────────────────┬───────────────────┘
                           │ 2. HTTP POST http://<arcgate>:3002/onebot/event
                           ▼
┌──────────────────────────────────────────────┐
│   ArcGate：Go 单文件单二进制（纯标准库）        │
│   监听 :3002，过滤退群/被踢事件，查询昵称       │
│   自带 Web 控制台（登录、操作、日志、配置）      │
└──────────────────────────┬───────────────────┘
                           │ 3. POST {OBSIDIAN_ARC_URL}/api/bot/departure
                           │    Authorization: Bearer <token>
                           │    {"qq":"<QQ号字符串>"}（不带 mode）
                           ▼
┌──────────────────────────────────────────────┐
│      Obsidian Arc 站点（Go + Vue 自部署）      │
│  按 users.qq 定位账号，封禁/删除并收回重置卡     │
│      校验令牌，返回 status / cards_revoked    │
└──────────────────────────┬───────────────────┘
                           │ 4. ArcGate 按 status 生成播报文案，
                           │    经 NapCat :3000 send_group_msg
                           ▼
                      播报回 QQ 群
```

## 1. 工作原理

**退群处理链路**（本机器人唯一职责）：

QQ 群成员退出/被踢 → NapCat（OneBot 11）HTTP POST 上报 `notice.group_decrease` 事件 → ArcGate 过滤（`leave`/`kick`、群白名单、QQ 格式校验、15 分钟去重）→ `POST {OBSIDIAN_ARC_URL}/api/bot/departure`（只带 QQ 号，档位由站点后台的默认配置决定，机器人不替管理员做决定）→ 站点按 `users.qq` 找到账号并封禁/删除，同时从邀请奖励记录（`invite_uses`）收回重置卡 → ArcGate 按返回 status 播报回群。

**「入群发卡」与 QQ 入群事件无关**：发卡是站点侧的邀请码机制——老用户生成邀请码，新用户注册时填写，站点落库并给邀请人账户发放重置卡；退群时收回的正是这些卡。QQ 群的 `group_increase`（入群）事件目前**不参与**任何流程，机器人也不监听它。

**幂等与安全**：同一 QQ 重复上报只会得到 `already_departed`，站点保证不会重复收卡——因此网络失败/超时可以放心重试。

## 2. 环境要求

| 部署方式 | 需要 |
| --- | --- |
| 本地二进制 | Go 1.25+（在 Go 1.27 上验证过）；NapCat 与 ArcGate 同机或内网可达 |
| 容器 | Docker 24+ 与 Docker Compose v2（见第 8 节，无需装 Go） |
| QQ 侧 | 一个已登录 NapCat 的 QQ 号作为机器人（NapCat 是 OneBot 11 协议实现，安装见 [napcat.qq.com](https://napcat.qq.com/)） |

## 3. 配置

全部配置走环境变量。本地运行复制 `.env.example` 为 `.env` 填写；容器部署直接注入环境变量（见第 8 节）。

| 变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `OBSIDIAN_ARC_URL` | 是 | 无 | Obsidian Arc 站点基址，不带末尾斜杠，如 `https://arc.example.com` |
| `OBSIDIAN_ARC_BOT_TOKEN` | 是 | 无 | 站点后台「安全 → 注册卡片底部 → QQ 机器人 Webhook 令牌」处获取/设置；不要硬编码、不要提交进仓库 |
| `OBSIDIAN_ARC_WATCH_GROUPS` | 否 | 空 | 要监听的群号，逗号分隔；留空 = 监听所有群 |
| `ARC_LISTEN_ADDR` | 否 | `127.0.0.1:3002` | 接收 NapCat HTTP 上报的监听地址；**容器内必须 `0.0.0.0:3002`** |
| `ARC_ONEBOT_API_URL` | 否 | `http://127.0.0.1:3000` | NapCat HTTP API 地址；compose 内为 `http://napcat:3000` |
| `ARC_ONEBOT_ACCESS_TOKEN` | 否 | 空 | NapCat 两个网络配置里的 Token，两边保持一致；都留空则此处也留空 |
| `ARC_PANEL_USER` | 否 | `admin` | 面板登录账号 |
| `ARC_PANEL_PASSWORD` | 否 | 空 | 面板登录密码（≥8 位）；留空 = 未初始化，首次打开面板时自己设置 |
| `ARC_LOG_LEVEL` | 否 | `info` | 日志级别：`debug` / `info` / `warn` / `error` |

调优项（一般不用动）：`ARC_HTTP_TIMEOUT_MS`(10000)、`ARC_RETRY_BACKOFF_MS`(2000)、`ARC_RATE_LIMIT_WAIT_MS`(5000)、`ARC_DEDUP_TTL_MINUTES`(15)、`ARC_DRAIN_INTERVAL_MS`(30000)、`ARC_DRAIN_GAP_MS`(6000)、`ARC_LOG_DIR`(logs)、`ARC_PENDING_DIR`(data)。

> 进程环境变量优先于 `.env` 文件——`.env` 只补缺，启动前已存在的环境变量不会被 `.env` 覆盖，便于 Docker/systemd 直接注入。

## 4. 状态面板（Web 控制台）

浏览器打开 `http://<ARC_LISTEN_ADDR>/`（默认 <http://127.0.0.1:3002/>），每 5 秒自动刷新：

- **运行**：版本、运行时长、模式、重试队列积压条数；
- **配置**：站点地址、令牌（只显示掩码和长度）、监听地址、OneBot API、群白名单；
- **连通性**：站点 `/api/health` 与 NapCat `/get_version_info` 实时探活（红绿点）；
- **计数**：收到/忽略/去重的事件数、Webhook 成功/失败与自动重试次数、播报成功/失败、处理结果分布；
- **最近事件**：最多 50 条（群号、QQ、状态、播报文案——只有 QQ 侧信息，站点账号数据永不显示）；
- **操作**：模拟退群（走完整链路）、测试播报（直接经 NapCat 发群消息）、立即排水、清空队列、重置统计、查看运行日志；
- **在线改配置**：站点地址、令牌、群白名单、NapCat 地址/令牌、面板账号密码、日志级别，保存写入 `.env`（面板账号密码立即生效，其余重启生效）。

**首次初始化与登录**：`ARC_PANEL_PASSWORD` 留空时，第一次打开面板进入初始化页——自己设置账号（默认 admin）与密码（≥8 位），保存写入 `.env` 并立即生效；之后打开面板先登录（token 有效期 7 天，改密码后所有旧登录态立即失效）。

> 未初始化状态下初始化接口是开放的（与「ObsidianArc 首个注册者成为管理员」同模式）。公网部署请先用反向代理加访问控制，或预置 `ARC_PANEL_PASSWORD`。

## 5. NapCat 配置（逐项对照）

打开 NapCat WebUI → 「网络配置」，新建**两个**配置。缺一个链路就不通：①是 ArcGate 发消息/查昵称的通道，②是 ArcGate 收退群事件的通道。

**① HTTP 服务器（供 ArcGate 调用 API）**

| NapCat 字段 | 填什么 | 说明 |
| --- | --- | --- |
| 启用 | 打开 | 打开后点保存才生效 |
| 名称 | `arcgate-api`（随意） | 仅标识用 |
| Host | 本机部署 `127.0.0.1`；**容器部署 `0.0.0.0`** | 容器内必须监听所有网卡，否则跨容器不可达 |
| Port | `3000` | 与 `ARC_ONEBOT_API_URL` 对应 |
| 启用 CORS | 关 | ArcGate 是服务端对服务端调用，不走浏览器，用不到 |
| 启用 Websocket | **关** | ArcGate 只用 HTTP 模式 |
| 消息格式 | `Array` | ArcGate 发送的就是消息段数组 |
| Token | 例 `q5z5GGd9GmfmCkJV` | 设置后必须同步填到 ArcGate 的 `ARC_ONEBOT_ACCESS_TOKEN` |

**② HTTP 上报（把群事件推给 ArcGate）**

| NapCat 字段 | 填什么 | 说明 |
| --- | --- | --- |
| 启用 | 打开 | |
| 名称 | `arcgate-report`（随意） | |
| URL | 本机 `http://127.0.0.1:3002/onebot/event`；compose 内 `http://arcgate:3002/onebot/event` | 与 `ARC_LISTEN_ADDR` 对应；ArcGate 接收任意路径 |
| 消息格式 | `Array` | |
| Token | 与 ① **同一个** 值 | ArcGate 用同一个 `ARC_ONEBOT_ACCESS_TOKEN` 校验上报与调用 API |

> - NapCat 与 ArcGate 不在同一台机器时，把 `127.0.0.1` 换成对方实际可达的 IP。
> - 验证连通：刷新 ArcGate 面板，「连通性」卡片的 NapCat API 应变绿「在线」；再用面板「测试播报」向测试群发一条消息做端到端确认。

## 6. 本地构建与运行

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

前台运行，`Ctrl+C` 退出。日志同时写控制台与 `logs/arcgate.log`（10MB × 3 轮转），首次运行自动创建 `logs/`、`data/`。

## 7. 容器部署（Docker / Docker Compose）

仓库自带 `Dockerfile`（多阶段构建，产出约 15MB 的 alpine 镜像，非 root 运行）与 `docker-compose.yml`（ArcGate + NapCat 一键起）。**无需在本机装 Go**。

### 7.1 只部署 ArcGate（NapCat 已在别处跑）

```bash
# 构建镜像
docker build -t arcgate .

# 运行（配置全部用环境变量注入）
docker run -d --name arcgate \
  -p 3002:3002 \
  -e OBSIDIAN_ARC_URL=https://arc.example.com \
  -e OBSIDIAN_ARC_BOT_TOKEN=<站点后台的令牌> \
  -e ARC_LISTEN_ADDR=0.0.0.0:3002 \
  -e ARC_ONEBOT_API_URL=http://<napcat主机IP>:3000 \
  -e ARC_ONEBOT_ACCESS_TOKEN=<与 NapCat 一致，未设置则省略> \
  -v "$PWD/logs:/app/logs" -v "$PWD/data:/app/data" \
  arcgate
```

要点：

- **`ARC_LISTEN_ADDR=0.0.0.0:3002` 必须显式设置**——容器里监听 `127.0.0.1` 外部将完全不可达（镜像内已内置该默认值）。
- `-v` 挂载 `logs/`（日志）与 `data/`（重试队列），容器重建后不丢积压事件。
- NapCat 侧的 HTTP 上报 URL 填 `http://<宿主机IP>:3002/onebot/event`。
- 面板暴露在 3002 端口：公网服务器建议只走反向代理 + HTTPS + 访问控制，或用防火墙限制来源。

### 7.2 一键起 ArcGate + NapCat（docker compose）

```bash
# 1. 准备配置
cp .env.example .env    # 至少填 OBSIDIAN_ARC_URL / OBSIDIAN_ARC_BOT_TOKEN
# 2. 启动
docker compose up -d --build
# 3. 扫码登录机器人 QQ（登录完成后可去掉 6099 端口映射）
docker compose logs -f napcat
# 4. NapCat WebUI（:6099）建两个网络配置（见 7.3），打开面板（:3002）初始化账号密码
docker compose logs -f arcgate
```

`docker-compose.yml` 要点（完整文件在仓库根目录）：

- `arcgate` 服务：`env_file: .env` 注入配置，内置 `ARC_LISTEN_ADDR=0.0.0.0:3002`、`ARC_ONEBOT_API_URL=http://napcat:3000`（compose 网络内服务名互访）；
- `napcat` 服务：官方 `mlikiowa/napcat-docker` 镜像，QQ 数据落在 `./napcat/` 卷，登录态重启不丢；
- 两容器同网络，NapCat 的 HTTP 上报 URL 填 `http://arcgate:3002/onebot/event`，ArcGate 的 API 地址填 `http://napcat:3000`。

### 7.3 容器模式下 NapCat 的两个网络配置

与第 5 节唯一的不同——**Host 必须是 `0.0.0.0`**（容器内监听所有网卡）：

- HTTP 服务器：Host `0.0.0.0`，Port `3000`；
- HTTP 上报：URL `http://arcgate:3002/onebot/event`；
- Token 两个配置保持一致，并填入 `.env` 的 `ARC_ONEBOT_ACCESS_TOKEN`。

### 7.4 上线检查清单

- [ ] NapCat 已扫码登录机器人 QQ，两个网络配置都已「启用」
- [ ] 面板「连通性」两颗灯全绿（站点 + NapCat）
- [ ] 面板「测试播报」能在测试群收到消息
- [ ] 面板「模拟退群」注入一个测试 QQ，站点后台用户状态正确变化、退群记录 `src=bot`
- [ ] 面板已初始化账号密码；公网部署已配反向代理/防火墙
- [ ] `OBSIDIAN_ARC_WATCH_GROUPS` 按需收紧（留空 = 所有群都会触发上报）

### 7.5 升级

```bash
git pull && docker compose up -d --build   # 日志/队列在卷里，重启不丢积压事件
```

## 8. 联调与自测

**离线全链路自测**——不需要 QQ 客户端，也不需要真实站点（构建并驱动真实二进制，62 项断言）：

```bash
go run ./tools/mock
```

**向真实站点注入模拟退群事件**：

```bash
./arcgate.exe -simulate <群号> <QQ号> [leave|kick]   # 或面板「模拟退群」
# 例：./arcgate.exe -simulate 123456789 10001 leave
```

- 第三个参数默认 `leave`（主动退群）；`kick` 表示被移出群。
- 被模拟的 QQ 必须真的不在群里，站点才会返回 `departed`（并收回卡片）。
- `-dry-run` 只打印将发出的请求与文案，不发任何 HTTP 请求：`./arcgate.exe -dry-run -simulate ...`。

## 9. 机器人行为

| 站点返回 status | 机器人动作 |
| --- | --- |
| `departed` | 播报「{昵称} 已退群」；若 `cards_revoked > 0` 追加「已收回 N 张重置卡」 |
| `already_departed` | 静默，不播报 |
| `unknown` | 静默，不播报 |
| `refused` | 播报「该账号需管理员在站点后台处理」 |

补充：

- `kick`（被移出群）同样上报，文案为「{昵称} 已被移出群」。
- 昵称只取 QQ 侧数据（群名片 → 昵称 → QQ 号兜底），通过 NapCat 的 `get_group_member_info` / `get_stranger_info` 查询。

## 10. 重试与可靠性

- 站点返回 `429`：等待 5 秒重试（站点限流为令牌桶：突发 10、每 5 秒补充 1 个）。
- 网络错误/超时/5xx：按 2s → 5s → 10s 退避重试，最多 3 次。
- 重试耗尽：事件写入 `data/pending.jsonl`，后台每 30 秒排水重试；站点接口幂等，不会重复收卡。
- 排水持续失败 5 轮：移入 `data/dead.jsonl` 死信，需人工排查。
- 同群同人 15 分钟去重，避免重复上报与重复播报。

## 11. 隐私与安全

- 播报只使用 QQ 侧昵称，**绝不**把站点返回的账号信息（`username` / `user_id` 等）发进群或写进日志。
- 令牌只从环境变量/`.env` 读取；日志对两类令牌与面板密码逐行脱敏。
- `.env`、`logs/`、`data/`、`.e2e/` 均已列入 `.gitignore`；`.dockerignore` 保证密钥不进镜像与构建上下文。
- 面板有登录保护（初始化后），写操作另带来源头校验；公网部署叠加反向代理访问控制。

## 12. 故障排查

| 现象 | 排查 |
| --- | --- |
| 日志出现 `401 bot_unauthorized` | `OBSIDIAN_ARC_BOT_TOKEN` 与站点后台不一致；站点后台接口未启用 |
| 面板 NapCat 显示「离线」 | NapCat 的 HTTP 服务器未启用/端口不对；容器模式下 Host 没填 `0.0.0.0`；`ARC_ONEBOT_API_URL` 地址不对 |
| 群里收不到播报 | NapCat「HTTP 上报」未启用或 URL 不对；`OBSIDIAN_ARC_WATCH_GROUPS` 漏了该群；NapCat 侧 Token 与 `ARC_ONEBOT_ACCESS_TOKEN` 不一致会导致上报被 403 拒收 |
| 容器起不来端口冲突 | `3002` 被占用：改 compose 映射（如 `13002:3002`）并同步 NapCat 上报 URL |
| 日志频繁 `429` | 正常限流，机器人自动退避，无需处理 |
| `data/pending.jsonl` 积压 | 站点曾不可达；恢复后每 30 秒自动排水，无需人工干预 |
| 容器日志中文乱码 | 日志文件为 UTF-8；部分终端需 `chcp 65001` 或用 `docker logs` 直接看 |
