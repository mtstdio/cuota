# Cuota Server - Codex 配额管家与 5h 自动激活独立服务

Cuota Server 是面向 [CLIProxyAPI (CPA)](https://github.com/router-for-me/CLIProxyAPI) 用户的轻量级 Codex 账号配额管理服务。它作为 CPA 的独立旁路控制器（Sidecar Controller）运行，彻底解耦代理数据面，通过 CPA Management API 与本地凭证目录实现账号生命周期管理、**Codex 5h 额度自动探测激活**、以及 **Fill-First 智能无缝切换**。

---

## 核心特性

- ⚡ **5h 额度自动探测激活（Probe Ping）**
  - 针对 OpenAI Codex“到期重置后无请求不启动 5h 倒计时”的延迟重置机制（Lazy Reset Window）。
  - 当轮询检测到 5h 额度重置到期或为 0% 且倒计时未开始时，自动向 `https://chatgpt.com/backend-api/codex/responses` 发送极简单轮请求（默认模型 `gpt-5.6-luna`，消耗极微量 Token）。
  - 成功激活后立即启动新一轮 5 小时倒计时时钟，保证下个 5h 额度准时刷新，杜绝额度闲置浪费。

- 🔄 **Fill-First 多账号智能切换流水线**
  - **集中消耗**：主账号处于最高优先级（`priority: 100`），集中承受 CPA 流量。
  - **耗尽熔断**：当主账号 5h 额度用尽（低于阈值，默认 2%）或遇到 429 临时耗尽时，系统自动通过 CPA API 将其标记为停用或降级，进入冷却队列。
  - **平滑递补**：自动从可用池中推举 5h 剩余额度最充足的备用账号晋升为主账号，业务流量不中断。
  - **自动回血**：冷却中账号到达重置点被 Probe 自动激活后，验证重置成功即可重新入列可用池。

- 🔌 **本地目录 + API 双模式同步**
  - 优先读取挂载的本地 `auth-dir`（例如 `~/.cli-proxy-api`）中的 JSON 凭据，直接更新状态并触发写盘。
  - 自动调用 CPA Management API（`/v0/management/auth-files`）热重载生效，兼容本地部署与 Docker 容器化编排。

- 📊 **现代化多 Tab Web 看板与决策审计（参考 Quota-Router）**
  - **单文件二进制内嵌**：基于 Go `embed.FS` 嵌入现代化 React 19 + TypeScript SPA，无外部静态 CDN 依赖。
  - **五大核心 Tab 视图**：
    - **Overview（概览）**：主力账号卡片、5h 倒计时、就绪与冷却统计、全局刷新。
    - **Accounts（账号管理）**：5h 与周配额双进度条、账号侧滑抽屉详情、一键手动 Probe 探测激活、一键切换主力。
    - **Sessions（会话追踪）**：会话亲和与绑定状态展示。
    - **Activity（决策流水）**：完整记录 5h 滑动漂移检测、Probe 激活、主力耗尽熔断等调度决策流水。
    - **Settings（配置中心）**：轮询周期、耗尽阈值、探测模型可视化展示与管理。
  - **双重 API 兼容**：同时支持标准 `/api/*` RESTful 接口与 Quota-Router 插件管理接口。

---

## 快速开始

### 1. 编译构建

要求环境：Go 1.20+

```bash
cd cuota-server
go build -o cuota-server .
```

### 2. 配置文件

复制并修改配置文件：

```bash
cp config.example.yaml config.yaml
```

`config.yaml` 示例：

```yaml
server:
  host: "0.0.0.0"
  port: 8318          # 看板与 API 监听端口
  username: "admin"   # Web 看板与 API 访问账号（默认 admin）
  password: "admin"   # Web 看板与 API 访问密码（默认 admin，两者留空关闭认证）

cpa:
  base_url: "http://127.0.0.1:8317" # CPA 服务的地址
  secret_key: ""                   # CPA management secret key（如有）
  auth_dir: "~/.cli-proxy-api"     # CPA 的 auth 文件目录

quota:
  poll_interval: "60s"             # 配额轮询间隔
  min_remaining_percent: 2.0       # 5h 耗尽阈值百分比
  request_timeout: "15s"

probe:
  enabled: true                    # 开启 5h 自动激活
  model: "gpt-5.6-luna"            # 激活探测模型
  endpoint: "https://chatgpt.com/backend-api/codex/responses"
  timeout: "30s"

scheduler:
  strategy: "fill_first"           # 调度策略
```

### 3. 运行服务

#### 本地二进制直接运行
```bash
./cuota-server -c config.yaml
```

#### Docker 容器运行
```bash
# 直接拉取 GitHub Actions 发布的官方镜像运行
docker run -d \
  --name cuota-server \
  --restart unless-stopped \
  -p 8318:8318 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v ~/.cli-proxy-api:/root/.cli-proxy-api \
  ghcr.io/mtstdio/cuota:latest
```

或使用 Docker Compose：
```bash
docker compose up -d
```

启动后在浏览器打开：`http://localhost:8318` 查看 Web 管理看板（默认账密 `admin` / `admin`）。

---

## REST API 接口

| 接口 | 方法 | 请求体 | 说明 |
| :--- | :--- | :--- | :--- |
| `/api/status` | `GET` | 无 | 获取当前主力账号、所有账号配额倒计时与 Probe 历史日志 |
| `/api/probe` | `POST` | `{"account_id": "codex-1.json"}` | 对指定账号手动触发一次 5h 激活 Probe |
| `/api/switch` | `POST` | `{"account_id": "codex-1.json"}` | 手动将指定账号指定为最高优先级主力账号 |
| `/api/account/toggle` | `POST` | `{"account_id": "...", "disabled": true/false}` | 手动启用或停用指定账号 |
| `/api/refresh` | `POST` | 无 | 手动立即触发全量配额轮询与状态和解 |

---

## 目录结构

```
cuota-server/
├── main.go               # 服务入口与信号控制
├── config/               # 配置文件解析与默认值
├── cpa/                  # CPA 凭证读取与 API 同步客户端
├── quota/                # Codex usage 抓取与 5h/周额度模型
├── probe/                # 5h 自动激活 Probe Ping 核心逻辑
├── scheduler/            # Fill-First 状态机与智能切换调度器
├── web/                  # 嵌入式 Web 看板与 REST API 路由
│   └── static/index.html # 纯单页前端仪表盘
├── config.example.yaml   # 配置模板
├── go.mod                # Go 模块定义
└── README.md             # 使用说明文档
```
