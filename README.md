<p>
  <img src="assets/readme/hero.svg" width="100%" alt="Harbor：排队式 AI 网关——上游忙时请求留在队列里等泊位，而不是立刻收到 429">
</p>

**Harbor 是一个不急着回 429 的 OpenAI 兼容网关。**

上游忙的时候，它把请求留在队列里等一个空出来的泊位，连接不断、请求体不丢；上游报错的时候，它只回统一格式的错误，不透传上游原文。客户端还是官方 OpenAI SDK，只改一个 `base_url`。

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8080/v1", api_key="你的 Harbor Key")
client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": "hi"}])
```

## 同样是等，区别在谁等

<img src="assets/readme/queue-vs-429.svg" width="100%" alt="A：网关立刻回 429，客户端自己退避重试；B：Harbor 让请求排队 8.4 秒，期间每 15 秒一次 SSE 注释心跳，拿到泊位后返回 200">

直接回 429 的网关，把退避、重排、重试风暴全丢给客户端。Harbor 把这段等待接过来：请求在进程内 FIFO 排队，同时拿到并发、RPM、Token 三档额度才发往上游，期间 HTTP 连接一直挂着。

排队不是无限等——超过 `queue.max_wait` 或队列已满，仍然收敛成一个带 `Retry-After` 的 429。

## 60 秒跑起来

Go 1.25+，纯 Go 无 cgo。

```powershell
go build -o harbor.exe .
.\harbor.exe --config harbor.yaml
```

第一次跑，先复制一份配置再把上游 Key 换成环境变量：`Copy-Item config.example.yaml harbor.yaml`。最小可跑配置：

```yaml
server:
  listen: "0.0.0.0:8080"

queue:
  max_wait: 120s      # 排队上限；超过 → 429 + Retry-After
  max_depth: 200      # 队列容量；满 → 立即 429

providers:
  - name: openai
    type: openai-compatible
    base_url: https://api.openai.com/v1
    concurrency: 2    # 上游账号并发
    rpm: 60
    tpm: 1000000      # Token 三档：/分钟 /小时 /天，<=0 不限
    tph: 60000000
    tpd: 1000000000
    credentials: ["${OPENAI_API_KEY}"]

routes:
  gpt-4o: { providers: [openai] }

keys:
  - secret: "hr-live-demo-00000000"
    allowed_models: [gpt-4o]

admin:
  token: ""           # 留空 = 管理接口关闭
```

确认进程起来了（`/v1/models` 要带 Harbor Key）：

```powershell
curl.exe -s http://127.0.0.1:8080/v1/models -H "Authorization: Bearer hr-live-demo-00000000"
```

`/` 是内置落地页，`/admin` 是管理台。

## 它是怎么工作的

<img src="assets/readme/architecture.svg" width="100%" alt="请求管线：客户端 → Key 鉴权 → 并发/RPM/Token 三档额度 → FIFO 队列 → 执行与 failover → 上游；上游失败时结算本次尝试并把原请求放回队尾">

1. **鉴权**：Harbor Key 只存 sha256；模型白名单挂在 Key 上，额度挂在 Provider 上。
2. **三档额度**：Provider 级并发 Semaphore、RPM 滑动窗口、Token Gate（`tpm` / `tph` / `tpd`，预扣 + 按可信 usage 结算）。
3. **FIFO 排队**：三档全部拿到才放行。事件驱动、不轮询；等待期间原 HTTP 请求挂起，不是 202 + 客户端轮询。
4. **执行与 failover**：429/529、5xx、连接错误 → 结算本次 attempt、释放槽位、原请求放回队尾；401/403 禁用该凭证。
5. **错误收敛**：对外永远只有 `{"error":{"message","type","code"}}`，429 带 `Retry-After`；上游原文只留在管理台审计里。

队头阻塞是刻意保留的：拿不到额度的请求不会被后来者越过。想更快就加额度或加上游凭证，而不是让队列插队。

## 能力清单

| 分组 | 能力 |
| --- | --- |
| 协议 | `POST /v1/chat/completions` 直通；`/v1/responses`、`/v1/messages` 桥接到 chat/completions；`/v1/embeddings`；`GET /v1/models`（带上下文与能力元数据） |
| 鉴权 | Harbor Key（sha256）· 模型白名单 · 封禁 / 解封 |
| 额度 | Provider 级 `concurrency` / `rpm` / `tpm` / `tph` / `tpd`，≤0 表示不限 |
| 排队 | 内存 FIFO · 事件驱动调度 · 挂起原请求 · `X-Queue-Position` · 超时/队满 → 429 |
| 容错 | 多凭证 / 多 Provider failover · 冷却 · `Retry-After`（整数秒 + HTTP 日期）· 重试总预算 |
| 流式 | SSE · 空闲心跳保活 · `X-Accel-Buffering: no` · 客户端断开即释放 |
| 可观测 | `X-Request-Id` 贯穿 · 请求审计落库 · 管理台七页 · p50/p95 等待 |
| 持久化 | SQLite 可选 · 凭证 AES-256-GCM · 运行设置热改落库 |
| 部署 | 单二进制 / Dockerfile / Zeabur / Fly.io / 反向代理 |

## 管理台

`/admin`：概览 / 队列 / 请求 / 密钥 / 上游 / 模型 / 设置。同源、无 CDN、无外部字体，CSP 为 `default-src 'self'`，不写行内样式；token 只存 `sessionStorage`，不进 URL、不落 Cookie。

设置页的热改项立即生效，写进 `settings` 表的 `runtime` 键，重启后由 `ApplyStoredSettings` 重放（存储优先于 yaml）：

| 分组 | 字段 | 生效对象 |
| --- | --- | --- |
| 排队 | 最长等待 / 队列容量 / 返回排队位置 | 调度器 `SetLimits` |
| 超时与重试 | 请求总超时 / 最大尝试 / 退避基数 / 冷却 | 请求链路与执行器 `SetPolicy` |
| 心跳保活 | 开关 / 间隔 / 形式 / 注释 | 排队与流式转发 |

只读项（监听地址、请求体上限、读写/空闲超时、在途预算、SQLite 与密钥路径、日志、admin token）和白名单环境变量（`PORT`、`HARBOR_CONFIG`、`HARBOR_DB_PATH`、`HARBOR_KEY_PATH`、`HARBOR_KEY`、`HARBOR_ADMIN_TOKEN`、`OPENAI_API_KEY`、`FLY_*`）只做展示；敏感值只显示「已设置」。

<details>
<summary>模型页：两个开关与 <code>/v1/models</code> 的可见性规则</summary>

每条上游模型是一条声明，两个开关 + 一组元数据，互不影响：

| 字段 | 含义 |
| --- | --- |
| `enabled` | 关掉后真的不参与路由（自动路由与显式映射都跳过），元数据保留，可随时开回 |
| `visible` | 关掉后只是不单独出现在 `/v1/models`，仍可按名调用、也可被映射引用 |
| `context` / `capabilities` | 展示用元数据（上下文窗口、能力标签），不参与调度 |

对外规则：统一模型名（路由 key）全部可见；被任一映射当作 `upstream_model` 的上游名不再单独暴露；其余「启用且可见」的模型同名直通；最后按 Key 的 `allowed_models` 过滤。上游返回但本地没声明的模型只出现在管理台「未声明」筛选里，不会对客户端可见。

</details>

<details>
<summary>模型元数据：从 models.dev 补齐上下文窗口与能力</summary>

「模型元数据」调用 `GET /api/model-catalog?ids=...`，服务端拉取 `https://models.dev/api.json` 并缓存 6 小时，按「精确 → 去 `:tag` → 去 `provider/` 前缀 → 忽略大小写 → 宽松前缀」匹配已声明模型；可逐个或全部应用，能力是合并而不是覆盖。

`/v1/models` 会下发 `context_window`、`context_length`、`capabilities`（如 `["chat","vision","reasoning","tools"]`）；没有元数据的模型只返回 id 等基础字段。落地页「快速接入」里的示例地址按当前访问域名（或 IP）生成，不写死部署域名。

</details>

<details>
<summary>请求页：审计字段与筛选</summary>

`GET /api/requests` 支持 `q`、`status`、`provider`、`model`、`key_id`、`since`、`until`（RFC3339 或 `YYYY-MM-DD`）、`limit`、`offset`。

审计在原有字段上补了 `client_ua`、`client_ip`、`upstream_model`、`first_token_ms`（流式首帧耗时）；旧库由 `migrateRequestsDetail` 补列，历史行这四个字段为空。日期筛选按 `substr(finished_at,1,10)` 做日粒度比较，避开 SQLite 对带时区字符串的解析问题。

</details>

## 管理 API

除 `GET /api/public/status` 外都需要 `Authorization: Bearer <admin.token>`；未配置 token 时全部关闭。

| 资源 | 端点 |
| --- | --- |
| 概览 | `GET /api/stats`、`GET /api/queue` |
| 请求 | `GET /api/requests`、`GET /api/requests/{id}`（内存近期 + SQLite 回退） |
| 密钥 | `GET` / `POST /api/keys`、`PUT` / `DELETE /api/keys/{id}`、`POST /api/keys/{id}/ban`、`POST /api/keys/{id}/unban` |
| 上游 | `GET` / `POST /api/providers`、`PUT` / `DELETE /api/providers/{name}`、`POST /api/providers/probe-models` |
| 凭证 | `POST /api/providers/{name}/credentials`、`DELETE /api/providers/{name}/credentials/{credID}` |
| 模型 | `GET` / `PUT /api/providers/{name}/models`、`POST /api/providers/{name}/models/probe`、`POST /api/model-map`、`GET /api/model-catalog` |
| 路由 | `GET` / `POST /api/routes`、`DELETE /api/routes/{model}` |
| 设置 | `GET` / `PUT /api/settings`、`PUT /api/settings/keepalive`（旧接口，保留兼容） |

`GET /api/stats` 给今日请求 / 成功 / 失败、队列深度、p50/p95 等待与 token 用量；`POST /api/providers/probe-models` 用 `base_url` + 凭证试拉 `/models`，新建上游前先看有哪些模型；`POST /api/providers/{name}/models/probe` 用已存凭证探测，用来比对「未声明」。

```powershell
curl.exe -s http://127.0.0.1:8080/api/stats -H "Authorization: Bearer <admin.token>"
```

## 配置

完整示例见 [config.example.yaml](config.example.yaml)。三个容易踩的点：

**`${ENV}` 全文展开** —— 整个配置文件做文本级替换，监听端口、上游凭证、Harbor Key、admin token、存储路径都能注入：

```yaml
server:
  listen: "0.0.0.0:${PORT}"
storage:
  db_path: ${HARBOR_DB_PATH}
  key_path: ${HARBOR_KEY_PATH}
```

引用了没设置的环境变量会直接报错并给出变量名，不会静默带空值启动。也可以用 `HARBOR_CONFIG` 注入完整 YAML 文本（优先级高于配置文件），适合配置不进仓库的 PaaS 场景。

**纯内存模式** —— 不配 `storage`（或 `db_path` 留空）就没有库和密钥文件，行为不变，重启后统计与记录丢失。

**密钥备份** —— `harbor.key` 是凭证加密主密钥，首次启动自动生成（32 字节、0600），丢失等于已落库的上游凭证和管理台 Key 永久报废。它必须和 `harbor.db` 一起备份、一起迁移，`key_path` 要始终指向同一个密钥。

<details>
<summary>旧版本自动迁移</summary>

启动时 `Migrate` 自动处理旧库：早期 `provider_credentials` 表缺 `secret_fingerprint` 列会重建；旧版 `routes` 的 `[]string` 格式会被识别并重建，当前 `[]ModelMapping` 路由重复启动不会被删。旧版本中只保存了哈希的管理台 Key 无法恢复 secret，需要在管理台重新创建。

</details>

## 排队早退 429

`queue.early_429_after`（默认关闭，示例配置 30s）：排队超过该时长仍未拿到上游额度，直接返回
**429 + Retry-After: 2**，不让客户端干等到自己的超时。早退阈值之前不提交 SSE，所以状态码是干净的 429；
阈值之后才用注释帧保活。设置页「排队」组可热改（`early_429_ms`，0 = 关闭）。

## 停机与部署

`kill_timeout = 90`（fly.toml）+ 进程内 60 秒排水：收到 SIGTERM 后先停止接收新连接，
让排队与流式请求在窗口内自然完成；只有超过窗口的请求才会被收敛成 **503 `server_restarting`**
（带 `Retry-After: 2`），并在退出前留 2 秒把响应写回，避免客户端看到"连接被重置 / 网络连接失败"。

部署会替换机器，正在排队的请求会受排水窗口保护；客户端仍应对 503 做退避重试。
## 心跳保活

长连接最怕静默：排队等上游、或上游迟迟不吐第一个 token 时，连接上没有字节流动，中间代理和部分客户端会按空闲超时直接掐断。Harbor 空闲超过阈值就主动补一个空事件。

```yaml
keepalive:
  enabled: true       # 默认 true
  interval: 15s       # 空闲阈值，1s–5m
  style: comment      # comment = SSE 注释行（SDK 忽略）；event = data 事件
  comment: keepalive  # comment 文本，最长 64 字符
```

`comment` 写 `: keepalive\n\n`，OpenAI 兼容 SDK 当注释忽略，最安全；`event` 写 `data: {"type":"keepalive"}\n\n`，适合要显式收到事件的自研客户端。排队与流式转发共用一套配置，非流式不涉及；没配 `keepalive.interval` 时沿用旧字段 `queue.keepalive`。

**只在事件边界发心跳**：转发阶段会记录已写出的最后 4 个字节，只有上一条 SSE 事件完整结束（`\n\n` / `\r\n\r\n`）或尚未写出任何字节时才插入心跳；否则 200ms 后重试。上游把一条 `data` 拆成多次写时，中途插注释会把 JSON 截断，这是历史上的流式失效根因。

## 部署

<details>
<summary>Zeabur（内置 Dockerfile + harbor.yaml）</summary>

1. 仓库推送到 GitHub，Zeabur 新建项目 → 添加服务 → 选仓库，自动识别 Dockerfile。
2. 必须挂持久卷到 `/data`：PaaS 磁盘重启即清空，不挂卷请求记录和凭证都会丢。
3. 环境变量：`OPENAI_API_KEY`、`HARBOR_KEY`、`HARBOR_ADMIN_TOKEN`、`HARBOR_DB_PATH=/data/harbor.db`、`HARBOR_KEY_PATH=/data/harbor.key`（`PORT` 由 Zeabur 注入）。
4. 绑定域名后，客户端 `base_url` 填 `https://<域名>/v1`。

```powershell
npx zeabur@latest deploy --project-id <project-id> --json
# 之后代码更新复用它，避免生成重复服务
npx zeabur@latest deploy --project-id <project-id> --service-id <service-id> --json
```

Caddy 反代默认不缓冲 SSE，可直接用；排队挂起时间仍受 `queue.max_wait` 控制。

</details>

<details>
<summary>Fly.io（内置 fly.toml）</summary>

```powershell
fly apps create <app> -o <org>
fly volumes create harbor_data -r nrt -s 1 -a <app> --yes
fly secrets set -a <app> OPENAI_API_KEY=... HARBOR_KEY=... HARBOR_ADMIN_TOKEN=...
fly deploy -a <app> --ha=false
```

- `internal_port = 8080` 必须与 `[env] PORT` 一致；进程监听 `0.0.0.0`。
- `[mounts]` 挂 `/data`，`HARBOR_DB_PATH` / `HARBOR_KEY_PATH` 指向卷内；Fly 会把卷属主改成镜像里的 `harbor`，非 root 可写。
- `auto_stop_machines = "off"`，别让停机打断排队中的请求；SSE 由 Fly 直接透传。
- SQLite 只能单机单卷（`--ha=false`）；多实例要自己做亲和或换托管库。
- `sk-REPLACE_ME` 这类占位符不会写入库（启动日志 `skip placeholder provider`），要在管理台 `POST /api/providers/{name}/credentials` 补真 Key。
- 运行时权威是 SQLite：`harbor.yaml` 只在该域为空时做种子，改 yaml 不会覆盖已有 Provider / Key / 路由。

</details>

<details>
<summary>反向代理（Nginx / Caddy / 网关）⚠️</summary>

排队挂起 + SSE 要求代理不能设短超时，否则代理先掐断连接，Harbor 的排队等待就失去意义：

```nginx
location /v1/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_read_timeout 600s;   # 至少覆盖 limits.request_timeout
    proxy_send_timeout 600s;
    proxy_buffering off;       # SSE 必须关闭缓冲
    chunked_transfer_encoding on;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

代理超时要覆盖 `limits.request_timeout` 并留足余量——这个总超时已经包含排队，不能把排队和执行各算一份预算。心跳只是保活，不代表上游已开始生成，也突破不了代理的硬性总超时。

</details>

## 上游失败之后：重排与重试

Harbor 按 **Provider** 共享并发、RPM、Token Gate——所有 Key 和模型只要使用同一个 Provider 就共用额度。SQLite 启用时，请在管理台核对运行时额度，不要只改作为空库种子的 YAML。

1. 校验通过后进全局 FIFO，拿到可用凭证和全部额度才发往上游。
2. 可恢复的 429/529、5xx、连接错误：关闭本次上游响应、结算该 attempt、释放槽位，把**原请求放回队尾**；期间保留 HTTP 连接、请求体和 Request ID，不向客户端发送中间失败。
3. `Retry-After` 支持整数秒与 HTTP 日期；凭证恢复时间取「上游期限 / 配置 cooldown / 已有期限」中最晚的那个，不缩短冷却。请求另受指数 backoff 控制；有备用 Provider 时不必等失败凭证的全部冷却。
4. 唤醒或额度释放后重新申请全部 Gate，每次实际发送都计入 RPM；失败且没有可信 usage 时保留预扣 Token，不把未知消耗误退成 0。
5. `max_provider_attempts` 是**整个请求的累计发送上限**，重排不重置；`queue.max_wait` 从首次入队计时，`request_timeout` 覆盖读取 + 全部等待 + 执行。任一预算耗尽、或重排时队列已满，就返回最终收敛错误，不无限重试。
6. 400/404 等不可恢复错误直接失败；结构化 `insufficient_quota` 不做短周期重试；401/403 禁用该凭证，只在还有其他可用凭证时切换。已经交付业务响应的流绝不重放。

`running` 包含仍在转发的 SSE，流结束才释放槽位；排队耗时累计各轮真实等待，不含上游执行时间；一次请求无论重试几次只记一次完成审计，`attempts` 是累计次数。

```powershell
go test ./internal/queue ./internal/limits ./internal/server -run 'TestRequeue|TestTokenWake|TestConcurrencyEnable|TestQueueWait|TestRunningIncludes' -count=1
```

## Responses 协议桥（SUB2API）

链路：客户端 Chat Completions → SUB2API 转 Responses → Harbor `/v1/responses` → 上游 Chat Completions。

- 转换扁平 `tools` / `tool_choice`、`text.format`、`reasoning.effort`，合并同一 assistant 回合的并行工具调用，保留工具结果与可读 reasoning summary。
- 流式请求向上游要 `stream_options.include_usage=true`，在 `finish_reason` 后继续读 usage 尾帧再生成终止事件；文本、工具参数、推理摘要都带稳定 item/index 与递增 `sequence_number`。
- 已提交 HTTP 200 后上游失败，发 `response.failed`——不能再靠状态码判断成功；截断流与转换失败记为错误，不伪造 `response.completed`。
- 这是**无状态转换**，不是完整 Responses 服务：内置 web/file/computer 工具、`previous_response_id`、`conversation`、存储 prompt、`item_reference` 都不支持；不支持的功能直接拒绝，不静默丢上下文。

```powershell
go test ./internal/compat ./internal/server -run 'TestResponses|TestStreamRegression|TestSub2API' -count=1
```

## 响应头契约

- `X-Request-Id: hr_xxxx`：所有响应（含错误）都带，贯穿日志与审计。
- `X-Harbor-Queue-Wait-Ms`：本次累计排队毫秒数；SSE 已提前提交响应头时，以审计里的 `queue_wait_ms` 为准。
- `X-Queue-Position`：仅排队、`queue.expose_queue_position: true` 且位置 > 0 时出现。
- 错误体统一 `{"error":{"message","type","code"}}`，绝不含上游原始报文；429 带 `Retry-After`。

## 开发与测试

```powershell
go build ./...
go vet ./...
go test ./...   # db / limits / queue / server 单测 + 集成测试
```

集成测试内置 mock 上游，覆盖排队行为、错误收敛、failover、token 结算、流式、客户端断开、持久化端到端（重启回退查库）等场景，不消耗真实额度。

## 已知限制

- 单实例进程内队列；多实例要在前置负载均衡做实例亲和。
- 路由是静态配置，Server 不缓存上游模型列表，`/v1/models` 只返回本地声明的模型。
- 队头阻塞是有意为之：不会自动协调同账号的其他客户端或实例，也不会自动提高所配置的并发上限。

## 目录结构

```
main.go                 # 入口：配置加载、存储与密钥初始化、HTTP 服务与优雅停机
internal/config/        # YAML 解析 + ${ENV} 文本级展开
internal/key/           # Harbor Key（sha256 存储、模型白名单、封禁）
internal/provider/      # 上游声明、凭证池、模型/路由、错误分类与 Retry-After
internal/limits/        # 并发 Semaphore · RPM 滑动窗口 · Token Gate（tpm/tph/tpd）
internal/queue/         # FIFO 排队、事件驱动调度、重排与超时收敛
internal/executor/      # 出队执行、失败结算、failover
internal/compat/        # Responses / Messages → Chat Completions 协议桥
internal/normalize/     # 请求/响应规范化
internal/db/            # SQLite 审计与运行设置（可选）
internal/stats/         # 今日统计与 p50/p95
internal/server/        # HTTP 路由、管理 API、落地页与管理台（ui/）
tools/                  # 压测、冒烟、图标生成等开发工具
```

## 许可证

[MIT](LICENSE)。仓库不包含任何真实凭证、数据库或部署环境信息；`harbor.yaml` 只使用 `${ENV}` 占位符。

