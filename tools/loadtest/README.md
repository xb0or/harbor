# Harbor 并发排队压测工具

`loadtest.py`：零依赖（纯 Python 标准库）的并发压测客户端，专门用于验证 Harbor
的 **并发排队** 行为——并发超过上游 Provider 并发上限时请求应排队等待而不是立刻失败，
队列满 / 排队超时按契约返回 429 + Retry-After。

`run-local.ps1`：本地一键验证脚本（mock 上游 + 本机 harbor 实例，不消耗真实 API 额度）。

## 为什么能看到“排队”

| 证据 | 来源 |
| --- | --- |
| `X-Queue-Position: N` | 响应头，N>0 说明真的在队列里排过队 |
| `X-Harbor-Queue-Wait-Ms` | 响应头，本次累计排队等待毫秒数 |
| `Retry-After` | 429 响应头，排队超时/队列满后的重试建议 |
| `X-Request-Id` | 每个响应都带，可与 Harbor 审计 `/api/requests/{id}` 对上 |
| `/api/stats` | 管理台实时 `queue.depth/running`，压测期间采样队列深度曲线 |
| `/api/queue` | 管理台排队明细，压测期间采样最大位置 |

## 快速开始（本地离线，推荐）

```powershell
cd harbor
powershell -ExecutionPolicy Bypass -File tools/loadtest/run-local.ps1
# 再加一个 25% 上游 429 的重排场景：
powershell -ExecutionPolicy Bypass -File tools/loadtest/run-local.ps1 -With429
# 流式请求：
powershell -ExecutionPolicy Bypass -File tools/loadtest/run-local.ps1 -Stream
```

脚本做的事：构建 `harbor.exe` + `mock.exe` → 启动 mock（127.0.0.1:18090，600ms 处理延迟）
→ 启动 harbor（127.0.0.1:8080，Provider `smoke` **concurrency=2**）→ 用 12 并发打 48 个
请求 → 打印排队统计与队列深度曲线 → 清理进程。结果 JSON 落在
`tools/loadtest/result-a.json` / `result-b.json`。

预期：48 个请求全部 200；大部分请求带 `X-Queue-Position > 0`；排队等待分布明显、
最大位置 ≈ 10（12 并发 − 2 在跑）；`/api/stats` 采样的 `queue.depth` 峰值 ≈ 10。

## 手动指定目标

```powershell
# 本地
python tools/loadtest/loadtest.py --base-url http://127.0.0.1:8080/v1 `
  --key hr-loadtest-00000000 --admin-token loadtest-admin `
  -c 12 -n 48 --model smoke-1

# 远程部署地址（会消耗真实上游额度！注意 concurrency=2 / rpm=60 配额）
# 域名与 Key 换成本次部署的 Harbor 地址 / Key，不要写进仓库
python tools/loadtest/loadtest.py --base-url https://<你的域名>/v1 `
  --key $env:HARBOR_KEY --admin-token $env:HARBOR_ADMIN_TOKEN `
  -c 8 -n 40 --model gpt-4o


# 持续 30 秒 + 流式
python tools/loadtest/loadtest.py --base-url https://<你的域名>/v1 `
  --key ... -c 20 -d 30 --stream --model gpt-4o

# 结果落盘
... --json result.json
```

## 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--base-url` | `http://127.0.0.1:8080/v1` | Harbor 对外地址（含 `/v1`） |
| `--key` | `$env:HARBOR_KEY` | Harbor Key |
| `--admin-token` | `$env:HARBOR_ADMIN_TOKEN` | 管理台 token，提供后采样 stats/queue |
| `--model` | `smoke-1` | 模型名 |
| `-c/--concurrency` | 10 | 并发 worker 数 |
| `-n/--total` | 0 | 总请求数（0=不限，需配 `-d`） |
| `-d/--duration` | 0 | 持续秒数（0=不限，需配 `-n`） |
| `--stream` | 关 | 流式请求 |
| `--max-tokens` | 16 | 请求 `max_tokens` |
| `--timeout` | 150 | 单请求超时秒 |
| `--ramp` | 0 | 渐变进入：ramp 秒内并发从 0 加到 `-c` |
| `--interval` | 2 | 管理台采样间隔秒 |
| `--extra` | 空 | 附加 JSON body 字段 |
| `--insecure` | 关 | 跳过 HTTPS 证书校验 |
| `--json` | 空 | 结果导出 JSON 路径 |

## 输出字段说明

- 状态分布：`200` 成功；`429` 排队超时/队列满（带 Retry-After）；`4xx/5xx` 上游错误收敛；
  网络错误/超时单独归类。
- 排队等待：只统计带 `X-Harbor-Queue-Wait-Ms` 的响应（排队过的请求）。
  流式（SSE）模式下 Harbor 可能在响应头提交后不再补该头——此时用 TTFB 观察排队，
  精确值查 `/api/requests/{id}` 审计里的 `queue_wait_ms`。
- 队列位置：带 `X-Queue-Position>0` 的请求数 + 最大位置。
- 管理台采样：`queue.depth` start/peak/end、`queue.running`、`today.requests/success/failed/queued`
  前后对比、`/api/queue` 峰值位置。

## mock 上游（tools/smoke/mock）

```powershell
go run ./tools/smoke/mock -addr 127.0.0.1:18090 -delay-ms 600 -fail-429-rate 0.25 -fail-500-rate 0
```

- `-delay-ms`：处理延迟，给排队留观察窗口
- `-fail-429-rate`：概率返回 429（`Retry-After: 1`），触发 Harbor 重排/冷却
- `-fail-500-rate`：概率返回 500
- 请求带 `stream:true` 时返回 SSE 流
- 每个请求打一行 `[mock]` 日志
