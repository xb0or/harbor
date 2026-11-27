# Harbor 调度修复验证记录（2026-09-09）

## 变更范围

本记录对应本地修复验证阶段；后续已按用户要求部署到原 Zeabur 服务，新 deployment `6aa13ca1ea9ecb9e577e64bc` 于 2026-09-09 19:03 +08:00 RUNNING。部署健康检查见 `zeabur-deploy-20260909-requeue.md`；未修改线上 SQLite/Provider 配额。

- `internal/executor/executor.go`：租约模式每次出队只执行一次；跨重排累计尝试预算，返回 `RequeueAt` 而非持槽睡眠。
- `internal/queue/queue.go`：保留原 Job/Request ID/HTTP result channel/请求体/原 deadline；按 attempt 释放额度，失败重回队尾；终结幂等清理；全部等待者按原 deadline 过期。
- `internal/provider/provider.go`、`internal/normalize/normalize.go`：解析 Retry-After 秒数/HTTP 日期，冷却只延长不缩短；保留恢复元数据；硬配额与参数错误不做短周期重试。
- `internal/limits/gate.go`、`internal/limits/token.go`：不限并发时仍计入在途占用；Token 剩余额度小于下一次预扣时也能预测窗口恢复。
- `internal/server/server.go`、`internal/server/admin_stats.go`：重新申请所有 Provider Gate，取消时回收迟到的 stream/result；排队时长不含执行时间；running 包含 SSE；一次原请求只记录一次最终结果。
- `README.md`：修正 Provider/Key 限额位置和超时说明，补充重排语义与边界。

## 回归证据

先建立失败测试，复现了：单出口429直接外泄、尝试次数停在1、150ms执行被记成151ms排队、活跃SSE running=0，以及停止时丢失重试次数和队尾请求deadline被延长。

修复后验证：

- 10个并发请求，每个首轮429、第二轮成功：客户端10次成功、上游20次尝试、最大上游并发不超过2、审计每条attempts=2。
- Retry-After: 1：等待至少约1秒，Responses SSE持续心跳，最终只有一个completed，无中间failed。
- 备用Provider并发为1且忙：两条失败请求等待后成功，备用最大并发仍为1。
- 连续429最多累计3次；重排不重置时间或次数；每次发送消耗RPM，不通过重试绕过限额。
- 冷却中取消、队列满、停止、原总超时、队尾提前截止均结束且清理；已启动响应不重放。
- 原并发/FIFO/RPM/Token/SUB2API/持久化等全项目测试继续通过。

执行：

```powershell
go test ./... -count=1
go vet ./...
go build -o harbor.exe .
go test ./internal/queue -run TestRequeue -count=30
go test ./internal/server -run 'TestRequeueConcurrent|TestRequeueRetryAfter|TestRequeueWaits' -count=10
```

Windows 构建及启动帮助正常；`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./...` 通过。

竞态检测尝试：`go test -race` 测试进程以 Windows 状态 `0xc0000139` 启动失败（先前未修改的stats包亦同），未得到有效竞态检测结果；本机无已安装 WSL 发行版。不能把此项记作通过。

## 保留边界

- 全局 FIFO 队头阻塞策略保留，不自动变更并发上限，不添加分布式共享额度。
- 真实上游额度不匹配、硬配额、预算耗尽仍可能返回最终429。
- 请求超时覆盖全部等待和执行；queue.max_wait从首次入队计算，重排不重置。
- 网络故障重试不提供上游 exactly-once 保证；已经开始输出的业务流不重放。
- 需要部署后，使用实际SUB2API链路和运行时Provider配额复测，才能确认线上效果。
