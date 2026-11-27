# 冒烟工具（开发/排障用）

真实二进制 + SQLite 落库的手工验证小工具。以下步骤在仓库根目录执行。

## 1. 启动 mock 上游

```powershell
go run ./tools/smoke/mock -addr 127.0.0.1:18090
```

## 2. 准备配置（临时）

```yaml
# harbor.smoke.yaml
server:
  listen: "127.0.0.1:18080"
queue:
  max_wait: 30s
  max_depth: 50
  keepalive: 1s
  expose_queue_position: true
limits:
  max_provider_attempts: 2
  request_timeout: 60s
  backoff_base: 10ms
  provider_cooldown: 5s
providers:
  - name: smoke
    type: openai-compatible
    base_url: http://127.0.0.1:18090/v1
    credentials:
      - smoke-upstream-secret
    concurrency: 2
    rpm: 1000
    tpm: 100000
    tph: 1000000
    tpd: 10000000
routes:
  smoke-1:
    providers: [smoke]
keys:
  - secret: "hr-smoke-key-0000000001"
    allowed_models: [smoke-1]
admin:
  token: "smoke-admin-token"
storage:
  db_path: ".smoke/data/harbor.db"
  key_path: ".smoke/data/harbor.key"
log:
  level: info
```

## 3. 构建并启动

```powershell
go build -o .smoke/harbor.exe .
Start-Process .smoke\harbor.exe -ArgumentList '--config','harbor.smoke.yaml' `
  -RedirectStandardOutput .smoke\harbor.log -RedirectStandardError .smoke\harbor.err.log
```

## 4. 验证

```powershell
# 单发（应 200 + X-Request-Id + X-Harbor-Queue-Wait-Ms）
curl.exe -s -D - http://127.0.0.1:18080/v1/chat/completions `
  -H 'Authorization: Bearer hr-smoke-key-0000000001' -H 'Content-Type: application/json' `
  -d '{"model":"smoke-1","messages":[{"role":"user","content":"hi"}]}'

# 4 并发（concurrency=2 → 2 个直接执行、2 个排队带 X-Queue-Position）

# 管理 API
curl.exe -s http://127.0.0.1:18080/api/stats -H 'Authorization: Bearer smoke-admin-token'
curl.exe -s http://127.0.0.1:18080/api/keys -H 'Authorization: Bearer smoke-admin-token'

# 重启持久化：杀 harbor → 重启动 → /api/requests/{id} 回退查库（应 200）
# 查库内容
go run ./tools/smoke/check -db .smoke/data/harbor.db
```

验证结束后删除 `.smoke/` 与 `harbor.smoke.yaml`。
