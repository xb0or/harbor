package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 基础合法配置模板：Provider + 模型路由 + Key 齐全。
const baseYAML = `
server:
  listen: "0.0.0.0:8080"
queue:
  max_wait: 30s
  max_depth: 10
  expose_queue_position: true
limits:
  max_provider_attempts: 2
providers:
  - name: up
    type: openai-compatible
    base_url: https://up.example.com/v1
    credentials:
      - sk-literal
routes:
  m1:
    providers: [up]
keys:
  - secret: hr-key-1
    concurrency: 1
admin:
  token: adm
`

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "harbor.yaml")
	if err := os.WriteFile(p, []byte(baseYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Listen != "0.0.0.0:8080" || len(cfg.Providers) != 1 || cfg.Providers[0].Credentials[0] != "sk-literal" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

// TestParseEnvExpand 覆盖文本级 ${ENV} 展开：listen 端口、key secret、
// admin token、storage 路径、凭证全部可从环境变量注入（PaaS 部署路径）。
func TestParseEnvExpand(t *testing.T) {
	t.Setenv("PORT", "18080")
	t.Setenv("HARBOR_KEY", "hr-env-key-1")
	t.Setenv("ADMIN_TOKEN", "adm-env")
	t.Setenv("DB_PATH", "/data/harbor.db")
	t.Setenv("KEY_PATH", "/data/harbor.key")
	t.Setenv("UPSTREAM_KEY", "sk-env-1")

	yaml := `
server:
  listen: "0.0.0.0:${PORT}"
providers:
  - name: up
    type: openai-compatible
    base_url: https://up.example.com/v1
    credentials:
      - ${UPSTREAM_KEY}
routes:
  m1:
    providers: [up]
keys:
  - secret: ${HARBOR_KEY}
    concurrency: 1
admin:
  token: ${ADMIN_TOKEN}
storage:
  db_path: ${DB_PATH}
  key_path: ${KEY_PATH}
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Server.Listen != "0.0.0.0:18080" {
		t.Fatalf("listen not expanded: %q", cfg.Server.Listen)
	}
	if cfg.Keys[0].Secret != "hr-env-key-1" {
		t.Fatalf("key secret not expanded: %q", cfg.Keys[0].Secret)
	}
	if cfg.Admin.Token != "adm-env" {
		t.Fatalf("admin token not expanded: %q", cfg.Admin.Token)
	}
	if cfg.Storage.DBPath != "/data/harbor.db" || cfg.Storage.KeyPath != "/data/harbor.key" {
		t.Fatalf("storage not expanded: %q %q", cfg.Storage.DBPath, cfg.Storage.KeyPath)
	}
	if cfg.Providers[0].Credentials[0] != "sk-env-1" {
		t.Fatalf("credential not expanded: %q", cfg.Providers[0].Credentials[0])
	}
}

// TestParseUnsetEnv 未设置的环境变量必须给清晰报错，而不是静默注入空值。
func TestParseUnsetEnv(t *testing.T) {
	t.Setenv("PORT", "18080") // 设置一部分，留 key secret 未设置
	t.Setenv("UPSTREAM_KEY", "sk-env-1")
	t.Setenv("ADMIN_TOKEN", "adm-env")
	t.Setenv("DB_PATH", "/data/harbor.db")
	t.Setenv("KEY_PATH", "/data/harbor.key")

	yaml := `
server:
  listen: "0.0.0.0:${PORT}"
providers:
  - name: up
    type: openai-compatible
    base_url: https://up.example.com/v1
    credentials:
      - ${UPSTREAM_KEY}
routes:
  m1:
    providers: [up]
keys:
  - secret: ${HARBOR_SECRET_NOT_SET}
    concurrency: 1
admin:
  token: ${ADMIN_TOKEN}
storage:
  db_path: ${DB_PATH}
  key_path: ${KEY_PATH}
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("want error for unset env var, got nil")
	}
	if !strings.Contains(err.Error(), "HARBOR_SECRET_NOT_SET") {
		t.Fatalf("error should mention unset var name, got: %v", err)
	}
}

// TestParseUnsetPort 未设置 PORT 且 listen 引用了它 → 报错指向 PORT。
func TestParseUnsetPort(t *testing.T) {
	yaml := `
server:
  listen: "0.0.0.0:${PORT}"
providers:
  - name: up
    type: openai-compatible
    base_url: https://up.example.com/v1
    credentials: [sk-1]
routes:
  m1:
    providers: [up]
keys:
  - secret: hr-key-1
    concurrency: 1
`
	_, err := Parse([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "PORT") {
		t.Fatalf("want PORT error, got: %v", err)
	}
}

// TestParseUnsetCredential 凭证引用了未设置的变量 → 报错（原语义保留）。
func TestParseUnsetCredential(t *testing.T) {
	yaml := `
providers:
  - name: up
    type: openai-compatible
    base_url: https://up.example.com/v1
    credentials:
      - ${OPENAI_KEY_UNSET}
routes:
  m1:
    providers: [up]
keys:
  - secret: hr-key-1
    concurrency: 1
`
	_, err := Parse([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "OPENAI_KEY_UNSET") {
		t.Fatalf("want credential env error, got: %v", err)
	}
}
