// Package db 提供 Harbor 的 SQLite 持久层（计划书 §13）。
//
// 表结构遵循计划书：
//   - api_keys：Harbor Key 的额度与状态（哈希存储，不含明文）；
//   - requests：单请求审计记录（不保存请求 Body）；
//   - provider_credentials：上游凭证（secret 加密存储，§18）。
//
// 内存组件（key.Store / queue / stats）仍是运行时权威与热路径；
// 本包负责持久化镜像与审计查询，容量与性能按自用单机设计。
package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// DB 是 SQLite 持久层。
type DB struct {
	sql *sql.DB
}

// Open 打开（或创建）SQLite 数据库并做基础配置。
// 父目录不存在时自动创建（部署友好：无需预先 mkdir）。
func Open(path string) (*DB, error) {
	if path == "" {
		path = "harbor.db"
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db dir %s: %w", dir, err)
		}
	}
	sq, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	// 自用单机：强制单连接串行化。SQLite 单写者模型下并发连接反而引入
	// 锁竞争（busy_timeout 仅作用于设置它的连接），单连接从根上消除
	// SQLITE_BUSY / 偶发落库丢失；本地写入微秒级，串行开销可忽略。
	sq.SetMaxOpenConns(1)
	// 自用单机：WAL 降低读写互斥，busy_timeout 避免并发写锁冲突
	if _, err := sq.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		_ = sq.Close()
		return nil, fmt.Errorf("set journal_mode: %w", err)
	}
	if _, err := sq.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		_ = sq.Close()
		return nil, fmt.Errorf("set busy_timeout: %w", err)
	}
	if _, err := sq.Exec(`PRAGMA synchronous=NORMAL`); err != nil {
		_ = sq.Close()
		return nil, fmt.Errorf("set synchronous: %w", err)
	}
	return &DB{sql: sq}, nil
}

// Close 关闭数据库。
func (d *DB) Close() error { return d.sql.Close() }

// Migrate 建表（幂等）。
func (d *DB) Migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS api_keys (
			id            TEXT PRIMARY KEY,
			prefix        TEXT NOT NULL,
			key_hash      TEXT NOT NULL UNIQUE,
			secret_encrypted TEXT NOT NULL DEFAULT '',
			status        TEXT NOT NULL DEFAULT 'active',
			allowed_models TEXT NOT NULL DEFAULT '[]',
			created_at    DATETIME NOT NULL,
			expires_at    DATETIME
		)`,
		`CREATE TABLE IF NOT EXISTS requests (
			request_id    TEXT PRIMARY KEY,
			key_id        TEXT NOT NULL,
			model         TEXT NOT NULL,
			provider      TEXT,
			status        TEXT NOT NULL,
			error_code    TEXT,
			upstream_code TEXT,
			upstream_type TEXT,
			upstream_http INTEGER NOT NULL DEFAULT 0,
			error_detail  TEXT,
			queued_at     DATETIME NOT NULL,
			started_at    DATETIME,
			finished_at   DATETIME,
			queue_wait_ms INTEGER NOT NULL DEFAULT 0,
			attempts      INTEGER NOT NULL DEFAULT 0,
			tokens_in     INTEGER NOT NULL DEFAULT 0,
			tokens_out    INTEGER NOT NULL DEFAULT 0,
			client_ua     TEXT,
			client_ip     TEXT,
			upstream_model TEXT,
			first_token_ms INTEGER NOT NULL DEFAULT 0,
			response_model TEXT,
			upstream_request_id TEXT,
			cached_tokens INTEGER NOT NULL DEFAULT 0,
			stream INTEGER NOT NULL DEFAULT 0,
			cache_write_tokens INTEGER NOT NULL DEFAULT 0,
			inbound_path TEXT,
			upstream_header_ms INTEGER NOT NULL DEFAULT 0,
			upstream_content_type TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_requests_finished ON requests(finished_at DESC)`,
		`CREATE TABLE IF NOT EXISTS provider_credentials (
			id               TEXT PRIMARY KEY,
			provider_id      TEXT NOT NULL,
			secret_encrypted TEXT NOT NULL,
			secret_fingerprint TEXT NOT NULL DEFAULT '',
			status           TEXT NOT NULL DEFAULT 'active',
			cooldown_until   DATETIME,
			UNIQUE(provider_id, secret_fingerprint)
		)`,
		`CREATE TABLE IF NOT EXISTS providers (
			id            TEXT PRIMARY KEY,
			name          TEXT UNIQUE NOT NULL,
			type          TEXT NOT NULL,
			base_url      TEXT NOT NULL,
			models        TEXT NOT NULL DEFAULT '[]',
			timeout       TEXT,
			cooldown      TEXT,
			concurrency   INTEGER NOT NULL DEFAULT 0,
			rpm           INTEGER NOT NULL DEFAULT 0,
			tpm           INTEGER NOT NULL DEFAULT 0,
			tph           INTEGER NOT NULL DEFAULT 0,
			tpd           INTEGER NOT NULL DEFAULT 0,
			created_at    DATETIME NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS routes (
			model     TEXT PRIMARY KEY,
			mappings  TEXT NOT NULL DEFAULT '[]'
		)`,
		`CREATE TABLE IF NOT EXISTS settings (
			key        TEXT PRIMARY KEY,
			value      TEXT NOT NULL,
			updated_at DATETIME NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := d.sql.Exec(s); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	// 旧版 provider_credentials 无 secret_fingerprint 列：重建（镜像表，
	// 数据可由启动镜像再生；避免每次重启因随机 credential id 膨胀重复行）。
	if err := d.migrateCredentialFingerprint(); err != nil {
		return fmt.Errorf("migrate credentials: %w", err)
	}
	// 旧版 api_keys 含 concurrency/rpm/tpm/tpd/priority 列：重建（额度已迁移到 Provider）。
	if err := d.migrateApiKeysDropLimits(); err != nil {
		return fmt.Errorf("migrate api_keys: %w", err)
	}
	// 旧版 api_keys 没有 secret_encrypted：新增加密密文列，用于恢复管理台创建的 Key。
	if err := d.migrateApiKeysSecret(); err != nil {
		return fmt.Errorf("migrate api_keys secret: %w", err)
	}
	// 旧版 routes 存储 []string 格式（简单 provider 名列表），新版存 []ModelMapping。
	// 只有检测到旧格式时才重建；当前格式必须保留，避免重启丢失管理台映射。
	if err := d.migrateRoutesFormat(); err != nil {
		return fmt.Errorf("migrate routes: %w", err)
	}
	// 旧版 requests 无 provider 列（未记录每请求命中的上游）：轻量加列。
	if err := d.migrateRequestsProvider(); err != nil {
		return fmt.Errorf("migrate requests: %w", err)
	}
	// 旧版 requests 无上游错误详情列：轻量加列，保留已有审计记录。
	if err := d.migrateRequestsDetail(); err != nil {
		return err
	}
	if err := d.migrateRequestsUpstreamMeta(); err != nil {
		return err
	}
	if err := d.migrateRequestsCacheWrite(); err != nil {
		return err
	}
	if err := d.migrateRequestsStream(); err != nil {
		return err
	}
	if err := d.migrateRequestsUpstreamError(); err != nil {
		return fmt.Errorf("migrate requests upstream error: %w", err)
	}
	if err := d.migrateRequestsStreamMeta(); err != nil {
		return fmt.Errorf("migrate requests stream meta: %w", err)
	}
	return nil
}

const credTableSQL = `CREATE TABLE IF NOT EXISTS provider_credentials (
	id               TEXT PRIMARY KEY,
	provider_id      TEXT NOT NULL,
	secret_encrypted TEXT NOT NULL,
	secret_fingerprint TEXT NOT NULL DEFAULT '',
	status           TEXT NOT NULL DEFAULT 'active',
	cooldown_until   DATETIME,
	UNIQUE(provider_id, secret_fingerprint)
)`

const apiKeysTableSQL = `CREATE TABLE IF NOT EXISTS api_keys (
	id            TEXT PRIMARY KEY,
	prefix        TEXT NOT NULL,
	key_hash      TEXT NOT NULL UNIQUE,
	secret_encrypted TEXT NOT NULL DEFAULT '',
	status        TEXT NOT NULL DEFAULT 'active',
	allowed_models TEXT NOT NULL DEFAULT '[]',
	created_at    DATETIME NOT NULL,
	expires_at    DATETIME
)`

const routesTableSQL = `CREATE TABLE IF NOT EXISTS routes (
	model     TEXT PRIMARY KEY,
	mappings  TEXT NOT NULL DEFAULT '[]'
)`

type sqlQueryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func tableColumns(q sqlQueryer, table string) (map[string]bool, error) {
	rows, err := q.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid, name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

// migrateCredentialFingerprint 将旧凭证表迁移到新结构并保留密文。
// 迁移在事务内完成；旧行暂用 legacy:<id> 指纹，启动恢复解密后再回填真实指纹。
func (d *DB) migrateCredentialFingerprint() error {
	columns, err := tableColumns(d.sql, "provider_credentials")
	if err != nil {
		return err
	}
	if columns["secret_fingerprint"] {
		return nil
	}
	for _, name := range []string{"id", "provider_id", "secret_encrypted"} {
		if !columns[name] {
			return fmt.Errorf("provider_credentials missing required column %q", name)
		}
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	rollback := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE provider_credentials RENAME TO provider_credentials_legacy`); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(credTableSQL); err != nil {
		return rollback(err)
	}
	statusExpr := `'active'`
	if columns["status"] {
		statusExpr = `status`
	}
	cooldownExpr := `NULL`
	if columns["cooldown_until"] {
		cooldownExpr = `cooldown_until`
	}
	rows, err := tx.Query(fmt.Sprintf(
		`SELECT id, provider_id, secret_encrypted, %s, %s FROM provider_credentials_legacy`,
		statusExpr, cooldownExpr,
	))
	if err != nil {
		return rollback(err)
	}
	type legacyCredential struct {
		id, providerID, encrypted, status string
		cooldown                          sql.NullTime
	}
	var legacy []legacyCredential
	for rows.Next() {
		var row legacyCredential
		if err := rows.Scan(&row.id, &row.providerID, &row.encrypted, &row.status, &row.cooldown); err != nil {
			rows.Close()
			return rollback(err)
		}
		legacy = append(legacy, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return rollback(err)
	}
	if err := rows.Close(); err != nil {
		return rollback(err)
	}
	for _, row := range legacy {
		if _, err := tx.Exec(`
			INSERT INTO provider_credentials
				(id, provider_id, secret_encrypted, secret_fingerprint, status, cooldown_until)
			VALUES (?, ?, ?, ?, ?, ?)`,
			row.id, row.providerID, row.encrypted, "legacy:"+row.id, row.status, nullTime(row.cooldown.Time),
		); err != nil {
			return rollback(err)
		}
	}
	if _, err := tx.Exec(`DROP TABLE provider_credentials_legacy`); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit credential migration: %w", err)
	}
	return nil
}

// migrateApiKeysDropLimits 将旧版 api_keys 迁移到新结构并保留公共字段。
// 额度字段已迁移到 Provider，但 Key 的身份、状态和白名单不能丢失。
func (d *DB) migrateApiKeysDropLimits() error {
	columns, err := tableColumns(d.sql, "api_keys")
	if err != nil {
		return err
	}
	hasOld := columns["concurrency"] || columns["rpm"] || columns["tpm"] ||
		columns["tpd"] || columns["priority"]
	if !hasOld {
		return nil
	}
	for _, name := range []string{"id", "prefix", "key_hash", "status", "allowed_models", "created_at"} {
		if !columns[name] {
			return fmt.Errorf("api_keys missing required column %q", name)
		}
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	rollback := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE api_keys RENAME TO api_keys_legacy`); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(apiKeysTableSQL); err != nil {
		return rollback(err)
	}
	columnsToCopy := []string{"id", "prefix", "key_hash", "status", "allowed_models", "created_at"}
	if columns["secret_encrypted"] {
		columnsToCopy = append(columnsToCopy, "secret_encrypted")
	}
	if columns["expires_at"] {
		columnsToCopy = append(columnsToCopy, "expires_at")
	}
	copySQL := fmt.Sprintf(
		"INSERT INTO api_keys (%s) SELECT %s FROM api_keys_legacy",
		strings.Join(columnsToCopy, ", "), strings.Join(columnsToCopy, ", "),
	)
	if _, err := tx.Exec(copySQL); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(`DROP TABLE api_keys_legacy`); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit api key migration: %w", err)
	}
	return nil
}

// migrateApiKeysSecret 为已有 api_keys 表增加加密密文列。
// 旧版本只保存哈希，旧 Key 无法从库中恢复明文；新建 Key 会写入该列。
func (d *DB) migrateApiKeysSecret() error {
	rows, err := d.sql.Query(`PRAGMA table_info(api_keys)`)
	if err != nil {
		return err
	}
	hasSecret := false
	for rows.Next() {
		var (
			cid, name, ctype string
			notnull, pk      int
			dflt             any
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "secret_encrypted" {
			hasSecret = true
			break
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if hasSecret {
		return nil
	}
	_, err = d.sql.Exec(`ALTER TABLE api_keys ADD COLUMN secret_encrypted TEXT NOT NULL DEFAULT ''`)
	return err
}

// migrateRoutesFormat 将旧版 []string 路由转换为 []ModelMapping，并保留已有映射。
// 迁移在事务内完成，异常时原表和数据一起回滚。
func (d *DB) migrateRoutesFormat() error {
	rows, err := d.sql.Query(`SELECT model, mappings FROM routes`)
	if err != nil {
		return err
	}
	type routeData struct {
		model, raw string
	}
	var data []routeData
	legacy := false
	for rows.Next() {
		var row routeData
		if err := rows.Scan(&row.model, &row.raw); err != nil {
			rows.Close()
			return err
		}
		var items []json.RawMessage
		if json.Unmarshal([]byte(row.raw), &items) == nil {
			for _, item := range items {
				var provider string
				if json.Unmarshal(item, &provider) == nil {
					legacy = true
					break
				}
			}
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !legacy {
		return nil
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	rollback := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE routes RENAME TO routes_legacy`); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(routesTableSQL); err != nil {
		return rollback(err)
	}
	for _, row := range data {
		mappings := row.raw
		var providers []string
		if json.Unmarshal([]byte(row.raw), &providers) == nil && len(providers) > 0 {
			converted := make([]ModelMapping, len(providers))
			for i, provider := range providers {
				converted[i] = ModelMapping{Provider: provider}
			}
			encoded, err := json.Marshal(converted)
			if err != nil {
				return rollback(err)
			}
			mappings = string(encoded)
		}
		if _, err := tx.Exec(`INSERT INTO routes (model, mappings) VALUES (?, ?)`, row.model, mappings); err != nil {
			return rollback(err)
		}
	}
	if _, err := tx.Exec(`DROP TABLE routes_legacy`); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit route migration: %w", err)
	}
	return nil
}

// migrateRequestsProvider 旧版 requests 无 provider 列：轻量 ALTER 加列，
// 不重建表，保留已有请求记录。
func (d *DB) migrateRequestsProvider() error {
	columns, err := tableColumns(d.sql, "requests")
	if err != nil {
		return err
	}
	if columns["provider"] {
		return nil
	}
	for _, name := range []string{"request_id", "model"} {
		if !columns[name] {
			return fmt.Errorf("requests missing required column %q", name)
		}
	}
	if _, err := d.sql.Exec(`ALTER TABLE requests ADD COLUMN provider TEXT`); err != nil {
		return fmt.Errorf("migrate requests.provider: %w", err)
	}
	return nil
}

// migrateRequestsUpstreamError 旧版 requests 无上游错误详情列：轻量 ALTER 加列，
// 不重建表，保留已有请求记录。
// migrateRequestsDetail 补齐客户端 / 上游模型 / 首帧耗时列（旧库自动迁移）。
func (d *DB) migrateRequestsDetail() error {
	columns, err := tableColumns(d.sql, "requests")
	if err != nil {
		return err
	}
	if columns["client_ua"] {
		return nil
	}
	for _, stmt := range []string{
		`ALTER TABLE requests ADD COLUMN client_ua TEXT`,
		`ALTER TABLE requests ADD COLUMN client_ip TEXT`,
		`ALTER TABLE requests ADD COLUMN upstream_model TEXT`,
		`ALTER TABLE requests ADD COLUMN first_token_ms INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := d.sql.Exec(stmt); err != nil {
			return fmt.Errorf("migrate requests detail: %w", err)
		}
	}
	return nil
}

// migrateRequestsUpstreamMeta 补齐上游响应元信息列（旧库自动迁移）。
func (d *DB) migrateRequestsUpstreamMeta() error {
	columns, err := tableColumns(d.sql, "requests")
	if err != nil {
		return err
	}
	if columns["response_model"] {
		return nil
	}
	for _, stmt := range []string{
		`ALTER TABLE requests ADD COLUMN response_model TEXT`,
		`ALTER TABLE requests ADD COLUMN upstream_request_id TEXT`,
		`ALTER TABLE requests ADD COLUMN cached_tokens INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := d.sql.Exec(stmt); err != nil {
			return fmt.Errorf("migrate requests upstream meta: %w", err)
		}
	}
	return nil
}

// migrateRequestsCacheWrite 补缓存写入列（旧库自动迁移）。
func (d *DB) migrateRequestsCacheWrite() error {
	columns, err := tableColumns(d.sql, "requests")
	if err != nil {
		return err
	}
	if columns["cache_write_tokens"] {
		return nil
	}
	if _, err := d.sql.Exec(`ALTER TABLE requests ADD COLUMN cache_write_tokens INTEGER NOT NULL DEFAULT 0`); err != nil {
		return fmt.Errorf("migrate requests cache write: %w", err)
	}
	return nil
}

// migrateRequestsStream 补 stream 列（旧库自动迁移）。
func (d *DB) migrateRequestsStream() error {
	columns, err := tableColumns(d.sql, "requests")
	if err != nil {
		return err
	}
	if columns["stream"] {
		return nil
	}
	if _, err := d.sql.Exec(`ALTER TABLE requests ADD COLUMN stream INTEGER NOT NULL DEFAULT 0`); err != nil {
		return fmt.Errorf("migrate requests stream: %w", err)
	}
	return nil
}

func (d *DB) migrateRequestsUpstreamError() error {
	columns, err := tableColumns(d.sql, "requests")
	if err != nil {
		return err
	}
	if columns["upstream_code"] {
		return nil
	}
	for _, name := range []string{"request_id", "status"} {
		if !columns[name] {
			return fmt.Errorf("requests missing required column %q", name)
		}
	}
	for _, stmt := range []string{
		`ALTER TABLE requests ADD COLUMN upstream_code TEXT`,
		`ALTER TABLE requests ADD COLUMN upstream_type TEXT`,
		`ALTER TABLE requests ADD COLUMN upstream_http INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE requests ADD COLUMN error_detail TEXT`,
	} {
		if _, err := d.sql.Exec(stmt); err != nil {
			return fmt.Errorf("migrate requests upstream error: %w", err)
		}
	}
	return nil
}

// migrateRequestsStreamMeta 补齐流式链路观测列（入站路径 / 上游响应头耗时 / Content-Type）。
// 逐列检查并 ALTER，兼容任意历史版本且不丢已有审计记录。
func (d *DB) migrateRequestsStreamMeta() error {
	columns, err := tableColumns(d.sql, "requests")
	if err != nil {
		return err
	}
	adds := []struct {
		name string
		def  string
	}{
		{"inbound_path", "TEXT"},
		{"upstream_header_ms", "INTEGER NOT NULL DEFAULT 0"},
		{"upstream_content_type", "TEXT"},
	}
	for _, add := range adds {
		if columns[add.name] {
			continue
		}
		if _, err := d.sql.Exec(`ALTER TABLE requests ADD COLUMN ` + add.name + ` ` + add.def); err != nil {
			return fmt.Errorf("migrate requests stream meta (%s): %w", add.name, err)
		}
	}
	return nil
}

// --- requests ---

// Request 是单请求审计记录（对应 requests 表）。
type Request struct {
	RequestID string `json:"request_id"`
	KeyID     string `json:"key_id"`
	Model     string `json:"model"`
	Provider  string `json:"provider,omitempty"` // 实际命中的上游 Provider
	Status    string `json:"status"`             // success | error | cancelled
	ErrorCode string `json:"error_code,omitempty"`
	// UpstreamCode/UpstreamType/UpstreamHTTP 上游原始错误信息（仅后台审计展示，绝不对外）。
	UpstreamCode string    `json:"upstream_code,omitempty"`
	UpstreamType string    `json:"upstream_type,omitempty"`
	UpstreamHTTP int       `json:"upstream_http,omitempty"`
	ErrorDetail  string    `json:"error_detail,omitempty"` // 内部原因（仅后台审计展示）
	QueuedAt     time.Time `json:"queued_at"`
	StartedAt    time.Time `json:"started_at,omitempty"`
	FinishedAt   time.Time `json:"finished_at"`
	QueueWaitMs  int64     `json:"queue_wait_ms"`
	Attempts     int       `json:"attempts"`
	TokensIn     int       `json:"tokens_in"`
	TokensOut    int       `json:"tokens_out"`
	// 明细字段（2026-11 新增）：客户端、上游模型、首帧耗时。
	ClientUA          string `json:"client_ua,omitempty"`
	ClientIP          string `json:"client_ip,omitempty"`
	UpstreamModel     string `json:"upstream_model,omitempty"`
	FirstTokenMs      int64  `json:"first_token_ms,omitempty"`
	ResponseModel     string `json:"response_model,omitempty"`
	UpstreamRequestID string `json:"upstream_request_id,omitempty"`
	CachedTokens      int    `json:"cached_tokens,omitempty"`
	CachedWriteTokens int    `json:"cache_write_tokens,omitempty"`
	Stream            bool   `json:"stream,omitempty"`
	// 流式链路观测（2026-11 新增）。
	InboundPath         string `json:"inbound_path,omitempty"`
	UpstreamHeaderMs    int64  `json:"upstream_header_ms,omitempty"`
	UpstreamContentType string `json:"upstream_content_type,omitempty"`
}

// RecordRequest 写入一条请求记录（按 request_id 幂等覆盖）。
func (d *DB) RecordRequest(r *Request) error {
	if r == nil {
		return errors.New("nil request")
	}
	_, err := d.sql.Exec(`
		INSERT INTO requests (request_id, key_id, model, provider, status, error_code,
			upstream_code, upstream_type, upstream_http, error_detail,
			queued_at, started_at, finished_at, queue_wait_ms, attempts,
			tokens_in, tokens_out, client_ua, client_ip, upstream_model, first_token_ms,
			response_model, upstream_request_id, cached_tokens, cache_write_tokens, stream,
			inbound_path, upstream_header_ms, upstream_content_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(request_id) DO UPDATE SET
			status=excluded.status, error_code=excluded.error_code,
			upstream_code=excluded.upstream_code, upstream_type=excluded.upstream_type,
			upstream_http=excluded.upstream_http, error_detail=excluded.error_detail,
			started_at=excluded.started_at, finished_at=excluded.finished_at,
			queue_wait_ms=excluded.queue_wait_ms, attempts=excluded.attempts,
			tokens_in=excluded.tokens_in, tokens_out=excluded.tokens_out,
			client_ua=excluded.client_ua, client_ip=excluded.client_ip,
			upstream_model=excluded.upstream_model, first_token_ms=excluded.first_token_ms,
			response_model=excluded.response_model, upstream_request_id=excluded.upstream_request_id,
			cached_tokens=excluded.cached_tokens, cache_write_tokens=excluded.cache_write_tokens,
			stream=excluded.stream, inbound_path=excluded.inbound_path,
			upstream_header_ms=excluded.upstream_header_ms,
			upstream_content_type=excluded.upstream_content_type,
			provider=excluded.provider`,
		r.RequestID, r.KeyID, r.Model, nullStr(r.Provider), r.Status, nullStr(r.ErrorCode),
		nullStr(r.UpstreamCode), nullStr(r.UpstreamType), r.UpstreamHTTP, nullStr(r.ErrorDetail),
		r.QueuedAt, nullTime(r.StartedAt), r.FinishedAt,
		r.QueueWaitMs, r.Attempts, r.TokensIn, r.TokensOut,
		nullStr(r.ClientUA), nullStr(r.ClientIP), nullStr(r.UpstreamModel), r.FirstTokenMs,
		nullStr(r.ResponseModel), nullStr(r.UpstreamRequestID), r.CachedTokens, r.CachedWriteTokens, boolInt(r.Stream),
		nullStr(r.InboundPath), r.UpstreamHeaderMs, nullStr(r.UpstreamContentType),
	)
	if err != nil {
		return fmt.Errorf("record request: %w", err)
	}
	return nil
}

// GetRequest 按 ID 查询请求。
func (d *DB) GetRequest(id string) (*Request, error) {
	row := d.sql.QueryRow(`
		SELECT request_id, key_id, model, provider, status, error_code,
			upstream_code, upstream_type, upstream_http, error_detail,
			queued_at, started_at, finished_at, queue_wait_ms, attempts,
			tokens_in, tokens_out, client_ua, client_ip, upstream_model, first_token_ms,
			response_model, upstream_request_id, cached_tokens, cache_write_tokens, stream,
			inbound_path, upstream_header_ms, upstream_content_type
		FROM requests WHERE request_id = ?`, id)
	return scanRequest(row)
}

// RequestFilter 是管理台请求列表的筛选条件。
type RequestFilter struct {
	Q        string
	Status   string
	Provider string
	Model    string
	KeyID    string
	Since    time.Time
	Until    time.Time
	Limit    int
	Offset   int
}

// RecentRequests 返回最近 limit 条请求（按完成时间倒序）。
func (d *DB) RecentRequests(limit int) ([]*Request, error) {
	if limit <= 0 || limit > 5000 {
		limit = 100
	}
	out, _, err := d.ListRequests(RequestFilter{Limit: limit})
	return out, err
}

// ListRequests 按完成时间倒序返回请求，并给出符合筛选的总数（分页用）。
func (d *DB) ListRequests(f RequestFilter) ([]*Request, int, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 5000 {
		f.Limit = 5000
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	q := strings.TrimSpace(f.Q)
	status := strings.TrimSpace(f.Status)
	switch status {
	case "success", "error", "timeout", "cancelled":
	default:
		status = ""
	}

	var (
		where []string
		args  []any
	)
	if q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, `(request_id LIKE ? ESCAPE '!' OR key_id LIKE ? ESCAPE '!' OR model LIKE ? ESCAPE '!' OR IFNULL(provider,'') LIKE ? ESCAPE '!' OR IFNULL(error_code,'') LIKE ? ESCAPE '!' OR IFNULL(upstream_code,'') LIKE ? ESCAPE '!' OR IFNULL(error_detail,'') LIKE ? ESCAPE '!')`)
		args = append(args, like, like, like, like, like, like, like)
	}
	if status != "" {
		where = append(where, `status = ?`)
		args = append(args, status)
	}
	if f.Since.After(time.Time{}) {
		where = append(where, `substr(finished_at,1,10) >= ?`)
		args = append(args, f.Since.In(time.Local).Format("2006-01-02"))
	}
	if f.Until.After(time.Time{}) {
		where = append(where, `substr(finished_at,1,10) < ?`)
		args = append(args, f.Until.In(time.Local).Format("2006-01-02"))
	}
	if v := strings.TrimSpace(f.Provider); v != "" {
		where = append(where, `IFNULL(provider,'') = ?`)
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.Model); v != "" {
		where = append(where, `model = ?`)
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.KeyID); v != "" {
		where = append(where, `key_id = ?`)
		args = append(args, v)
	}
	clause := "1=1"
	if len(where) > 0 {
		clause = strings.Join(where, " AND ")
	}

	var total int
	if err := d.sql.QueryRow(`SELECT COUNT(*) FROM requests WHERE `+clause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count requests: %w", err)
	}

	queryArgs := append(append([]any{}, args...), f.Limit, f.Offset)
	rows, err := d.sql.Query(`
		SELECT request_id, key_id, model, provider, status, error_code,
			upstream_code, upstream_type, upstream_http, error_detail,
			queued_at, started_at, finished_at, queue_wait_ms, attempts,
			tokens_in, tokens_out, client_ua, client_ip, upstream_model, first_token_ms,
			response_model, upstream_request_id, cached_tokens, cache_write_tokens, stream,
			inbound_path, upstream_header_ms, upstream_content_type
		FROM requests WHERE `+clause+` ORDER BY finished_at DESC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list requests: %w", err)
	}
	defer rows.Close()
	var out []*Request
	for rows.Next() {
		r, err := scanRequest(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}

func escapeLike(s string) string {
	s = strings.ReplaceAll(s, "!", "!!")
	s = strings.ReplaceAll(s, "%", "!%")
	s = strings.ReplaceAll(s, "_", "!_")
	return s
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRequest(r rowScanner) (*Request, error) {
	var (
		x             Request
		provider      sql.NullString
		errorCode     sql.NullString
		upCode        sql.NullString
		upType        sql.NullString
		upHTTP        int
		detail        sql.NullString
		startedAt     sql.NullTime
		finishedAt    sql.NullTime
		queueWait     int64
		attempts      int
		tokensIn      int
		tokensOut     int
		clientUA      sql.NullString
		clientIP      sql.NullString
		upModel       sql.NullString
		firstMs       int64
		respModel     sql.NullString
		upReqID       sql.NullString
		cachedTok     int
		cacheWriteTok int
		streamFlag    int
		inboundPath   sql.NullString
		upHeaderMs    int64
		upContentType sql.NullString
	)
	err := r.Scan(&x.RequestID, &x.KeyID, &x.Model, &provider, &x.Status, &errorCode,
		&upCode, &upType, &upHTTP, &detail,
		&x.QueuedAt, &startedAt, &finishedAt, &queueWait, &attempts,
		&tokensIn, &tokensOut, &clientUA, &clientIP, &upModel, &firstMs,
		&respModel, &upReqID, &cachedTok, &cacheWriteTok, &streamFlag,
		&inboundPath, &upHeaderMs, &upContentType)
	if err != nil {
		return nil, err
	}
	x.Provider = provider.String
	x.ErrorCode = errorCode.String
	x.UpstreamCode = upCode.String
	x.UpstreamType = upType.String
	x.UpstreamHTTP = upHTTP
	x.ErrorDetail = detail.String
	x.StartedAt = startedAt.Time
	x.FinishedAt = finishedAt.Time
	x.QueueWaitMs = queueWait
	x.Attempts = attempts
	x.TokensIn = tokensIn
	x.TokensOut = tokensOut
	x.ClientUA = clientUA.String
	x.ClientIP = clientIP.String
	x.UpstreamModel = upModel.String
	x.FirstTokenMs = firstMs
	x.ResponseModel = respModel.String
	x.UpstreamRequestID = upReqID.String
	x.CachedTokens = cachedTok
	x.CachedWriteTokens = cacheWriteTok
	x.Stream = streamFlag != 0
	x.InboundPath = inboundPath.String
	x.UpstreamHeaderMs = upHeaderMs
	x.UpstreamContentType = upContentType.String
	return &x, nil
}

// --- api_keys ---

// Key 是 api_keys 表的镜像（不含明文，不含额度配置）。
type Key struct {
	ID              string
	Prefix          string
	KeyHash         string
	SecretEncrypted string // AES-GCM 密文；仅用于重启恢复管理台创建的 Key
	Status          string
	AllowedModels   []string
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

// UpsertKey 幂等写入 Key。
func (d *DB) UpsertKey(k *Key) error {
	models, err := json.Marshal(k.AllowedModels)
	if err != nil {
		return fmt.Errorf("marshal models: %w", err)
	}
	_, err = d.sql.Exec(`
		INSERT INTO api_keys (id, prefix, key_hash, secret_encrypted, status, allowed_models, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			prefix=excluded.prefix, key_hash=excluded.key_hash,
			secret_encrypted=CASE WHEN excluded.secret_encrypted <> '' THEN excluded.secret_encrypted ELSE api_keys.secret_encrypted END,
			status=excluded.status, allowed_models=excluded.allowed_models,
			expires_at=excluded.expires_at`,
		k.ID, k.Prefix, k.KeyHash, k.SecretEncrypted, k.Status, string(models), k.CreatedAt, nullTime(k.ExpiresAt),
	)
	if err != nil {
		return fmt.Errorf("upsert key: %w", err)
	}
	return nil
}

// ListKeys 列出全部 Key。
func (d *DB) ListKeys() ([]*Key, error) {
	rows, err := d.sql.Query(`
		SELECT id, prefix, key_hash, secret_encrypted, status, allowed_models, created_at, expires_at
		FROM api_keys ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer rows.Close()
	var out []*Key
	for rows.Next() {
		var (
			k         Key
			models    string
			expiresAt sql.NullTime
		)
		if err := rows.Scan(&k.ID, &k.Prefix, &k.KeyHash, &k.SecretEncrypted, &k.Status,
			&models, &k.CreatedAt, &expiresAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(models), &k.AllowedModels)
		k.ExpiresAt = expiresAt.Time
		out = append(out, &k)
	}
	return out, rows.Err()
}

// DeleteKey 按 ID 删除 Key。
func (d *DB) DeleteKey(id string) error {
	_, err := d.sql.Exec(`DELETE FROM api_keys WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}
	return nil
}

// SetKeyStatus 更新 Key 状态（ban/unban）。
func (d *DB) SetKeyStatus(id, status string) error {
	res, err := d.sql.Exec(`UPDATE api_keys SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("set key status: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// --- provider_credentials ---

// Credential 是 provider_credentials 表镜像（secret 为密文）。
type Credential struct {
	ID                string
	ProviderID        string
	SecretEncrypted   string
	SecretFingerprint string // sha256(明文 secret) hex；稳定去重键，避免重启膨胀
	Status            string
	CooldownUntil     time.Time
}

// UpsertCredential 幂等写入凭证（secret 加密后调用方传入）。
// 唯一键是 (provider_id, secret_fingerprint)：同一上游凭证重启后仍命中同一行。
func (d *DB) UpsertCredential(c *Credential) error {
	_, err := d.sql.Exec(`
		INSERT INTO provider_credentials (id, provider_id, secret_encrypted,
			secret_fingerprint, status, cooldown_until)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(provider_id, secret_fingerprint) DO UPDATE SET
			id=excluded.id,
			secret_encrypted=excluded.secret_encrypted,
			status=excluded.status, cooldown_until=excluded.cooldown_until`,
		c.ID, c.ProviderID, c.SecretEncrypted, c.SecretFingerprint,
		c.Status, nullTime(c.CooldownUntil),
	)
	if err != nil {
		return fmt.Errorf("upsert credential: %w", err)
	}
	return nil
}

// UpdateCredentialFingerprint 将迁移期间的临时指纹替换为明文 secret 的真实指纹。
func (d *DB) UpdateCredentialFingerprint(providerID, id, fingerprint string) error {
	if providerID == "" || id == "" || fingerprint == "" {
		return errors.New("provider, credential id and fingerprint are required")
	}
	_, err := d.sql.Exec(`
		UPDATE provider_credentials
		SET secret_fingerprint = ?
		WHERE provider_id = ? AND id = ?`, fingerprint, providerID, id)
	if err != nil {
		return fmt.Errorf("update credential fingerprint: %w", err)
	}
	return nil
}

// ListCredentials 列出全部凭证（密文）。
func (d *DB) ListCredentials() ([]*Credential, error) {
	rows, err := d.sql.Query(`
		SELECT id, provider_id, secret_encrypted, secret_fingerprint, status, cooldown_until
		FROM provider_credentials`)
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	defer rows.Close()
	var out []*Credential
	for rows.Next() {
		var (
			c  Credential
			cd sql.NullTime
		)
		if err := rows.Scan(&c.ID, &c.ProviderID, &c.SecretEncrypted,
			&c.SecretFingerprint, &c.Status, &cd); err != nil {
			return nil, err
		}
		c.CooldownUntil = cd.Time
		out = append(out, &c)
	}
	return out, rows.Err()
}

// --- providers ---

// ModelSpec 是 providers.models 列里的模型声明（与 provider.ModelSpec 的 JSON 兼容）。
type ModelSpec struct {
	ID           string   `json:"id"`
	Enabled      *bool    `json:"enabled,omitempty"`
	Visible      *bool    `json:"visible,omitempty"`
	Context      int      `json:"context,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	// ConcurrencyCost 是该上游模型每次请求占用的并发槽系数（默认 1）。
	ConcurrencyCost float64 `json:"concurrency_cost,omitempty"`
}

// ProviderRow 是 providers 表的镜像（不含凭证密文）。
type ProviderRow struct {
	ID          string
	Name        string
	Type        string
	BaseURL     string
	Models      []string    // 已启用的模型名（由 ModelSpecs 派生）
	ModelSpecs  []ModelSpec // 完整模型声明（含禁用/不展示）
	Timeout     string
	Cooldown    string
	Concurrency int
	RPM         int
	TPM         int64
	TPH         int64
	TPD         int64
	CreatedAt   time.Time
}

// UpsertProvider 幂等写入 Provider 元数据。
// models 列同时兼容两种历史格式：["a","b"]（旧）与 [{"id":"a"}...]（新）。
func (d *DB) UpsertProvider(p *ProviderRow) error {
	payload := p.ModelSpecs
	if len(payload) == 0 && len(p.Models) > 0 {
		payload = make([]ModelSpec, len(p.Models))
		for i, id := range p.Models {
			payload[i] = ModelSpec{ID: id}
		}
	}
	if payload == nil {
		payload = []ModelSpec{}
	}
	models, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal models: %w", err)
	}
	_, err = d.sql.Exec(`
		INSERT INTO providers (id, name, type, base_url, models, timeout, cooldown,
			concurrency, rpm, tpm, tph, tpd, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			id=excluded.id, type=excluded.type, base_url=excluded.base_url,
			models=excluded.models, timeout=excluded.timeout, cooldown=excluded.cooldown,
			concurrency=excluded.concurrency, rpm=excluded.rpm,
			tpm=excluded.tpm, tph=excluded.tph, tpd=excluded.tpd`,
		p.ID, p.Name, p.Type, p.BaseURL, string(models), p.Timeout, p.Cooldown,
		p.Concurrency, p.RPM, p.TPM, p.TPH, p.TPD, p.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert provider: %w", err)
	}
	return nil
}

// ListProviders 列出全部 Provider。
func (d *DB) ListProviders() ([]*ProviderRow, error) {
	rows, err := d.sql.Query(`
		SELECT id, name, type, base_url, models, timeout, cooldown,
			concurrency, rpm, tpm, tph, tpd, created_at
		FROM providers ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list providers: %w", err)
	}
	defer rows.Close()
	var out []*ProviderRow
	for rows.Next() {
		var (
			p       ProviderRow
			models  string
			timeout sql.NullString
			cool    sql.NullString
		)
		if err := rows.Scan(&p.ID, &p.Name, &p.Type, &p.BaseURL, &models,
			&timeout, &cool, &p.Concurrency, &p.RPM,
			&p.TPM, &p.TPH, &p.TPD, &p.CreatedAt); err != nil {
			return nil, err
		}
		p.ModelSpecs, p.Models = parseProviderModels(models)
		p.Timeout = timeout.String
		p.Cooldown = cool.String
		out = append(out, &p)
	}
	return out, rows.Err()
}

// parseProviderModels 解析 models 列，兼容旧格式 ["a","b"] 与新格式 [{"id":"a","visible":false}]。
func parseProviderModels(raw string) ([]ModelSpec, []string) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return nil, nil
	}
	var specs []ModelSpec
	if err := json.Unmarshal([]byte(raw), &specs); err == nil {
		enabled := make([]string, 0, len(specs))
		for _, s := range specs {
			if strings.TrimSpace(s.ID) == "" {
				continue
			}
			if s.Enabled == nil || *s.Enabled {
				enabled = append(enabled, s.ID)
			}
		}
		return specs, enabled
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, nil
	}
	specs = make([]ModelSpec, 0, len(ids))
	enabled := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		specs = append(specs, ModelSpec{ID: id, Capabilities: []string{"chat"}})
		enabled = append(enabled, id)
	}
	return specs, enabled
}

// DeleteCredential 按 Provider 与 ID 删除凭证。
func (d *DB) DeleteCredential(providerID, id string) error {
	res, err := d.sql.Exec(`DELETE FROM provider_credentials WHERE provider_id = ? AND id = ?`, providerID, id)
	if err != nil {
		return fmt.Errorf("delete credential: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DeleteProvider 按 name 删除 Provider 及其凭证。
func (d *DB) DeleteProvider(name string) error {
	tx, err := d.sql.Begin()
	if err != nil {
		return fmt.Errorf("begin delete provider: %w", err)
	}
	rollback := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.Exec(`DELETE FROM provider_credentials WHERE provider_id = ?`, name); err != nil {
		return rollback(fmt.Errorf("delete provider credentials: %w", err))
	}
	if _, err := tx.Exec(`DELETE FROM providers WHERE name = ?`, name); err != nil {
		return rollback(fmt.Errorf("delete provider: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete provider: %w", err)
	}
	return nil
}

// --- settings ---

// GetSetting 读取一条运行时设置；不存在时返回 sql.ErrNoRows。
func (d *DB) GetSetting(key string) (string, error) {
	var value string
	if err := d.sql.QueryRow(`SELECT value FROM settings WHERE key = ?`, key).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

// SetSetting 写入或覆盖一条运行时设置。
func (d *DB) SetSetting(key, value string) error {
	if _, err := d.sql.Exec(`
		INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at`,
		key, value, time.Now()); err != nil {
		return fmt.Errorf("set setting: %w", err)
	}
	return nil
}

// --- routes ---

// ModelMapping 是模型映射的持久化结构（与 provider.ModelRoute JSON 兼容）。
type ModelMapping struct {
	Provider      string `json:"provider"`
	UpstreamModel string `json:"upstream_model"`
}

// RouteRow 是 routes 表的镜像。
type RouteRow struct {
	Model    string
	Mappings []ModelMapping
}

// UpsertRoute 幂等写入模型映射（添加或更新）。
func (d *DB) UpsertRoute(model string, mappings []ModelMapping) error {
	data, err := json.Marshal(mappings)
	if err != nil {
		return fmt.Errorf("marshal mappings: %w", err)
	}
	_, err = d.sql.Exec(`
		INSERT INTO routes (model, mappings)
		VALUES (?, ?)
		ON CONFLICT(model) DO UPDATE SET mappings=excluded.mappings`,
		model, string(data),
	)
	if err != nil {
		return fmt.Errorf("upsert route: %w", err)
	}
	return nil
}

// ListRoutes 列出全部模型映射。
func (d *DB) ListRoutes() ([]*RouteRow, error) {
	rows, err := d.sql.Query(`SELECT model, mappings FROM routes ORDER BY model`)
	if err != nil {
		return nil, fmt.Errorf("list routes: %w", err)
	}
	defer rows.Close()
	var out []*RouteRow
	for rows.Next() {
		var (
			r   RouteRow
			raw string
		)
		if err := rows.Scan(&r.Model, &raw); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &r.Mappings)
		out = append(out, &r)
	}
	return out, rows.Err()
}

// DeleteRoute 按 model 删除模型映射。
func (d *DB) DeleteRoute(model string) error {
	_, err := d.sql.Exec(`DELETE FROM routes WHERE model = ?`, model)
	if err != nil {
		return fmt.Errorf("delete route: %w", err)
	}
	return nil
}

// --- helpers ---

// boolInt 把布尔值写成 0/1（SQLite 无布尔类型）。
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
