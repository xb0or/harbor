package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// openTest 打开临时目录下的测试库。
func openTest(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	d, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return d, dir
}

func TestMigrateIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idem.db")
	d, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()
	if err := d.Migrate(); err != nil {
		t.Fatalf("Migrate #1: %v", err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatalf("Migrate #2 (same conn): %v", err)
	}
	// 新连接再迁移：表已存在也必须幂等
	d2, err := Open(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()
	if err := d2.Migrate(); err != nil {
		t.Fatalf("Migrate #3 (new conn): %v", err)
	}
}

func TestMigratePreservesCurrentRoutes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "routes.db")
	d, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()
	if err := d.Migrate(); err != nil {
		t.Fatalf("Migrate #1: %v", err)
	}
	want := []ModelMapping{{Provider: "airouter", UpstreamModel: "deepseek-chat"}}
	if err := d.UpsertRoute("DeepSeek-V4-Flash", want); err != nil {
		t.Fatalf("UpsertRoute: %v", err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatalf("Migrate #2: %v", err)
	}
	routes, err := d.ListRoutes()
	if err != nil {
		t.Fatalf("ListRoutes: %v", err)
	}
	if len(routes) != 1 || routes[0].Model != "DeepSeek-V4-Flash" || !reflect.DeepEqual(routes[0].Mappings, want) {
		t.Fatalf("current route was lost during migration: %+v", routes)
	}
}

// TestOpenCreatesParentDir 回归：Open 必须自动创建不存在的父目录
// （真实部署时 db_path 指向尚未创建的目录，否则 SQLite 报 unable to open database file）。
func TestOpenCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	deep := filepath.Join(dir, "nested", "deeper", "data", "harbor.db")
	d, err := Open(deep)
	if err != nil {
		t.Fatalf("Open with missing parent dir: %v", err)
	}
	defer d.Close()
	if _, err := os.Stat(filepath.Dir(deep)); err != nil {
		t.Fatalf("parent dir not created: %v", err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := os.Stat(deep); err != nil {
		t.Fatalf("db file not created: %v", err)
	}
	// 再开一次验证可重入（父目录已存在路径）
	d2, err := Open(deep)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer d2.Close()
}

func TestRequestRoundTrip(t *testing.T) {
	d, _ := openTest(t)

	in := &Request{
		RequestID:     "hr_test_req_1",
		KeyID:         "key-1",
		Model:         "gpt-4o",
		Status:        "success",
		ErrorCode:     "",
		QueuedAt:      time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC),
		StartedAt:     time.Date(2026, 8, 28, 10, 0, 1, 0, time.UTC),
		FinishedAt:    time.Date(2026, 8, 28, 10, 0, 2, 0, time.UTC),
		QueueWaitMs:   1234,
		Attempts:      2,
		TokensIn:      100,
		TokensOut:     50,
		ThinkingLevel: "max",
	}
	if err := d.RecordRequest(in); err != nil {
		t.Fatalf("RecordRequest: %v", err)
	}

	got, err := d.GetRequest("hr_test_req_1")
	if err != nil {
		t.Fatalf("GetRequest: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, in)
	}

	// 覆盖更新：status 变化 + 空 Started/Finished（null 处理）
	upd := &Request{
		RequestID:     "hr_test_req_1",
		KeyID:         "key-1",
		Model:         "gpt-4o",
		Status:        "error",
		ErrorCode:     "upstream_error",
		QueuedAt:      time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC),
		QueueWaitMs:   99,
		Attempts:      1,
		ThinkingLevel: "low",
	}
	if err := d.RecordRequest(upd); err != nil {
		t.Fatalf("RecordRequest(update): %v", err)
	}
	got2, err := d.GetRequest("hr_test_req_1")
	if err != nil {
		t.Fatalf("GetRequest(update): %v", err)
	}
	if got2.Status != "error" || got2.ErrorCode != "upstream_error" {
		t.Fatalf("update not applied: %+v", got2)
	}
	if got2.ThinkingLevel != "low" {
		t.Fatalf("thinking level update lost: %+v", got2)
	}
	if !got2.StartedAt.IsZero() || !got2.FinishedAt.IsZero() {
		t.Fatalf("null time should be zero: %+v", got2)
	}
}

func TestGetRequestNotFound(t *testing.T) {
	d, _ := openTest(t)
	if _, err := d.GetRequest("nope"); err != sql.ErrNoRows {
		t.Fatalf("want sql.ErrNoRows, got %v", err)
	}
}

func TestRecentRequests(t *testing.T) {
	d, _ := openTest(t)
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		ft := base.Add(time.Duration(i) * time.Minute)
		r := &Request{
			RequestID:  "hr_recent_" + string(rune('a'+i)),
			KeyID:      "k",
			Model:      "m",
			Status:     "success",
			QueuedAt:   ft,
			FinishedAt: ft,
		}
		if err := d.RecordRequest(r); err != nil {
			t.Fatalf("RecordRequest #%d: %v", i, err)
		}
	}
	got, err := d.RecentRequests(3)
	if err != nil {
		t.Fatalf("RecentRequests: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
	// 按 finished_at 倒序：e, d, c
	want := []string{"hr_recent_e", "hr_recent_d", "hr_recent_c"}
	for i, r := range got {
		if r.RequestID != want[i] {
			t.Fatalf("row %d: want %s, got %s", i, want[i], r.RequestID)
		}
	}
	// limit 越界回落默认 100
	if _, err := d.RecentRequests(99999); err != nil {
		t.Fatalf("RecentRequests(clamp): %v", err)
	}

	filtered, total, err := d.ListRequests(RequestFilter{Q: "hr_recent_c", Limit: 10})
	if err != nil {
		t.Fatalf("ListRequests q: %v", err)
	}
	if total != 1 || len(filtered) != 1 || filtered[0].RequestID != "hr_recent_c" {
		t.Fatalf("ListRequests q got total=%d rows=%v", total, filtered)
	}
	page, total, err := d.ListRequests(RequestFilter{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("ListRequests page: %v", err)
	}
	if total != 5 || len(page) != 2 || page[0].RequestID != "hr_recent_c" {
		t.Fatalf("ListRequests page total=%d first=%v", total, page)
	}
	okStatus, total, err := d.ListRequests(RequestFilter{Status: "success", Limit: 10})
	if err != nil {
		t.Fatalf("ListRequests status: %v", err)
	}
	if total != 5 || len(okStatus) != 5 {
		t.Fatalf("ListRequests status total=%d len=%d", total, len(okStatus))
	}
}

func TestKeyCRUD(t *testing.T) {
	d, _ := openTest(t)

	k := &Key{
		ID:              "key-1",
		Prefix:          "hr_abc",
		KeyHash:         "sha256:deadbeef",
		SecretEncrypted: "encrypted-key-secret",
		Status:          "active",
		AllowedModels:   []string{"gpt-4o", "gpt-4o-mini"},
		CreatedAt:       time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC),
	}
	if err := d.UpsertKey(k); err != nil {
		t.Fatalf("UpsertKey: %v", err)
	}

	keys, err := d.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("want 1 key, got %d", len(keys))
	}
	got := keys[0]
	if got.ID != k.ID || got.Prefix != k.Prefix || got.KeyHash != k.KeyHash || got.SecretEncrypted != k.SecretEncrypted ||
		!reflect.DeepEqual(got.AllowedModels, k.AllowedModels) {
		t.Fatalf("key mismatch:\n got %+v\nwant %+v", got, k)
	}
	if !got.CreatedAt.Equal(k.CreatedAt) || !got.ExpiresAt.IsZero() {
		t.Fatalf("time fields wrong: created=%v expires=%v", got.CreatedAt, got.ExpiresAt)
	}

	// Upsert 覆盖
	k2 := *k
	k2.AllowedModels = []string{"gpt-4o"}
	k2.SecretEncrypted = ""
	if err := d.UpsertKey(&k2); err != nil {
		t.Fatalf("UpsertKey(update): %v", err)
	}
	keys, _ = d.ListKeys()
	if len(keys) != 1 {
		t.Fatalf("upsert should update in place: %+v", keys)
	}
	if len(keys[0].AllowedModels) != 1 || keys[0].AllowedModels[0] != "gpt-4o" {
		t.Fatalf("models update failed: %+v", keys[0].AllowedModels)
	}
	if keys[0].SecretEncrypted != k.SecretEncrypted {
		t.Fatalf("metadata update must not erase encrypted secret: %+v", keys[0])
	}

	// SetKeyStatus
	if err := d.SetKeyStatus("key-1", "banned"); err != nil {
		t.Fatalf("SetKeyStatus: %v", err)
	}
	keys, _ = d.ListKeys()
	if keys[0].Status != "banned" {
		t.Fatalf("status not updated: %+v", keys[0])
	}
	// 不存在 → ErrNoRows
	if err := d.SetKeyStatus("missing", "banned"); err != sql.ErrNoRows {
		t.Fatalf("want sql.ErrNoRows, got %v", err)
	}
}

func TestCredentialCRUD(t *testing.T) {
	d, _ := openTest(t)
	cd := time.Date(2026, 8, 28, 11, 0, 0, 0, time.UTC)

	c := &Credential{
		ID:                "cred-1",
		ProviderID:        "duck",
		SecretEncrypted:   "base64blob",
		SecretFingerprint: "fp-duck-1",
		Status:            "active",
		CooldownUntil:     cd,
	}
	if err := d.UpsertCredential(c); err != nil {
		t.Fatalf("UpsertCredential: %v", err)
	}
	list, err := d.ListCredentials()
	if err != nil {
		t.Fatalf("ListCredentials: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("want 1 credential, got %d", len(list))
	}
	got := list[0]
	if got.ID != c.ID || got.ProviderID != c.ProviderID ||
		got.SecretEncrypted != c.SecretEncrypted || got.Status != c.Status {
		t.Fatalf("credential mismatch:\n got %+v\nwant %+v", got, c)
	}
	if !got.CooldownUntil.Equal(cd) {
		t.Fatalf("cooldown mismatch: %v", got.CooldownUntil)
	}

	// 覆盖
	c2 := *c
	c2.Status = "disabled"
	if err := d.UpsertCredential(&c2); err != nil {
		t.Fatalf("UpsertCredential(update): %v", err)
	}
	list, _ = d.ListCredentials()
	if len(list) != 1 || list[0].Status != "disabled" {
		t.Fatalf("credential update failed: %+v", list)
	}
	if err := d.DeleteCredential("duck", "cred-1"); err != nil {
		t.Fatalf("DeleteCredential: %v", err)
	}
	list, _ = d.ListCredentials()
	if len(list) != 0 {
		t.Fatalf("credential delete failed: %+v", list)
	}
}

// TestCredentialMirrorIdempotent 回归：同一上游凭证（同 provider + 同指纹）
// 即使 id/密文每次不同（AES-GCM 随机 nonce + 随机 crd_ id），重启镜像也不得膨胀。
func TestCredentialMirrorIdempotent(t *testing.T) {
	d, _ := openTest(t)
	// 模拟两次启动镜像：id 与密文都不同，指纹相同
	for i := 0; i < 3; i++ {
		c := &Credential{
			ID:                fmt.Sprintf("crd_rand_%d", i),
			ProviderID:        "smoke",
			SecretEncrypted:   fmt.Sprintf("ciphertext-variant-%d", i),
			SecretFingerprint: "fp-same-secret",
			Status:            "active",
		}
		if err := d.UpsertCredential(c); err != nil {
			t.Fatalf("UpsertCredential #%d: %v", i, err)
		}
	}
	list, err := d.ListCredentials()
	if err != nil {
		t.Fatalf("ListCredentials: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("want exactly 1 row after 3 mirror upserts, got %d: %+v", len(list), list)
	}
	if list[0].ID != "crd_rand_2" || list[0].SecretEncrypted != "ciphertext-variant-2" {
		t.Fatalf("row should hold latest mirror data: %+v", list[0])
	}
	// 同 provider 不同 secret → 允许共存
	other := &Credential{
		ID: "crd_other", ProviderID: "smoke",
		SecretEncrypted: "blob2", SecretFingerprint: "fp-other-secret",
	}
	if err := d.UpsertCredential(other); err != nil {
		t.Fatalf("UpsertCredential(other): %v", err)
	}
	list, _ = d.ListCredentials()
	if len(list) != 2 {
		t.Fatalf("want 2 rows with distinct secrets, got %d", len(list))
	}
}

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	token := "sk-abcdef1234567890"
	blob, err := Encrypt(key, token)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if blob == token {
		t.Fatal("encrypted blob must not equal plaintext")
	}
	plain, err := Decrypt(key, blob)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if plain != token {
		t.Fatalf("round trip mismatch: %q != %q", plain, token)
	}

	// 错误密钥 → GCM auth 失败
	wrong := make([]byte, 32)
	if _, err := Decrypt(wrong, blob); err == nil {
		t.Fatal("Decrypt with wrong key should fail")
	}

	// 非法 base64
	if _, err := Decrypt(key, "!!!not-base64!!!"); err == nil {
		t.Fatal("Decrypt with bad base64 should fail")
	}

	// 太短（不足 nonce 长度）
	if _, err := Decrypt(key, "aGk="); err == nil {
		t.Fatal("Decrypt with short blob should fail")
	}

	// 篡改密文
	raw := []byte(blob)
	raw[len(raw)-1] ^= 0xFF
	if _, err := Decrypt(key, string(raw)); err == nil {
		t.Fatal("Decrypt with tampered blob should fail")
	}
}

func TestLoadOrCreateKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "harbor.key")

	k1, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatalf("LoadOrCreateKey #1: %v", err)
	}
	if len(k1) != 32 {
		t.Fatalf("key length = %d, want 32", len(k1))
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	// Windows 没有 POSIX 权限位，Mode().Perm() 不反映 0600；类 Unix 平台严格校验
	if runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file perm = %o, want 600", fi.Mode().Perm())
	}

	// 幂等：再次加载同一文件返回相同 key
	k2, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatalf("LoadOrCreateKey #2: %v", err)
	}
	if string(k1) != string(k2) {
		t.Fatal("LoadOrCreateKey must not regenerate existing key")
	}

	// 错误长度 → 报错
	bad := filepath.Join(dir, "bad.key")
	if err := os.WriteFile(bad, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateKey(bad); err == nil {
		t.Fatal("LoadOrCreateKey with wrong-length file should fail")
	}
}

func TestNullHelpers(t *testing.T) {
	if nullStr("") != nil {
		t.Fatal("nullStr(\"\") should be nil")
	}
	if nullStr("x") != "x" {
		t.Fatal("nullStr should passthrough")
	}
	if nullTime(time.Time{}) != nil {
		t.Fatal("nullTime(zero) should be nil")
	}
	if nullTime(time.Unix(1, 0)) == nil {
		t.Fatal("nullTime(nonzero) should not be nil")
	}
}
