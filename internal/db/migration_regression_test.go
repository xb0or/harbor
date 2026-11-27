package db

import (
	"strings"
	"testing"
	"time"
)

func TestMigratePreservesLegacyData(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir + "/harbor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	encKey := []byte("01234567890123456789012345678901")
	ciphertext, err := Encrypt(encKey, "legacy-provider-secret")
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().UTC()
	for _, stmt := range []string{
		`CREATE TABLE provider_credentials (
			id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, secret_encrypted TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'active', cooldown_until DATETIME)`,
		`CREATE TABLE api_keys (
			id TEXT PRIMARY KEY, prefix TEXT NOT NULL, key_hash TEXT NOT NULL UNIQUE,
			status TEXT NOT NULL DEFAULT 'active', allowed_models TEXT NOT NULL DEFAULT '[]',
			concurrency INTEGER NOT NULL DEFAULT 0, rpm INTEGER NOT NULL DEFAULT 0,
			tpm INTEGER NOT NULL DEFAULT 0, tpd INTEGER NOT NULL DEFAULT 0,
			priority INTEGER NOT NULL DEFAULT 0, created_at DATETIME NOT NULL, expires_at DATETIME)`,
		`CREATE TABLE routes (model TEXT PRIMARY KEY, mappings TEXT NOT NULL DEFAULT '[]')`,
	} {
		if _, err := store.sql.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.sql.Exec(`INSERT INTO provider_credentials
		(id, provider_id, secret_encrypted, status) VALUES (?, ?, ?, ?)`,
		"legacy-cred", "legacy-provider", ciphertext, "active"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.sql.Exec(`INSERT INTO api_keys
		(id, prefix, key_hash, status, allowed_models, concurrency, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"legacy-key", "legacy", "legacy-hash", "banned", `["m1"]`, 2, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.sql.Exec(`INSERT INTO routes (model, mappings) VALUES (?, ?)`,
		"m1", `["legacy-provider"]`); err != nil {
		t.Fatal(err)
	}

	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	creds, err := store.ListCredentials()
	if err != nil || len(creds) != 1 {
		t.Fatalf("credentials after migration: err=%v rows=%+v", err, creds)
	}
	if creds[0].ID != "legacy-cred" || !strings.HasPrefix(creds[0].SecretFingerprint, "legacy:") {
		t.Fatalf("legacy credential metadata lost: %+v", creds[0])
	}
	if got, err := Decrypt(encKey, creds[0].SecretEncrypted); err != nil || got != "legacy-provider-secret" {
		t.Fatalf("legacy credential ciphertext lost: value=%q err=%v", got, err)
	}

	keys, err := store.ListKeys()
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys after migration: err=%v rows=%+v", err, keys)
	}
	if keys[0].ID != "legacy-key" || keys[0].Status != "banned" || len(keys[0].AllowedModels) != 1 || keys[0].AllowedModels[0] != "m1" {
		t.Fatalf("legacy key metadata lost: %+v", keys[0])
	}

	routes, err := store.ListRoutes()
	if err != nil || len(routes) != 1 || len(routes[0].Mappings) != 1 || routes[0].Mappings[0].Provider != "legacy-provider" {
		t.Fatalf("legacy routes after migration: err=%v rows=%+v", err, routes)
	}
}

func TestMigrateRequestsProviderColumn(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir + "/harbor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// 旧版 requests 表：无 provider 列，含既有记录。
	legacyRequests := `CREATE TABLE requests (
		request_id    TEXT PRIMARY KEY,
		key_id        TEXT NOT NULL,
		model         TEXT NOT NULL,
		status        TEXT NOT NULL,
		error_code    TEXT,
		queued_at     DATETIME NOT NULL,
		started_at    DATETIME,
		finished_at   DATETIME,
		queue_wait_ms INTEGER NOT NULL DEFAULT 0,
		attempts      INTEGER NOT NULL DEFAULT 0,
		tokens_in     INTEGER NOT NULL DEFAULT 0,
		tokens_out    INTEGER NOT NULL DEFAULT 0)`
	if _, err := store.sql.Exec(legacyRequests); err != nil {
		t.Fatal(err)
	}
	if _, err := store.sql.Exec(`INSERT INTO requests
		(request_id, key_id, model, status, error_code, queued_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"hr_legacy", "k1", "m1", "success", "", time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	columns, err := tableColumns(store.sql, "requests")
	if err != nil {
		t.Fatal(err)
	}
	if !columns["provider"] {
		t.Fatal("requests.provider column missing after migration")
	}

	rec, err := store.GetRequest("hr_legacy")
	if err != nil {
		t.Fatalf("legacy request lost: %v", err)
	}
	if rec.Model != "m1" || rec.Status != "success" {
		t.Fatalf("legacy request row corrupted: %+v", rec)
	}
}
