package server

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestFailoverUsesBackupProviderGate(t *testing.T) {
	primary := newMockUpstream(0)
	primary.statusCode = 429
	primaryServer := httptest.NewServer(primary.Handler())
	defer primaryServer.Close()

	backup := newMockUpstream(250 * time.Millisecond)
	backupServer := httptest.NewServer(backup.Handler())
	defer backupServer.Close()

	cfg := failoverConfig(primaryServer.URL, backupServer.URL)
	cfg.Limits.ProviderCooldown = "2s"
	cfg.Providers[0].Concurrency = 0
	cfg.Providers[1].Concurrency = 1
	handler, _, _ := buildTestServer(t, cfg, primaryServer)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	type result struct {
		resp *http.Response
		body []byte
	}
	firstDone := make(chan result, 1)
	go func() {
		resp, body := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
		firstDone <- result{resp: resp, body: body}
	}()

	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&backup.requests) < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&backup.requests); got != 1 {
		t.Fatalf("backup did not receive the first failover request, got %d", got)
	}

	second, secondBody := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	if second.StatusCode != 200 {
		t.Fatalf("second request status=%d body=%s", second.StatusCode, secondBody)
	}
	if second.Header.Get("X-Queue-Position") == "" {
		t.Fatal("second request should queue behind the backup Provider gate")
	}

	first := <-firstDone
	if first.resp.StatusCode != 200 {
		t.Fatalf("first request status=%d body=%s", first.resp.StatusCode, first.body)
	}
	if got := atomic.LoadInt32(&backup.maxConcurrent); got != 1 {
		t.Fatalf("backup max concurrency=%d, want 1", got)
	}
	if got := atomic.LoadInt32(&backup.requests); got != 2 {
		t.Fatalf("backup requests=%d, want 2", got)
	}
}
