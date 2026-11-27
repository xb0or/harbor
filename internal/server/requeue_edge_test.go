package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequeueRetryAfterAndResponsesHeartbeat(t *testing.T) {
	var calls atomic.Int32
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"id\":\"chatcmpl-retry\",\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\ndata: [DONE]\n\n")
	}))
	defer ups.Close()
	cfg := testConfig()
	cfg.Limits.ProviderCooldown = "20ms"
	cfg.Limits.BackoffBase = "5ms"
	cfg.Queue.Keepalive = "20ms"
	h, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(h)
	defer ts.Close()
	start := time.Now()
	resp, data := doJSON(t, ts.URL, "/v1/responses", cfg.Keys[0].Secret, map[string]any{"model": "mock-1", "input": "hi", "stream": true})
	if resp.StatusCode != 200 || calls.Load() != 2 || time.Since(start) < 950*time.Millisecond {
		t.Fatalf("Retry-After not honored: status=%d calls=%d elapsed=%v body=%s", resp.StatusCode, calls.Load(), time.Since(start), data)
	}
	text := string(data)
	if !strings.Contains(text, ": keepalive") || strings.Count(text, "event: response.completed") != 1 || strings.Contains(text, "response.failed") {
		t.Fatalf("intermediate failure leaked: %s", data)
	}
	log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
	if log.Attempts != 2 || log.Status != "success" || log.TokensIn != 1 || log.TokensOut != 1 {
		t.Fatalf("audit=%+v", log)
	}
}

func TestRequeueRespectsOriginalTimeout(t *testing.T) {
	for _, totalFirst := range []bool{false, true} {
		var calls atomic.Int32
		ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
		}))
		cfg := testConfig()
		cfg.Limits.ProviderCooldown = "20ms"
		cfg.Limits.BackoffBase = "5ms"
		if totalFirst {
			cfg.Limits.RequestTimeout = "90ms"
		} else {
			cfg.Queue.MaxWait = "90ms"
		}
		h, _, _ := buildTestServer(t, cfg, ups)
		ts := httptest.NewServer(h)
		start := time.Now()
		resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
		want := 429
		if totalFirst {
			want = 504
		}
		log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
		ts.Close()
		ups.Close()
		if resp.StatusCode != want || calls.Load() != 1 || time.Since(start) > 500*time.Millisecond || log.Attempts != 1 {
			t.Fatalf("deadline changed: status=%d calls=%d log=%+v body=%s", resp.StatusCode, calls.Load(), log, data)
		}
	}
}

func TestRequeuePermanentErrorsDoNotRetry(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{{400, `{"error":{"message":"rate_limit is an invalid input field"}}`}, {429, `{"error":{"code":"insufficient_quota"}}`}, {401, `{"error":{"code":"invalid_api_key"}}`}} {
		var calls atomic.Int32
		ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(tc.status)
			io.WriteString(w, tc.body)
		}))
		cfg := testConfig()
		cfg.Limits.ProviderCooldown = "20ms"
		h, _, _ := buildTestServer(t, cfg, ups)
		ts := httptest.NewServer(h)
		resp, _ := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
		ts.Close()
		ups.Close()
		if calls.Load() != 1 || resp.StatusCode == 200 {
			t.Fatalf("permanent error retried: status=%d calls=%d", tc.status, calls.Load())
		}
	}
}

func TestRequeueRPMIsChargedForEveryAttempt(t *testing.T) {
	var calls atomic.Int32
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(429) }))
	defer ups.Close()
	cfg := testConfig()
	cfg.Providers[0].RPM = 1
	cfg.Queue.MaxWait = "100ms"
	cfg.Limits.ProviderCooldown = "10ms"
	cfg.Limits.BackoffBase = "5ms"
	h, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(h)
	defer ts.Close()
	resp, _ := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	if resp.StatusCode != 429 || calls.Load() != 1 {
		t.Fatalf("retry bypassed RPM: status=%d calls=%d", resp.StatusCode, calls.Load())
	}
}

func TestRequeueWaitsForBusyBackupWithoutExceedingConcurrency(t *testing.T) {
	var entered atomic.Int32
	both := make(chan struct{})
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if entered.Add(1) == 2 {
			close(both)
		}
		select {
		case <-both:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(429)
	}))
	defer primary.Close()
	backup := newMockUpstream(40 * time.Millisecond)
	upBackup := httptest.NewServer(backup.Handler())
	defer upBackup.Close()
	cfg := failoverConfig(primary.URL, upBackup.URL)
	cfg.Providers[0].Concurrency = 2
	cfg.Providers[1].Concurrency = 1
	cfg.Limits.ProviderCooldown = "1s"
	cfg.Limits.BackoffBase = "5ms"
	h, _, _ := buildTestServer(t, cfg, primary)
	ts := httptest.NewServer(h)
	defer ts.Close()
	statuses := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() { resp, _ := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil); statuses <- resp.StatusCode }()
	}
	for i := 0; i < 2; i++ {
		select {
		case status := <-statuses:
			if status != 200 {
				t.Errorf("busy backup returned %d instead of waiting", status)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("backup not resumed")
		}
	}
	if atomic.LoadInt32(&backup.requests) != 2 || atomic.LoadInt32(&backup.maxConcurrent) != 1 {
		t.Fatalf("backup requests=%d max=%d", backup.requests, backup.maxConcurrent)
	}
}

func TestRequeueClientDisconnectDuringCooldown(t *testing.T) {
	var calls atomic.Int32
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
	}))
	defer ups.Close()
	cfg := testConfig()
	cfg.Limits.ProviderCooldown = "20ms"
	cfg.Limits.BackoffBase = "5ms"
	cfg.Queue.Keepalive = "10ms"
	h, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(h)
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw, _ := json.Marshal(map[string]any{"model": "mock-1", "input": "hi", "stream": true})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/responses", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for sched.Stats().Depth != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	resp.Body.Close()
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st := sched.Stats()
		if st.Depth == 0 && st.Running == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	st := sched.Stats()
	if st.Depth != 0 || st.Running != 0 || calls.Load() != 1 {
		t.Fatalf("cancelled retry leaked: %+v calls=%d", st, calls.Load())
	}
}
