package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequeueSingleProviderRecoversOnOriginalRequest(t *testing.T) {
	var calls atomic.Int32
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(429)
			io.WriteString(w, `{"error":{"code":"rate_limit_exceeded"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"recovered"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	defer ups.Close()
	cfg := testConfig()
	cfg.Limits.ProviderCooldown = "70ms"
	cfg.Limits.BackoffBase = "5ms"
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	start := time.Now()
	resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("temporary 429 escaped instead of requeue: status=%d body=%s calls=%d", resp.StatusCode, data, calls.Load())
	}
	if calls.Load() != 2 || time.Since(start) < 60*time.Millisecond {
		t.Fatalf("cooldown not honored: calls=%d elapsed=%v", calls.Load(), time.Since(start))
	}
	log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
	if log.Attempts != 2 || log.Status != "success" || log.QueueWaitMs < 60 {
		t.Fatalf("original request audit=%+v", log)
	}
	today := requeueStats(t, ts.URL, cfg.Admin.Token)["today"].(map[string]any)
	if today["requests"] != float64(1) || today["success"] != float64(1) || today["failed"] != float64(0) || today["queued"] != float64(1) {
		t.Fatalf("intermediate retry counted as another request: %v", today)
	}
}

func TestRequeueKeepsAttemptBudget(t *testing.T) {
	var calls atomic.Int32
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(429)
	}))
	defer ups.Close()
	cfg := testConfig()
	cfg.Limits.MaxProviderAttempts = 3
	cfg.Limits.ProviderCooldown = "20ms"
	cfg.Limits.BackoffBase = "5ms"
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	if resp.StatusCode != 429 || calls.Load() != 3 {
		t.Fatalf("budget not applied across requeues: status=%d calls=%d body=%s", resp.StatusCode, calls.Load(), data)
	}
	log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
	if log.Attempts != 3 {
		t.Fatalf("attempts reset: %+v", log)
	}
}

func TestQueueWaitExcludesUpstreamExecution(t *testing.T) {
	mock := newMockUpstream(150 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	resp, _ := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
	if log.QueueWaitMs >= 100 {
		t.Fatalf("execution mislabeled queue wait: %+v", log)
	}
}

func TestRunningIncludesActiveStream(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer ups.Close()
	cfg := testConfig()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", map[string]any{"stream": true})
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	running := sched.Stats().Running
	close(release)
	<-done
	if running != 1 {
		t.Fatalf("active SSE missing from running: %d", running)
	}
}

// Used by further requeue tests to inspect the in-memory admin snapshot.
func requeueStats(t *testing.T, base, token string) map[string]any {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/api/stats", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	return got
}
