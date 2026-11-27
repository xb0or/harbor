package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequeueConcurrentRequestsKeepProviderLimit(t *testing.T) {
	var active, maximum, calls atomic.Int32
	var mu sync.Mutex
	seen := map[string]int{}
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if old >= n || maximum.CompareAndSwap(old, n) {
				break
			}
		}
		calls.Add(1)
		var body struct{ Messages []struct{ Content string } }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Messages) != 1 {
			http.Error(w, "bad test body", 400)
			return
		}
		id := body.Messages[0].Content
		mu.Lock()
		seen[id]++
		attempt := seen[id]
		mu.Unlock()
		time.Sleep(15 * time.Millisecond)
		if attempt == 1 {
			w.WriteHeader(429)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	defer ups.Close()
	cfg := testConfig()
	cfg.Providers[0].Concurrency = 2
	cfg.Limits.ProviderCooldown = "20ms"
	cfg.Limits.BackoffBase = "5ms"
	h, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(h)
	defer ts.Close()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, data := doJSON(t, ts.URL, "/v1/chat/completions", cfg.Keys[0].Secret, map[string]any{"model": "mock-1", "messages": []any{map[string]any{"role": "user", "content": fmt.Sprint(i)}}})
			if resp.StatusCode != 200 {
				t.Errorf("request %d failed: %d %s", i, resp.StatusCode, data)
				return
			}
			log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
			if log.Attempts != 2 || log.Status != "success" {
				t.Errorf("request %d lost audit: %+v", i, log)
			}
		}(i)
	}
	wg.Wait()
	if maximum.Load() > 2 || calls.Load() != 20 || active.Load() != 0 {
		t.Fatalf("upstream max=%d calls=%d active=%d", maximum.Load(), calls.Load(), active.Load())
	}
	today := requeueStats(t, ts.URL, cfg.Admin.Token)["today"].(map[string]any)
	if today["requests"] != float64(10) || today["success"] != float64(10) || today["failed"] != float64(0) {
		t.Fatalf("retry inflated client stats: %v", today)
	}
}
