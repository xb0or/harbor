package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"harbor/internal/config"
	"harbor/internal/provider"
)

// TestModelCostAPIRoundTrip 模型管理接口必须能读回并保存 concurrency_cost。
func TestModelCostAPIRoundTrip(t *testing.T) {
	ups := httptest.NewServer(newMockUpstream(0).Handler())
	t.Cleanup(ups.Close)

	cfg := testConfig()
	srv, _, _ := buildTestServerWithServer(t, cfg, ups)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	getModels := func() map[string]float64 {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/providers/mock/models", nil)
		req.Header.Set("Authorization", "Bearer "+cfg.Admin.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body struct {
			Models []struct {
				ID              string  `json:"id"`
				ConcurrencyCost float64 `json:"concurrency_cost"`
			} `json:"models"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, m := range body.Models {
			out[m.ID] = m.ConcurrencyCost
		}
		return out
	}

	if got := getModels()["mock-1"]; got != 1 {
		t.Fatalf("default cost = %v, want 1", got)
	}
	body := `{"models":[{"id":"mock-1","enabled":true,"context":0,"capabilities":["chat"],"concurrency_cost":0.25}]}`
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/providers/mock/models", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+cfg.Admin.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}
	if got := srv.mgr.ModelConcurrencyCost("mock-1", "mock"); got != 0.25 {
		t.Fatalf("manager cost = %v, want 0.25", got)
	}
	if got := getModels()["mock-1"]; got != 0.25 {
		t.Fatalf("round-trip cost = %v, want 0.25", got)
	}
}

// 4 个系数 0.25 的轻量请求应同时打到上游；未加权时最多只有 1 个。
func TestModelConcurrencyCostAllowsFractionalOvercommit(t *testing.T) {
	mock := newMockUpstream(300 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Providers[0].Concurrency = 1
	cfg.Providers[0].Models = []string{"light"}
	cfg.Routes = map[string]config.Route{"light": {Providers: []string{"mock"}}}
	cfg.Keys[0].AllowedModels = []string{"light"}
	srv, _, _ := buildTestServerWithServer(t, cfg, ups)
	if p, ok := srv.mgr.Get("mock"); ok {
		p.SetModelSpecs([]provider.ModelSpec{{ID: "light", ConcurrencyCost: 0.25}})
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	const n = 4
	var wg sync.WaitGroup
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, _ := doChat(t, ts.URL, cfg.Keys[0].Secret, "light", nil)
			codes[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()
	for i, code := range codes {
		if code != 200 {
			t.Fatalf("request %d status=%d", i, code)
		}
	}
	if got := atomic.LoadInt32(&mock.maxConcurrent); got != n {
		t.Fatalf("max concurrent upstream = %d, want %d (0.25 × 4 should fill one slot)", got, n)
	}
}

// TestModelConcurrencyCostHeavyBlocksLight 系数 3 的重型请求占满 3 个槽时，
// 系数 0.25 的轻量请求必须排队；未加权时两者会同时打到上游。
func TestModelConcurrencyCostHeavyBlocksLight(t *testing.T) {
	mock := newMockUpstream(300 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Providers[0].Concurrency = 3
	cfg.Providers[0].Models = []string{"heavy", "light"}
	cfg.Routes = map[string]config.Route{
		"heavy": {Providers: []string{"mock"}},
		"light": {Providers: []string{"mock"}},
	}
	cfg.Keys[0].AllowedModels = []string{"heavy", "light"}
	srv, _, _ := buildTestServerWithServer(t, cfg, ups)
	if p, ok := srv.mgr.Get("mock"); ok {
		p.SetModelSpecs([]provider.ModelSpec{
			{ID: "heavy", ConcurrencyCost: 3},
			{ID: "light", ConcurrencyCost: 0.25},
		})
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	var wg sync.WaitGroup
	codes := make([]int, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp, _ := doChat(t, ts.URL, cfg.Keys[0].Secret, "heavy", nil)
		codes[0] = resp.StatusCode
	}()
	// 等重型请求真正到达上游后再发轻量请求，确保它在队头。
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&mock.concurrent) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("heavy request did not reach upstream")
		}
		time.Sleep(5 * time.Millisecond)
	}
	go func() {
		defer wg.Done()
		resp, _ := doChat(t, ts.URL, cfg.Keys[0].Secret, "light", nil)
		codes[1] = resp.StatusCode
	}()
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&mock.concurrent); got != 1 {
		t.Fatalf("light request ran concurrently with heavy: concurrent=%d", got)
	}
	wg.Wait()
	for i, code := range codes {
		if code != 200 {
			t.Fatalf("request %d status=%d", i, code)
		}
	}
	if got := atomic.LoadInt32(&mock.maxConcurrent); got != 1 {
		t.Fatalf("max concurrent upstream = %d, want 1", got)
	}
}
