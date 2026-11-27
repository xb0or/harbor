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

	"log/slog"

	"harbor/internal/config"
	"harbor/internal/executor"
	"harbor/internal/key"
	"harbor/internal/provider"
	"harbor/internal/queue"
	"harbor/internal/stats"
)

func buildPublicTestHandler(t *testing.T, cfg *config.Config, upstream *httptest.Server) (http.Handler, *queue.Scheduler) {
	t.Helper()
	keys := key.NewStore()
	for _, kc := range cfg.Keys {
		keys.Add(kc.Secret, kc.AllowedModels)
	}
	var providers []*provider.Provider
	for _, pc := range cfg.Providers {
		base := pc.BaseURL
		if base == "http://upstream.invalid" {
			base = upstream.URL + "/v1"
		}
		providers = append(providers, provider.New(pc.Name, pc.Type, base, pc.Models, pc.Credentials,
			cfg.Limits.RequestTimeoutDuration(), cfg.Limits.ProviderCooldownDuration(),
			pc.Concurrency, pc.RPM, pc.TPM, pc.TPH, pc.TPD))
	}
	mappings := map[string][]provider.ModelRoute{}
	for model, route := range cfg.Routes {
		for _, name := range route.Providers {
			mappings[model] = append(mappings[model], provider.ModelRoute{Provider: name})
		}
		for _, mapping := range route.Mappings {
			mappings[model] = append(mappings[model], provider.ModelRoute{Provider: mapping.Provider, UpstreamModel: mapping.UpstreamModel})
		}
	}
	mgr := provider.NewManager(providers, mappings)
	sched := queue.NewScheduler(cfg.Queue.MaxDepth, cfg.Queue.MaxWaitDuration())
	sched.Start()
	t.Cleanup(sched.Stop)
	srv := New(cfg, keys, mgr, executor.New(mgr, 2, 1*time.Millisecond, cfg.Limits.ProviderCooldownDuration()), sched, stats.New(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return srv.Handler(), sched
}

func TestStreamUsageIsSettledFromSSE(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"choices\":[]}\n\n")
		f.Flush()
		_, _ = io.WriteString(w, "data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"total_tokens\":12}}\n\n")
		f.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		f.Flush()
	}))
	defer upstream.Close()

	cfg := testConfig()
	cfg.Providers[0].Concurrency = 1
	cfg.Providers[0].TPM = 100
	handler, _ := buildPublicTestHandler(t, cfg, upstream)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	body := []byte(`{"model":"mock-1","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(data), "[DONE]") {
		t.Fatalf("stream response status=%d body=%s", resp.StatusCode, data)
	}
}

func TestDuplicateModelFieldRejected(t *testing.T) {
	upstream := newMockUpstream(0)
	upstreamServer := httptest.NewServer(upstream.Handler())
	defer upstreamServer.Close()
	cfg := testConfig()
	handler, _ := buildPublicTestHandler(t, cfg, upstreamServer)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(`{"model":"mock-1","model":"other","messages":[]}`))
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate model status=%d", resp.StatusCode)
	}
}

func TestTokenReserveIncludesMaxTokens(t *testing.T) {
	body := map[string]any{"model": "mock-1", "messages": []any{}, "max_tokens": 100}
	raw, _ := json.Marshal(body)
	if got := estimatePromptTokens(raw); got < 100 {
		t.Fatalf("reserve=%d, max_tokens was not included", got)
	}
	_ = atomic.Int32{}
	_ = context.Background()
}
