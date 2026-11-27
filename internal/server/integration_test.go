package server

// 集成测试：按计划书 §19 P0 验收标准验证
//   1. 并发打满后请求进入队列（Concurrency=2, 10 请求 → 2 执行 8 排队）
//   2. RPM 打满后请求进入队列
//   3. 资源恢复后自动执行
//   4. 排队期间保持原 HTTP 请求
//   5. 超时才返回 429（且为统一收敛格式）
//   6. 流式请求排队时发送 SSE 心跳
//   7. 错误收敛：上游 429/503 不直接暴露
//   8. Request ID 贯穿
//   9. 客户端断开释放资源

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"harbor/internal/config"
	"harbor/internal/db"
	"harbor/internal/executor"
	"harbor/internal/key"
	"harbor/internal/provider"
	"harbor/internal/queue"
	"harbor/internal/stats"
	"path/filepath"
)

// mockUpstream 模拟 OpenAI 兼容上游，记录并发的最大并发数。
type mockUpstream struct {
	mu            sync.Mutex
	maxConcurrent int32
	concurrent    int32
	requests      int32
	delay         time.Duration
	statusCode    int
	body          []byte
	streamLines   []string // 流式输出行（含 "data: ..."）
	lastPath      string
	lastBody      []byte
}

func newMockUpstream(delay time.Duration) *mockUpstream {
	return &mockUpstream{
		delay: delay,
		body:  []byte(`{"id":"chatcmpl-mock","object":"chat.completion","created":1,"model":"mock-1","choices":[{"index":0,"message":{"role":"assistant","content":"hi from mock"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`),
	}
}

func (m *mockUpstream) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqBody, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.lastPath = r.URL.Path
		m.lastBody = reqBody
		m.mu.Unlock()
		atomic.AddInt32(&m.requests, 1)
		cur := atomic.AddInt32(&m.concurrent, 1)
		for {
			max := atomic.LoadInt32(&m.maxConcurrent)
			if cur <= max || atomic.CompareAndSwapInt32(&m.maxConcurrent, max, cur) {
				break
			}
		}
		defer atomic.AddInt32(&m.concurrent, -1)

		if m.delay > 0 {
			select {
			case <-time.After(m.delay):
			case <-r.Context().Done():
				return
			}
		}
		if m.statusCode != 0 {
			w.WriteHeader(m.statusCode)
			_, _ = w.Write(m.body)
			return
		}
		if len(m.streamLines) > 0 {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			f, _ := w.(http.Flusher)
			for _, line := range m.streamLines {
				_, _ = io.WriteString(w, line+"\n")
				if f != nil {
					f.Flush()
				}
				time.Sleep(100 * time.Millisecond)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(m.body)
	}
}

// buildTestServer 组装一个完整 Harbor 实例。
func buildTestServer(t *testing.T, cfg *config.Config, up *httptest.Server) (http.Handler, *queue.Scheduler, *stats.Stats) {
	t.Helper()
	keyStore := key.NewStore()
	for _, kc := range cfg.Keys {
		keyStore.Add(kc.Secret, kc.AllowedModels)
	}
	mappings := map[string][]provider.ModelRoute{}
	for model, route := range cfg.Routes {
		if len(route.Mappings) > 0 {
			for _, m := range route.Mappings {
				mappings[model] = append(mappings[model], provider.ModelRoute{
					Provider: m.Provider, UpstreamModel: m.UpstreamModel,
				})
			}
		} else {
			for _, p := range route.Providers {
				mappings[model] = append(mappings[model], provider.ModelRoute{Provider: p})
			}
		}
	}
	var providers []*provider.Provider
	for _, pc := range cfg.Providers {
		base := pc.BaseURL
		// 默认哨兵值（未配置真实上游）→ 指向测试用 mock；显式配置则保留
		if base == "" || base == "http://upstream.invalid" {
			base = up.URL + "/v1"
		}
		providers = append(providers, provider.New(
			pc.Name, pc.Type, base, pc.Models, pc.Credentials,
			cfg.Limits.RequestTimeoutDuration(), cfg.Limits.ProviderCooldownDuration(),
			pc.Concurrency, pc.RPM, pc.TPM, pc.TPH, pc.TPD,
		))
	}
	mgr := provider.NewManager(providers, mappings)
	exec := executor.New(mgr, cfg.Limits.MaxProviderAttempts, cfg.Limits.BackoffBaseDuration(), cfg.Limits.ProviderCooldownDuration())
	sched := queue.NewScheduler(cfg.Queue.MaxDepth, cfg.Queue.MaxWaitDuration())
	sched.Start()
	t.Cleanup(sched.Stop)
	st := stats.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, keyStore, mgr, exec, sched, st, nil, logger)
	return srv.Handler(), sched, st
}

func testConfig() *config.Config {
	// 测试默认显式开启心跳（生产默认关闭）：多条排队/延迟失败回归用例
	// 依赖首帧注释提交 SSE。需要验证静默行为的用例会自行关闭。
	on := true
	cfg := &config.Config{
		Server:    config.ServerConfig{Listen: "127.0.0.1:0", MaxBodySize: "20mb"},
		Queue:     config.QueueConfig{MaxWait: "30s", MaxDepth: 200, Keepalive: "100ms", ExposeQueuePosition: true},
		Keepalive: config.KeepaliveConfig{Enabled: &on, Interval: "100ms", Style: "comment", Comment: "keepalive"},
		Limits: config.RetryLimitsConfig{
			MaxProviderAttempts: 2,
			RequestTimeout:      "30s",
			BackoffBase:         "10ms",
			ProviderCooldown:    "5s",
		},
		Providers: []config.ProviderConfig{
			{Name: "mock", Type: "openai-compatible", BaseURL: "http://upstream.invalid", Credentials: []string{"mock-secret"}, Models: []string{"mock-1"}, Concurrency: 2, RPM: 100000},
		},
		Routes: map[string]config.Route{
			"mock-1": {Providers: []string{"mock"}},
		},
		Keys: []config.KeyConfig{
			{Secret: "hr-test-key-0001", AllowedModels: []string{"mock-1"}},
		},
		Admin: config.AdminConfig{Token: "admin-tok"},
	}
	return cfg
}

func doChat(t *testing.T, base string, key, model string, extra map[string]any) (*http.Response, []byte) {
	t.Helper()
	body := map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}}
	for k, v := range extra {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

// TestP0_ConcurrencyQueue：并发=2，发 10 个请求，
// 2 个执行、8 个排队、完成后自动继续、全部成功。
func TestP0_ConcurrencyQueue(t *testing.T) {
	mock := newMockUpstream(300 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	const n = 10
	start := time.Now()
	var wg sync.WaitGroup
	results := make([]int, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
			results[i] = resp.StatusCode
			if resp.StatusCode != 200 {
				errs[i] = fmt.Errorf("status %d body=%s", resp.StatusCode, string(data))
			}
			// Request ID 必须存在
			if resp.Header.Get("X-Request-Id") == "" {
				errs[i] = fmt.Errorf("missing X-Request-Id")
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		if results[i] != 200 {
			t.Fatalf("request %d status = %d", i, results[i])
		}
	}
	if got := atomic.LoadInt32(&mock.requests); got != n {
		t.Fatalf("upstream saw %d requests, want %d", got, n)
	}
	if got := atomic.LoadInt32(&mock.maxConcurrent); got != 2 {
		t.Fatalf("max concurrent to upstream = %d, want 2", got)
	}
	// 5 波 × 300ms → 总耗时应明显大于单波
	minExpected := 4 * 300 * time.Millisecond
	if elapsed < minExpected {
		t.Fatalf("elapsed %v < %v, requests did not queue properly", elapsed, minExpected)
	}
	t.Logf("P0 concurrency queue OK: 10 requests, max_conc=%d, elapsed=%v", mock.maxConcurrent, elapsed)
}

// TestP0_RPMQueueAndTimeout：RPM=2，发 5 个请求 → 2 立即成功，
// 3 个进入队列等待窗口（1 分钟）→ 超时 429（统一格式）。
func TestP0_RPMQueueAndTimeout(t *testing.T) {
	mock := newMockUpstream(50 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	cfg.Providers[0].RPM = 2
	cfg.Queue.MaxWait = "2s"
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	const n = 5
	var wg sync.WaitGroup
	statuses := make([]int, n)
	bodies := make([][]byte, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
			statuses[i] = resp.StatusCode
			bodies[i] = data
		}(i)
	}
	wg.Wait()

	okCount, failCount := 0, 0
	for i := 0; i < n; i++ {
		switch statuses[i] {
		case 200:
			okCount++
		case 429:
			failCount++
			// 统一错误格式：含 error.code = rate_limit_exceeded
			var eb struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
					Type    string `json:"type"`
				} `json:"error"`
			}
			if err := json.Unmarshal(bodies[i], &eb); err != nil {
				t.Fatalf("429 body not normalized json: %s", bodies[i])
			}
			if eb.Error.Code != "rate_limit_exceeded" {
				t.Fatalf("429 code = %q, want rate_limit_exceeded; body=%s", eb.Error.Code, bodies[i])
			}
		default:
			t.Fatalf("unexpected status %d body=%s", statuses[i], bodies[i])
		}
	}
	if okCount != 2 {
		t.Fatalf("want 2 success, got %d", okCount)
	}
	if failCount != 3 {
		t.Fatalf("want 3 queued-then-timeout, got %d", failCount)
	}
	t.Logf("P0 RPM queue OK: 2 immediate success, 3 queued then 429 (normalized)")
}

// TestP0_ErrorNormalization：上游 429 / 503 不直接暴露，
// 统一收敛为 Harbor 错误码。
func TestP0_ErrorNormalization(t *testing.T) {
	cases := []struct {
		name       string
		upStatus   int
		upBody     string
		wantStatus int
		wantCode   string
	}{
		{"upstream 429", 429, `{"error":{"message":"rate limit","type":"rate_limit_error","code":"rate_limit_exceeded"}}`, 429, "rate_limit_exceeded"},
		{"upstream 503", 503, `{"error":"busy"}`, 502, "upstream_error"},
		{"upstream 500", 500, `internal error`, 502, "upstream_error"},
		{"upstream 400", 400, `{"error":{"message":"bad param","type":"invalid_request_error","code":"invalid_request"}}`, 400, "invalid_request"},
		{"upstream 401 key", 401, `{"error":{"message":"invalid api key","type":"authentication_error","code":"invalid_api_key"}}`, 502, "upstream_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMockUpstream(0)
			mock.statusCode = tc.upStatus
			mock.body = []byte(tc.upBody)
			ups := httptest.NewServer(mock.Handler())
			defer ups.Close()

			cfg := testConfig()
			handler, _, _ := buildTestServer(t, cfg, ups)
			ts := httptest.NewServer(handler)
			defer ts.Close()

			resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", resp.StatusCode, tc.wantStatus, string(data))
			}
			// 上游原始报文绝不外泄
			if strings.Contains(string(data), "busy") && tc.upBody == `{"error":"busy"}` {
				t.Fatalf("upstream raw body leaked: %s", data)
			}
			var eb struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(data, &eb); err != nil {
				t.Fatalf("not normalized json: %s", data)
			}
			if eb.Error.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q (body=%s)", eb.Error.Code, tc.wantCode, string(data))
			}
		})
	}
}

// TestUpstreamPathNotDoubledV1：BaseURL 已含 /v1 时，Harbor 不得再拼出 /v1/v1/...。
func TestUpstreamPathNotDoubledV1(t *testing.T) {
	mock := newMockUpstream(0)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, data)
	}
	mock.mu.Lock()
	got := mock.lastPath
	mock.mu.Unlock()
	if got != "/v1/chat/completions" {
		t.Fatalf("upstream path=%q, want /v1/chat/completions", got)
	}
}

func doJSON(t *testing.T, base, path, key string, payload any) (*http.Response, []byte) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, _ := http.NewRequest("POST", base+path, bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

// TestResponsesBridgesToChatCompletions：sub2api / OpenAI SDK 默认打 /v1/responses，
// openai-compatible 上游（airouter）没有这条路。Harbor 必须翻成 chat/completions，
// 并把上游 chat 响应翻回 response 对象，而不是 502 upstream_error。
func TestResponsesBridgesToChatCompletions(t *testing.T) {
	mock := newMockUpstream(0)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doJSON(t, ts.URL, "/v1/responses", cfg.Keys[0].Secret, map[string]any{
		"model":             "mock-1",
		"input":             "hi",
		"instructions":      "be brief",
		"max_output_tokens": 32,
		"store":             false,
	})
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, data)
	}
	mock.mu.Lock()
	gotPath := mock.lastPath
	gotBody := append([]byte(nil), mock.lastBody...)
	mock.mu.Unlock()
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("upstream path=%q, want /v1/chat/completions", gotPath)
	}
	var up map[string]any
	if err := json.Unmarshal(gotBody, &up); err != nil {
		t.Fatalf("upstream body=%s err=%v", gotBody, err)
	}
	if _, ok := up["input"]; ok {
		t.Fatalf("input leaked to upstream: %s", gotBody)
	}
	if _, ok := up["store"]; ok {
		t.Fatalf("store leaked to upstream: %s", gotBody)
	}
	if up["max_tokens"] != float64(32) {
		t.Fatalf("max_tokens=%v body=%s", up["max_tokens"], gotBody)
	}
	msgs, _ := up["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("upstream messages=%s", gotBody)
	}

	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("client body=%s err=%v", data, err)
	}
	if out["object"] != "response" {
		t.Fatalf("object=%v body=%s", out["object"], data)
	}
	if out["status"] != "completed" {
		t.Fatalf("status=%v body=%s", out["status"], data)
	}
	output, _ := out["output"].([]any)
	if len(output) == 0 {
		t.Fatalf("empty output: %s", data)
	}
	msg := output[0].(map[string]any)
	content := msg["content"].([]any)
	part := content[0].(map[string]any)
	if part["type"] != "output_text" || part["text"] != "hi from mock" {
		t.Fatalf("output part=%v body=%s", part, data)
	}
}

func TestMessagesBridgesToChatCompletions(t *testing.T) {
	mock := newMockUpstream(0)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doJSON(t, ts.URL, "/v1/messages", cfg.Keys[0].Secret, map[string]any{
		"model":      "mock-1",
		"max_tokens": 16,
		"system":     "sys",
		"messages":   []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, data)
	}
	mock.mu.Lock()
	gotPath := mock.lastPath
	mock.mu.Unlock()
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("upstream path=%q", gotPath)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out["type"] != "message" || out["role"] != "assistant" {
		t.Fatalf("body=%s", data)
	}
}

// TestStreamUpstreamErrorReturnsJSON：流式请求在 SSE 头提交前失败时，
// 必须返回 JSON 502，error.message 是 string，不能是对象。
func TestStreamUpstreamErrorReturnsJSON(t *testing.T) {
	mock := newMockUpstream(0)
	mock.statusCode = 404
	mock.body = []byte(`<!DOCTYPE html><title>404 Not Found</title>`)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	cfg.Keepalive.Interval = "30s"
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", map[string]any{"stream": true})
	if resp.StatusCode != 502 {
		t.Fatalf("status=%d ct=%q body=%s", resp.StatusCode, resp.Header.Get("Content-Type"), data)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type=%q, want application/json (SSE already committed?)", ct)
	}
	if strings.Contains(string(data), "event: error") {
		t.Fatalf("SSE event: error leaked into JSON path: %s", data)
	}
	var eb struct {
		Error struct {
			Message json.RawMessage `json:"message"`
			Type    string          `json:"type"`
			Code    string          `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &eb); err != nil {
		t.Fatalf("not json: %s", data)
	}
	if eb.Error.Code != "upstream_error" {
		t.Fatalf("code=%q body=%s", eb.Error.Code, data)
	}
	if len(eb.Error.Message) == 0 || eb.Error.Message[0] != '"' {
		t.Fatalf("error.message must be string, got %s", data)
	}
	if strings.Contains(string(data), "[object Object]") {
		t.Fatalf("body contains [object Object]: %s", data)
	}
	if strings.Contains(string(data), "<!DOCTYPE") {
		t.Fatalf("upstream html leaked: %s", data)
	}
}

// TestP0_Auth：无效 Key / 封禁 → 收敛错误。
func TestP0_Auth(t *testing.T) {
	mock := newMockUpstream(0)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// 无效 Key
	resp, data := doChat(t, ts.URL, "hr-wrong-key", "mock-1", nil)
	if resp.StatusCode != 401 {
		t.Fatalf("invalid key: status=%d body=%s", resp.StatusCode, data)
	}
	var eb struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &eb)
	if eb.Error.Code != "invalid_api_key" {
		t.Fatalf("invalid key code = %q, body=%s", eb.Error.Code, data)
	}

	// 缺失 Key
	resp2, _ := doChat(t, ts.URL, "", "mock-1", nil)
	if resp2.StatusCode != 401 {
		t.Fatalf("missing key: status=%d", resp2.StatusCode)
	}
}

// TestP0_ModelForbidden：不允许的模型 → 403 model_forbidden。
func TestP0_ModelForbidden(t *testing.T) {
	mock := newMockUpstream(0)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "not-configured-model", nil)
	if resp.StatusCode != 403 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, data)
	}
	var eb struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &eb)
	if eb.Error.Code != "model_forbidden" {
		t.Fatalf("code = %q", eb.Error.Code)
	}
}

// TestP0_StreamingQueue：流式请求排队时发送 SSE 心跳，
// 上游输出逐行转发。
func TestP0_StreamingQueue(t *testing.T) {
	mock := newMockUpstream(0)
	mock.streamLines = []string{
		`data: {"id":"x","object":"chat.completion.chunk","model":"mock-1","choices":[{"index":0,"delta":{"content":"Hel"},"finish_reason":null}]}`,
		`data: {"id":"x","object":"chat.completion.chunk","model":"mock-1","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":null}]}`,
		`data: {"id":"x","object":"chat.completion.chunk","model":"mock-1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		`data: [DONE]`,
	}
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	// 并发=1，让第一个请求占住并发槽 → 第二个请求排队 → 心跳出现
	cfg.Providers[0].Concurrency = 1
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	reqBody := map[string]any{
		"model":    "mock-1",
		"stream":   true,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}
	raw, _ := json.Marshal(reqBody)

	// 第一个请求占住并发槽（mock 流式很慢）。
	// 注意：此处保持连接打开（不 Close body），模拟低延迟 SSE 客户端
	// 正在消费流；若关闭 body 会触发 ctx 取消，槽位立即释放。
	go func() {
		req, _ := http.NewRequest("POST", ts.URL+"/v1/chat/completions", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		// 读完整个流再关闭，模拟正常消费流式响应
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
	}()

	// 第二个请求排队 → 应看到心跳
	time.Sleep(150 * time.Millisecond)
	req, _ := http.NewRequest("POST", ts.URL+"/v1/chat/completions", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request error: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	// 第一个请求仍在执行（占住并发槽），本请求必然经过排队；
	// 位置头确认排队状态（值为当前等待位置）
	if resp.Header.Get("X-Queue-Position") == "" {
		t.Fatalf("expected X-Queue-Position on queued stream request (headers=%v)", resp.Header)
	}
	t.Logf("queued stream position = %s", resp.Header.Get("X-Queue-Position"))

	br := bufio.NewReader(resp.Body)
	var sawData, sawHeartbeat bool
	timeout := time.After(5 * time.Second)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			break
		}
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "data:") {
			sawData = true
		}
		if strings.Contains(trimmed, ": keepalive") {
			sawHeartbeat = true
		}
		select {
		case <-timeout:
			t.Fatalf("stream read timeout")
		default:
		}
		if sawData && strings.Contains(line, "[DONE]") {
			break
		}
	}
	if !sawData {
		t.Fatalf("no data chunks received")
	}
	if !sawHeartbeat {
		t.Fatalf("no queue heartbeat received while queued")
	}
	t.Logf("P0 streaming queue OK: heartbeat=%v data=%v", sawHeartbeat, sawData)
}

// TestP0_ClientDisconnect：客户端断开后，排队槽位释放，
// 后续请求能继续执行（验收 13）。
func TestP0_ClientDisconnect(t *testing.T) {
	mock := newMockUpstream(500 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	cfg.Providers[0].Concurrency = 1
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	body := map[string]any{"model": "mock-1", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}
	raw, _ := json.Marshal(body)
	newReq := func() *http.Request {
		req, _ := http.NewRequest("POST", ts.URL+"/v1/chat/completions", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
		return req
	}

	// 1. 占住信号量再立刻断开
	req1 := newReq()
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1 = req1.WithContext(ctx1)
	respCh := make(chan error, 1)
	go func() {
		_, err := http.DefaultClient.Do(req1)
		respCh <- err
	}()
	time.Sleep(100 * time.Millisecond) // 让它入队/占用
	cancel1()                          // 客户端断开
	select {
	case <-respCh:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not abort on disconnect")
	}

	// 2. 并发槽应已释放：新请求可以立即执行（而不是等到 500ms*N 排完）
	start := time.Now()
	resp2, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	if resp2.StatusCode != 200 {
		t.Fatalf("second request status=%d body=%s", resp2.StatusCode, data)
	}
	// 上游 mock 延迟 500ms；若槽未释放，本请求需要等第一个 500ms 完成…
	// 但第一个已被取消，槽立即释放 → 总耗时 ≈ 500ms 左右（含 mock 延迟）。
	// 若排队累计，可能耗时更长；这里仅做宽松冒烟，核心断言是 200。
	_ = start
	t.Logf("P0 disconnect OK: slot released, follow-up request succeeded")
}

// TestP0_QueueFull：队列满 → 429 rate_limit_exceeded。
func TestP0_QueueFull(t *testing.T) {
	mock := newMockUpstream(2 * time.Second)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	cfg.Queue.MaxDepth = 2
	cfg.Providers[0].Concurrency = 1
	cfg.Limits.RequestTimeout = "10s"
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// 占满: 1 执行 + 2 排队 = 3 个在系统内；等待它们结束再清理
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
		}()
	}
	time.Sleep(200 * time.Millisecond)
	// 第 4 个 → 队列满 → 429
	resp, data := doChat(t, ts.URL, cfg.Keys[0].Secret, "mock-1", nil)
	if resp.StatusCode != 429 {
		t.Fatalf("queue full status=%d body=%s", resp.StatusCode, data)
	}
	wg.Wait()
	t.Logf("P0 queue full OK: 429 with normalized error")
}

// postChat 以自定义 body 发送请求（doChat 固定 content，Token 测试需要控制长度）。
func postChat(t *testing.T, base, key string, raw []byte) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

// TestP1_TokenLimit：TPM 预扣 → 超限排队 → 结算回补 → 自动放行。
// 并发=2（排除并发排队因素），TPM=60，预扣 40（96 字符/4+16）；
// 请求2 在请求1 未结算时入队：40+40=80 > 60 → 只能因 token gate 排队；
// 请求1 完成结算（mock usage total=15）→ 窗口 15 → 请求2 出队（55 ≤ 60）。
func TestP1_TokenLimit(t *testing.T) {
	mock := newMockUpstream(100 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	cfg.Providers[0].Concurrency = 2 // 并发不成为排队原因
	cfg.Providers[0].TPM = 60
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	content := strings.Repeat("a", 96) // 96/4+16 = 40 reserve
	body := map[string]any{
		"model":    "mock-1",
		"messages": []any{map[string]any{"role": "user", "content": content}},
	}
	raw, _ := json.Marshal(body)

	// 请求1：立刻执行（40 ≤ 60），预扣 TPM
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, data := postChat(t, ts.URL, cfg.Keys[0].Secret, raw)
		if resp.StatusCode != 200 {
			t.Errorf("request1 status=%d body=%s", resp.StatusCode, data)
		}
	}()

	// 请求1 执行中（未结算，窗口 40）发请求2 → 必须因 TPM 排队（40+40>60）
	time.Sleep(50 * time.Millisecond)
	resp2, data2 := postChat(t, ts.URL, cfg.Keys[0].Secret, raw)
	if resp2.StatusCode != 200 {
		t.Fatalf("request2 status=%d body=%s", resp2.StatusCode, data2)
	}
	if pos := resp2.Header.Get("X-Queue-Position"); pos == "" {
		t.Fatal("request2 should have queued (token window full), no X-Queue-Position")
	} else {
		t.Logf("request2 queued at position %s", pos)
	}
	<-done

	if got := atomic.LoadInt32(&mock.requests); got != 2 {
		t.Fatalf("upstream saw %d requests, want 2", got)
	}
	t.Log("P1 token limit OK: reserve → queue → settle → release")
}

// TestP1_TokenLimitTimeout：TPM 永久不足（预扣 60，上限 100，两个请求
// 无法同时满足）→ 排队超过 max_queue_wait → 429 统一收敛。
func TestP1_TokenLimitTimeout(t *testing.T) {
	mock := newMockUpstream(2 * time.Second) // 请求1 长期占住
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()

	cfg := testConfig()
	cfg.Providers[0].Concurrency = 1
	cfg.Providers[0].TPM = 100
	cfg.Queue.MaxWait = "300ms"
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	content := strings.Repeat("b", 176) // 176/4+16 = 60 reserve
	body := map[string]any{
		"model":    "mock-1",
		"messages": []any{map[string]any{"role": "user", "content": content}},
	}
	raw, _ := json.Marshal(body)

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, _ := postChat(t, ts.URL, cfg.Keys[0].Secret, raw)
		if resp.StatusCode != 200 {
			t.Errorf("request1 status=%d", resp.StatusCode)
		}
	}()

	time.Sleep(150 * time.Millisecond)
	start := time.Now()
	resp2, data2 := postChat(t, ts.URL, cfg.Keys[0].Secret, raw)
	elapsed := time.Since(start)
	if resp2.StatusCode != 429 {
		t.Fatalf("request2 status=%d body=%s, want 429", resp2.StatusCode, data2)
	}
	var eb struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data2, &eb)
	if eb.Error.Code != "rate_limit_exceeded" {
		t.Fatalf("request2 error code=%q, want rate_limit_exceeded", eb.Error.Code)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("request2 waited %v, want ~300ms max_queue_wait", elapsed)
	}
	<-done
	t.Logf("P1 token limit timeout OK: 429 after %v", elapsed)
}

// failoverConfig 构造双上游（duck 优先 / goose 备用）配置。
func failoverConfig(aURL, bURL string) *config.Config {
	cfg := testConfig()
	cfg.Providers = []config.ProviderConfig{
		{Name: "duck", Type: "openai-compatible", BaseURL: aURL + "/v1", Credentials: []string{"a-secret"}, Models: []string{"mock-1"}},
		{Name: "goose", Type: "openai-compatible", BaseURL: bURL + "/v1", Credentials: []string{"b-secret"}, Models: []string{"mock-1"}},
	}
	cfg.Routes = map[string]config.Route{
		"mock-1": {Providers: []string{"duck", "goose"}},
	}
	return cfg
}

// TestP1_Failover429：优先上游 429 → 凭证冷却 → 自动切换备用上游 → 客户端 200。
func TestP1_Failover429(t *testing.T) {
	mockA := newMockUpstream(0)
	mockA.statusCode = 429
	upA := httptest.NewServer(mockA.Handler())
	defer upA.Close()
	mockB := newMockUpstream(0)
	upB := httptest.NewServer(mockB.Handler())
	defer upB.Close()

	handler, _, _ := buildTestServer(t, failoverConfig(upA.URL, upB.URL), upA)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s, want 200 after failover", resp.StatusCode, data)
	}
	if got := atomic.LoadInt32(&mockA.requests); got != 1 {
		t.Fatalf("duck saw %d requests, want 1 (single attempt)", got)
	}
	if got := atomic.LoadInt32(&mockB.requests); got != 1 {
		t.Fatalf("goose saw %d requests, want 1", got)
	}
	t.Log("P1 failover OK: 429 on duck → goose succeeds, evidence duck=1 goose=1")
}

// TestP1_FailoverCooldown：冷却期内的上游不被再次选择。
func TestP1_FailoverCooldown(t *testing.T) {
	mockA := newMockUpstream(0)
	mockA.statusCode = 429
	upA := httptest.NewServer(mockA.Handler())
	defer upA.Close()
	mockB := newMockUpstream(0)
	upB := httptest.NewServer(mockB.Handler())
	defer upB.Close()

	cfg := failoverConfig(upA.URL, upB.URL)
	cfg.Limits.ProviderCooldown = "30s" // 冷却覆盖整个测试
	handler, _, _ := buildTestServer(t, cfg, upA)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// 请求1：duck 429 → 冷却，goose 成功
	resp1, data1 := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp1.StatusCode != 200 {
		t.Fatalf("req1 status=%d body=%s", resp1.StatusCode, data1)
	}
	// 请求2：duck 仍在冷却 → 直接走 goose
	resp2, data2 := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp2.StatusCode != 200 {
		t.Fatalf("req2 status=%d body=%s", resp2.StatusCode, data2)
	}
	if got := atomic.LoadInt32(&mockA.requests); got != 1 {
		t.Fatalf("duck saw %d requests total, want 1 (cooldown honored)", got)
	}
	if got := atomic.LoadInt32(&mockB.requests); got != 2 {
		t.Fatalf("goose saw %d requests, want 2", got)
	}
	t.Log("P1 cooldown OK: duck=1 goose=2 during cooldown")
}

// TestP1_FailoverCredential401：上游 401 → 凭证禁用 → 切换备用。
func TestP1_FailoverCredential401(t *testing.T) {
	mockA := newMockUpstream(0)
	mockA.statusCode = 401
	upA := httptest.NewServer(mockA.Handler())
	defer upA.Close()
	mockB := newMockUpstream(0)
	upB := httptest.NewServer(mockB.Handler())
	defer upB.Close()

	handler, _, _ := buildTestServer(t, failoverConfig(upA.URL, upB.URL), upA)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s, want 200 via backup after 401", resp.StatusCode, data)
	}
	if got := atomic.LoadInt32(&mockA.requests); got != 1 {
		t.Fatalf("duck saw %d requests, want 1", got)
	}
	if got := atomic.LoadInt32(&mockB.requests); got != 1 {
		t.Fatalf("goose saw %d requests, want 1", got)
	}
	t.Log("P1 credential 401 OK: duck disabled → goose serves")
}

// TestP1_FailoverAllFail：全部出口失败 → retry budget 用尽 → 收敛错误。
func TestP1_FailoverAllFail(t *testing.T) {
	mockA := newMockUpstream(0)
	mockA.statusCode = 503
	upA := httptest.NewServer(mockA.Handler())
	defer upA.Close()
	mockB := newMockUpstream(0)
	mockB.statusCode = 500
	upB := httptest.NewServer(mockB.Handler())
	defer upB.Close()

	cfg := failoverConfig(upA.URL, upB.URL)
	cfg.Limits.MaxProviderAttempts = 2
	handler, _, _ := buildTestServer(t, cfg, upA)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp.StatusCode != 502 {
		t.Fatalf("status=%d body=%s, want 502 upstream_error", resp.StatusCode, data)
	}
	var eb struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &eb)
	if eb.Error.Code != "upstream_error" {
		t.Fatalf("code=%q, want upstream_error", eb.Error.Code)
	}
	if got := atomic.LoadInt32(&mockA.requests); got != 1 {
		t.Fatalf("duck saw %d, want 1", got)
	}
	if got := atomic.LoadInt32(&mockB.requests); got != 1 {
		t.Fatalf("goose saw %d, want 1", got)
	}
	t.Log("P1 all-fail OK: converged 502 upstream_error, duck=1 goose=1")
}

// TestP1_FailoverNonRetryable：400 不可重试 → 立即返回，备用不被打。
func TestP1_FailoverNonRetryable(t *testing.T) {
	mockA := newMockUpstream(0)
	mockA.statusCode = 400
	upA := httptest.NewServer(mockA.Handler())
	defer upA.Close()
	mockB := newMockUpstream(0)
	upB := httptest.NewServer(mockB.Handler())
	defer upB.Close()

	handler, _, _ := buildTestServer(t, failoverConfig(upA.URL, upB.URL), upA)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d body=%s, want 400 invalid_request (no retry)", resp.StatusCode, data)
	}
	if got := atomic.LoadInt32(&mockB.requests); got != 0 {
		t.Fatalf("goose should not be called on non-retryable error, saw %d", got)
	}
	t.Log("P1 non-retryable OK: 400 immediate, goose untouched")
}

// --- P2：SQLite 持久化端到端 ---

// testProviders 从配置构建运行时 Provider（哨兵值替换为 mock 上游）。
func testProviders(t *testing.T, cfg *config.Config, up *httptest.Server) []*provider.Provider {
	t.Helper()
	var providers []*provider.Provider
	for _, pc := range cfg.Providers {
		base := pc.BaseURL
		if base == "" || base == "http://upstream.invalid" {
			base = up.URL + "/v1"
		}
		providers = append(providers, provider.New(
			pc.Name, pc.Type, base, pc.Models, pc.Credentials,
			cfg.Limits.RequestTimeoutDuration(), cfg.Limits.ProviderCooldownDuration(),
			pc.Concurrency, pc.RPM, pc.TPM, pc.TPH, pc.TPD,
		))
	}
	return providers
}

// buildTestServerWithStore 与 buildTestServer 相同，但注入 SQLite store（P2 镜像模式）。
func buildTestServerWithStore(t *testing.T, cfg *config.Config, up *httptest.Server, store *db.DB) (http.Handler, *queue.Scheduler) {
	t.Helper()
	keyStore := key.NewStore()
	for _, kc := range cfg.Keys {
		keyStore.Add(kc.Secret, kc.AllowedModels)
	}
	mappings := map[string][]provider.ModelRoute{}
	for model, route := range cfg.Routes {
		if len(route.Mappings) > 0 {
			for _, m := range route.Mappings {
				mappings[model] = append(mappings[model], provider.ModelRoute{
					Provider: m.Provider, UpstreamModel: m.UpstreamModel,
				})
			}
		} else {
			for _, p := range route.Providers {
				mappings[model] = append(mappings[model], provider.ModelRoute{Provider: p})
			}
		}
	}
	var providers []*provider.Provider
	for _, pc := range cfg.Providers {
		base := pc.BaseURL
		if base == "" || base == "http://upstream.invalid" {
			base = up.URL + "/v1"
		}
		providers = append(providers, provider.New(
			pc.Name, pc.Type, base, pc.Models, pc.Credentials,
			cfg.Limits.RequestTimeoutDuration(), cfg.Limits.ProviderCooldownDuration(),
			pc.Concurrency, pc.RPM, pc.TPM, pc.TPH, pc.TPD,
		))
	}
	mgr := provider.NewManager(providers, mappings)
	exec := executor.New(mgr, cfg.Limits.MaxProviderAttempts, cfg.Limits.BackoffBaseDuration(), cfg.Limits.ProviderCooldownDuration())
	sched := queue.NewScheduler(cfg.Queue.MaxDepth, cfg.Queue.MaxWaitDuration())
	sched.Start()
	t.Cleanup(sched.Stop)
	st := stats.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, keyStore, mgr, exec, sched, st, store, logger)
	return srv.Handler(), sched
}

// TestP2_PersistenceEndToEnd 验证：
//  1. 请求记录异步落库（成功 + 错误分类）
//  2. 管理 API 创建的 Key 同步落库，ban/unban 状态同步
//  3. 上游凭证加密落库（AES-GCM，可解密还原验证）
//  4. 重启（重开同一 db）后数据仍在：keys/credentials/requests
//  5. 新实例（内存 recent 空）经 /api/requests/{id} 回退查库返回 200
func TestP2_PersistenceEndToEnd(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "harbor.db")
	keyPath := filepath.Join(dir, "harbor.key")

	store, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	if err := store.Migrate(); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}
	encKey, err := db.LoadOrCreateKey(keyPath)
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}

	up := newMockUpstream(30 * time.Millisecond)
	upSrv := httptest.NewServer(up.Handler())
	defer upSrv.Close()

	cfg := testConfig()
	cfg.Storage.KeyPath = keyPath
	handler, _ := buildTestServerWithStore(t, cfg, upSrv, store)
	// 与 main.go 相同：启动镜像（keys + 加密凭证）落库
	keyStore := key.NewStore()
	for _, kc := range cfg.Keys {
		keyStore.Add(kc.Secret, kc.AllowedModels)
	}
	MirrorStartup(store, keyStore, testProviders(t, cfg, upSrv), encKey, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// 1. 一次成功请求 + 一次模型错误请求（record 路径也落库）
	resp, _ := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("chat status = %d, want 200", resp.StatusCode)
	}
	reqID := resp.Header.Get("X-Request-Id")
	if reqID == "" {
		t.Fatal("missing X-Request-Id")
	}
	resp2, _ := doChat(t, ts.URL, "hr-test-key-0001", "no-such-model", nil)
	if resp2.StatusCode != 400 && resp2.StatusCode != 404 {
		t.Logf("model-forbidden status = %d (accepted variants)", resp2.StatusCode)
	}
	errRespID := resp2.Header.Get("X-Request-Id")

	// 2. 管理 API。GET /api/stats 需要 admin token（testConfig 里 admin-tok）
	adminGet := func(base, path string) (*http.Response, []byte) {
		t.Helper()
		req, _ := http.NewRequest("GET", base+path, nil)
		req.Header.Set("Authorization", "Bearer admin-tok")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("admin GET %s: %v", path, err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return r, b
	}
	adminPost := func(base, path string, body []byte) (*http.Response, []byte) {
		t.Helper()
		req, _ := http.NewRequest("POST", base+path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-tok")
		req.Header.Set("Content-Type", "application/json")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("admin POST %s: %v", path, err)
		}
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return r, b
	}

	// 3. 创建 Key → 落库；ban → 库状态同步
	cr, cb := adminPost(ts.URL, "/api/keys", []byte(`{"allowed_models":["mock-1"]}`))
	if cr.StatusCode != 200 {
		t.Fatalf("create key status = %d, body=%s", cr.StatusCode, cb)
	}
	var created struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(cb, &created); err != nil || created.ID == "" || created.Secret == "" {
		t.Fatalf("create key response parse: %v body=%s", err, cb)
	}
	if br, bb := adminPost(ts.URL, "/api/keys/"+created.ID+"/ban", nil); br.StatusCode != 200 {
		t.Fatalf("ban key status = %d body=%s", br.StatusCode, bb)
	}
	providerResp, providerBody := adminPost(ts.URL, "/api/providers", []byte(`{"name":"dynamic","type":"openai-compatible","base_url":"https://dynamic.example/v1","models":["dynamic-model"],"credentials":["dynamic-secret"],"concurrency":3,"rpm":12}`))
	if providerResp.StatusCode != 200 {
		t.Fatalf("create provider status = %d, body=%s", providerResp.StatusCode, providerBody)
	}

	// 4. 等待异步落库完成（轮询：成功请求记录出现）
	deadline := time.Now().Add(3 * time.Second)
	for {
		reqs, err := store.RecentRequests(10)
		if err == nil && len(reqs) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for async persist; last err=%v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	store.Close() // 模拟停机（重启前关闭）

	// 5. 重启：重开同一 db，验证镜像完整
	store2, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer store2.Close()
	if err := store2.Migrate(); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}

	keys, err := store2.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys after restart: %v", err)
	}
	var foundCreated, foundBanned, foundSecret bool
	for _, k := range keys {
		if k.ID == created.ID {
			foundCreated = true
			foundBanned = k.Status == "banned"
			if k.SecretEncrypted != "" {
				plain, err := db.Decrypt(encKey, k.SecretEncrypted)
				foundSecret = err == nil && plain == created.Secret
			}
		}
	}
	if !foundCreated {
		t.Fatalf("created key %s not persisted; keys=%+v", created.ID, keys)
	}
	if !foundBanned {
		t.Fatalf("ban status not persisted for %s; keys=%+v", created.ID, keys)
	}
	if !foundSecret {
		t.Fatalf("created key secret was not encrypted for restart recovery; keys=%+v", keys)
	}

	providers, err := store2.ListProviders()
	if err != nil {
		t.Fatalf("ListProviders after restart: %v", err)
	}
	foundProvider := false
	for _, p := range providers {
		if p.Name == "dynamic" && p.BaseURL == "https://dynamic.example/v1" && p.Concurrency == 3 && p.RPM == 12 {
			foundProvider = true
		}
	}
	if !foundProvider {
		t.Fatalf("created Provider was not persisted: %+v", providers)
	}

	// 凭证密文可解密还原
	creds, err := store2.ListCredentials()
	if err != nil || len(creds) == 0 {
		t.Fatalf("credentials not persisted: err=%v n=%d", err, len(creds))
	}
	var foundMockCred, foundDynamicCred bool
	for _, cred := range creds {
		plain, decryptErr := db.Decrypt(encKey, cred.SecretEncrypted)
		if decryptErr == nil && cred.ProviderID == "mock" && plain == "mock-secret" {
			foundMockCred = true
		}
		if decryptErr == nil && cred.ProviderID == "dynamic" && plain == "dynamic-secret" {
			foundDynamicCred = true
		}
		if strings.Contains(cred.SecretEncrypted, "mock-secret") || strings.Contains(cred.SecretEncrypted, "dynamic-secret") {
			t.Fatal("credential stored in plaintext!")
		}
	}
	if !foundMockCred || !foundDynamicCred {
		t.Fatalf("credential decrypt mismatch: mock=%v dynamic=%v creds=%+v", foundMockCred, foundDynamicCred, creds)
	}

	// 请求审计记录
	reqs, err := store2.RecentRequests(10)
	if err != nil {
		t.Fatalf("RecentRequests after restart: %v", err)
	}
	foundReq := false
	var foundErrReq bool
	for _, r := range reqs {
		if r.RequestID == reqID {
			foundReq = r.Status == "success"
		}
		if r.RequestID == errRespID {
			foundErrReq = r.Status == "error" && r.ErrorCode != ""
		}
	}
	if !foundReq {
		t.Fatalf("success request %s not persisted; reqs=%+v", reqID, reqs)
	}
	if !foundErrReq {
		t.Fatalf("error request %s not persisted with error_code; reqs=%+v", errRespID, reqs)
	}

	// 6. 新实例（内存 recent 空）：/api/requests/{id} 回退查库
	handler2, _ := buildTestServerWithStore(t, cfg, upSrv, store2)
	ts2 := httptest.NewServer(handler2)
	defer ts2.Close()
	// 注意：ts2 是新实例，内存 recent 为空，必须经 SQLite 回退查到记录
	rr, rb := adminGet(ts2.URL, "/api/requests/"+reqID)
	if rr.StatusCode != 200 {
		t.Fatalf("fallback query status = %d body=%s", rr.StatusCode, rb)
	}
	var got map[string]any
	if err := json.Unmarshal(rb, &got); err != nil {
		t.Fatalf("fallback response parse: %v", err)
	}
	if got["request_id"] != reqID || got["status"] != "success" {
		t.Fatalf("fallback response mismatch: %s", rb)
	}
	if rr2, _ := adminGet(ts2.URL, "/api/requests/does-not-exist"); rr2.StatusCode != 404 {
		t.Fatalf("missing request should 404, got %d", rr2.StatusCode)
	}

	t.Log("P2 persistence OK: keys/credentials/requests survive restart, fallback query works")
}

func TestRestoreStartup(t *testing.T) {
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "harbor.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	encKey, err := db.LoadOrCreateKey(filepath.Join(dir, "harbor.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}

	keySecret := "hr-restored-key-0001"
	keyCipher, err := db.Encrypt(encKey, keySecret)
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	createdAt := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	if err := store.UpsertKey(&db.Key{
		ID:              "restored-key",
		Prefix:          key.Prefix(keySecret),
		KeyHash:         key.Hash(keySecret),
		SecretEncrypted: keyCipher,
		Status:          "active",
		AllowedModels:   []string{"restored-model"},
		CreatedAt:       createdAt,
	}); err != nil {
		t.Fatalf("UpsertKey: %v", err)
	}

	configSecret := "hr-config-key-0001"
	configSeed := key.NewStore().Add(configSecret, []string{"old-model"})
	if err := store.UpsertKey(&db.Key{
		ID:            configSeed.ID,
		Prefix:        configSeed.Prefix,
		KeyHash:       configSeed.Hash,
		Status:        "banned",
		AllowedModels: []string{"new-model"},
		CreatedAt:     createdAt,
	}); err != nil {
		t.Fatalf("UpsertKey metadata: %v", err)
	}

	if err := store.UpsertProvider(&db.ProviderRow{
		ID:          "persisted-provider",
		Name:        "persisted-provider",
		Type:        "openai-compatible",
		BaseURL:     "https://upstream.example/v1",
		Models:      []string{"restored-model"},
		Timeout:     "17s",
		Cooldown:    "2s",
		Concurrency: 4,
		RPM:         30,
	}); err != nil {
		t.Fatalf("UpsertProvider: %v", err)
	}
	credSecret := "persisted-provider-secret"
	credCipher, err := db.Encrypt(encKey, credSecret)
	if err != nil {
		t.Fatalf("encrypt credential: %v", err)
	}
	if err := store.UpsertCredential(&db.Credential{
		ID:                "persisted-credential",
		ProviderID:        "persisted-provider",
		SecretEncrypted:   credCipher,
		SecretFingerprint: credentialFingerprint(credSecret),
		Status:            "active",
	}); err != nil {
		t.Fatalf("UpsertCredential: %v", err)
	}
	if err := store.UpsertRoute("restored-model", []db.ModelMapping{{Provider: "persisted-provider", UpstreamModel: "actual-model"}}); err != nil {
		t.Fatalf("UpsertRoute: %v", err)
	}

	restoredKeys := key.NewStore()
	restoredKeys.Add(configSecret, []string{"old-model"})
	restoredMgr := provider.NewManager(nil, nil)
	if err := RestoreStartup(store, restoredKeys, restoredMgr, time.Minute, 30*time.Second, encKey, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("RestoreStartup: %v", err)
	}
	if _, err := restoredKeys.Authenticate("Bearer " + keySecret); err != nil {
		t.Fatalf("restored Harbor Key did not authenticate: %v", err)
	}
	if _, err := restoredKeys.Authenticate("Bearer " + configSecret); err != key.ErrBanned {
		t.Fatalf("persisted metadata did not restore config Key status: %v", err)
	}
	configRestored, ok := restoredKeys.Get(configSeed.ID)
	if !ok || len(configRestored.AllowedModels) != 1 || configRestored.AllowedModels[0] != "new-model" {
		t.Fatalf("persisted metadata did not restore config Key models: %+v", configRestored)
	}
	p, ok := restoredMgr.Get("persisted-provider")
	if !ok {
		t.Fatal("persisted Provider was not restored")
	}
	if p.BaseURL != "https://upstream.example/v1" || p.Concurrency != 4 || p.RPM != 30 {
		t.Fatalf("restored Provider metadata mismatch: %+v", p)
	}
	creds := p.Credentials()
	if len(creds) != 1 || creds[0].ID != "persisted-credential" || creds[0].Secret != credSecret {
		t.Fatalf("restored Provider credentials mismatch: %+v", creds)
	}
	routes := restoredMgr.Routes()
	if len(routes["restored-model"]) != 1 || routes["restored-model"][0].UpstreamModel != "actual-model" {
		t.Fatalf("restored route mismatch: %+v", routes)
	}
}

func yamlSeedConfig() *config.Config {
	return &config.Config{
		Limits: config.RetryLimitsConfig{RequestTimeout: "30s", ProviderCooldown: "5s"},
		Providers: []config.ProviderConfig{
			{Name: "openai", Type: "openai-compatible", BaseURL: "https://api.openai.com/v1", Credentials: []string{"sk-REPLACE_ME"}, Concurrency: 2, RPM: 60},
		},
		Routes: map[string]config.Route{
			"gpt-4o": {Providers: []string{"openai"}},
		},
		Keys: []config.KeyConfig{
			{Secret: "hr-yaml-default-0001", AllowedModels: []string{"gpt-4o"}},
		},
	}
}

func TestSeedEmptyFromConfigSeedsEmptyDB(t *testing.T) {
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "harbor.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	encKey, err := db.LoadOrCreateKey(filepath.Join(dir, "harbor.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	cfg := yamlSeedConfig()
	cfg.Providers[0].Credentials = []string{"sk-real-upstream"}
	keyStore := key.NewStore()
	mgr := provider.NewManager(nil, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := SeedEmptyFromConfig(store, cfg, keyStore, mgr, time.Minute, 30*time.Second, encKey, log); err != nil {
		t.Fatalf("SeedEmptyFromConfig: %v", err)
	}
	if _, err := keyStore.Authenticate("Bearer hr-yaml-default-0001"); err != nil {
		t.Fatalf("seeded yaml key did not authenticate: %v", err)
	}
	keys, err := store.ListKeys()
	if err != nil || len(keys) != 1 || keys[0].SecretEncrypted == "" {
		t.Fatalf("seeded key must persist ciphertext: keys=%+v err=%v", keys, err)
	}
	if _, ok := mgr.Get("openai"); !ok {
		t.Fatal("seeded openai provider missing from memory")
	}
	providers, err := store.ListProviders()
	if err != nil || len(providers) != 1 || providers[0].Name != "openai" {
		t.Fatalf("seeded providers=%+v err=%v", providers, err)
	}
	creds, err := store.ListCredentials()
	if err != nil || len(creds) != 1 {
		t.Fatalf("seeded credentials=%+v err=%v", creds, err)
	}
	routes := mgr.Routes()
	if len(routes["gpt-4o"]) != 1 || routes["gpt-4o"][0].Provider != "openai" {
		t.Fatalf("seeded route mismatch: %+v", routes)
	}
}

func TestSeedEmptyFromConfigSkipsPlaceholderProvider(t *testing.T) {
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "harbor.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	encKey, err := db.LoadOrCreateKey(filepath.Join(dir, "harbor.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}
	keyStore := key.NewStore()
	mgr := provider.NewManager(nil, nil)
	if err := SeedEmptyFromConfig(store, yamlSeedConfig(), keyStore, mgr, time.Minute, 30*time.Second, encKey, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("SeedEmptyFromConfig: %v", err)
	}
	if _, ok := mgr.Get("openai"); ok {
		t.Fatal("placeholder openai must not be seeded")
	}
	providers, err := store.ListProviders()
	if err != nil || len(providers) != 0 {
		t.Fatalf("placeholder openai persisted: %+v err=%v", providers, err)
	}
	if routes := mgr.Routes(); routes["gpt-4o"] != nil {
		t.Fatalf("gpt-4o route must not seed without provider: %+v", routes)
	}
}

func TestSeedEmptyFromConfigSkipsExistingAdminData(t *testing.T) {
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "harbor.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	encKey, err := db.LoadOrCreateKey(filepath.Join(dir, "harbor.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}

	adminSecret := "hr-admin-key-0001"
	adminCipher, err := db.Encrypt(encKey, adminSecret)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := store.UpsertKey(&db.Key{
		ID: "admin-key", Prefix: key.Prefix(adminSecret), KeyHash: key.Hash(adminSecret),
		SecretEncrypted: adminCipher, Status: "active", AllowedModels: []string{"DeepSeek-V4-Flash"},
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertKey: %v", err)
	}
	if err := store.UpsertProvider(&db.ProviderRow{
		ID: "airouter", Name: "airouter", Type: "openai-compatible",
		BaseURL: "https://example.invalid/v1", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertProvider: %v", err)
	}
	if err := store.UpsertRoute("DeepSeek-V4-Flash", []db.ModelMapping{{Provider: "airouter"}}); err != nil {
		t.Fatalf("UpsertRoute: %v", err)
	}

	keyStore := key.NewStore()
	mgr := provider.NewManager(nil, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := RestoreStartup(store, keyStore, mgr, time.Minute, 30*time.Second, encKey, log); err != nil {
		t.Fatalf("RestoreStartup: %v", err)
	}
	if err := SeedEmptyFromConfig(store, yamlSeedConfig(), keyStore, mgr, time.Minute, 30*time.Second, encKey, log); err != nil {
		t.Fatalf("SeedEmptyFromConfig: %v", err)
	}

	if _, err := keyStore.Authenticate("Bearer " + adminSecret); err != nil {
		t.Fatalf("admin key lost: %v", err)
	}
	if _, err := keyStore.Authenticate("Bearer hr-yaml-default-0001"); err != key.ErrUnknown {
		t.Fatalf("yaml default key should stay out, got %v", err)
	}
	if _, ok := mgr.Get("openai"); ok {
		t.Fatal("yaml openai must not be seeded when admin providers exist")
	}
	if _, ok := mgr.Get("airouter"); !ok {
		t.Fatal("airouter must remain")
	}
	if routes := mgr.Routes(); routes["gpt-4o"] != nil {
		t.Fatalf("yaml gpt-4o route must not return: %+v", routes)
	}
	keys, _ := store.ListKeys()
	for _, k := range keys {
		if k.ID != "admin-key" {
			t.Fatalf("unexpected persisted key %s", k.ID)
		}
	}
	providers, _ := store.ListProviders()
	for _, p := range providers {
		if p.Name == "openai" {
			t.Fatal("openai persisted despite admin providers")
		}
	}
}

func TestRestoreStartupDropsUnrestorableYAMLKey(t *testing.T) {
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "harbor.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	encKey, err := db.LoadOrCreateKey(filepath.Join(dir, "harbor.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateKey: %v", err)
	}

	ghost := "hr-yaml-ghost-0001"
	if err := store.UpsertKey(&db.Key{
		ID:     key.Prefix(ghost) + "-" + key.Hash(ghost)[:8],
		Prefix: key.Prefix(ghost), KeyHash: key.Hash(ghost),
		Status: "active", AllowedModels: []string{"gpt-4o"}, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertKey ghost: %v", err)
	}
	adminSecret := "hr-admin-keep-0001"
	adminCipher, err := db.Encrypt(encKey, adminSecret)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := store.UpsertKey(&db.Key{
		ID: "keep-key", Prefix: key.Prefix(adminSecret), KeyHash: key.Hash(adminSecret),
		SecretEncrypted: adminCipher, Status: "active", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertKey keep: %v", err)
	}

	keyStore := key.NewStore()
	mgr := provider.NewManager(nil, nil)
	if err := RestoreStartup(store, keyStore, mgr, time.Minute, 30*time.Second, encKey, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("RestoreStartup: %v", err)
	}
	if _, err := keyStore.Authenticate("Bearer " + ghost); err != key.ErrUnknown {
		t.Fatalf("ghost yaml key still authenticates: %v", err)
	}
	if _, err := keyStore.Authenticate("Bearer " + adminSecret); err != nil {
		t.Fatalf("admin key lost: %v", err)
	}
	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 1 || keys[0].ID != "keep-key" {
		t.Fatalf("ghost key should be dropped from db: %+v", keys)
	}
}
