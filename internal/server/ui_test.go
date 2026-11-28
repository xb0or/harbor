package server

// 管理前端与请求列表端点测试：
//   - GET / 与自定义管理入口返回内嵌页面；旧 /admin 必须 404
//   - 页面登录脚本必须先校验 /api/stats，错误 token 不能直接进入管理台
//   - GET /api/requests：无 token 401；纯内存模式列出 recent；持久化模式查 SQLite

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harbor/internal/db"
	"harbor/internal/key"
)

func fetchBody(t *testing.T, url string) (int, string, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
}

func fetchHeaders(t *testing.T, url string) http.Header {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Header.Clone()
}

func TestUI_Pages(t *testing.T) {
	up := newMockUpstream(10 * time.Millisecond)
	ups := httptest.NewServer(up.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	adminPath := cfg.Admin.PathValue()

	status, ct, src := fetchBody(t, ts.URL+"/")
	if status != 200 {
		t.Fatalf("GET / status=%d want 200", status)
	}
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("GET / content-type=%q", ct)
	}
	if !strings.Contains(src, "Harbor") {
		t.Fatal("GET / body missing page marker")
	}
	if !strings.Contains(src, "/api/public/status") {
		t.Fatal("GET / must reference /api/public/status")
	}
	if strings.Contains(src, "Harbor 本身只提供") {
		t.Fatal("GET / leftover 'no frontend' copy")
	}
	if strings.Contains(src, `href="/admin"`) {
		t.Fatal("GET / must not expose the legacy admin entry")
	}
	if !strings.Contains(src, "https://github.com/xb0or/harbor") {
		t.Fatal("GET / missing GitHub link")
	}
	if status, _, _ := fetchBody(t, ts.URL+"/admin"); status != http.StatusNotFound {
		t.Fatalf("GET /admin status=%d want 404", status)
	}

	status, ct, src = fetchBody(t, ts.URL+adminPath)
	if status != 200 {
		t.Fatalf("GET %s status=%d want 200", adminPath, status)
	}
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("GET %s content-type=%q", adminPath, ct)
	}
	if !strings.Contains(src, "Harbor") {
		t.Fatalf("GET %s body missing page marker", adminPath)
	}
	if !strings.Contains(src, `id="login-err"`) {
		t.Fatalf("GET %s missing login error element", adminPath)
	}
	if !strings.Contains(src, "https://github.com/xb0or/harbor") {
		t.Fatalf("GET %s missing GitHub link", adminPath)
	}
	if strings.Contains(src, "unpkg.com") || strings.Contains(src, "cdn.jsdelivr.net") || strings.Contains(src, "alpinejs") {
		t.Fatalf("GET %s must not load CDN scripts", adminPath)
	}
	if strings.Contains(src, "bg-canvas") {
		t.Fatalf("GET %s leftover particle canvas", adminPath)
	}

	hdr := fetchHeaders(t, ts.URL+adminPath)
	csp := hdr.Get("Content-Security-Policy")
	if !strings.Contains(csp, "style-src 'self'") || !strings.Contains(csp, "script-src 'self'") {
		t.Fatalf("GET %s missing strict CSP, got %q", adminPath, csp)
	}
	if strings.Contains(csp, "unsafe-inline") {
		t.Fatalf("GET %s CSP must not allow unsafe-inline", adminPath)
	}

	_, _, css := fetchBody(t, ts.URL+adminPath+"/admin.css")
	if !strings.Contains(css, "--accent") || !strings.Contains(css, "--surface") {
		t.Fatalf("GET %s/admin.css missing design tokens", adminPath)
	}
	_, jsCT, js := fetchBody(t, ts.URL+adminPath+"/admin.js")
	if !strings.Contains(jsCT, "javascript") {
		t.Fatalf("GET %s/admin.js content-type=%q", adminPath, jsCT)
	}
	if !strings.Contains(js, "token 无效") {
		t.Fatal("GET /admin login must reject invalid token")
	}
	if !strings.Contains(js, "verifyToken") || !strings.Contains(js, "/api/stats") {
		t.Fatal("GET /admin login must probe /api/stats before enter")
	}
	if !strings.Contains(js, "errText(") {
		t.Fatal("GET /admin must format object-shaped errors instead of showing [object Object]")
	}
	if !strings.Contains(js, `pageHead("HARBOR / OVERVIEW"`) || !strings.Contains(js, "statCard(") {
		t.Fatal("overview must expose the redesigned visual hierarchy")
	}
	for _, iconID := range []string{"i-menu", "i-arrow-up-right", "i-activity", "i-circle-check", "i-alert-triangle", "i-layers-3", "i-timer", "i-shield-check"} {
		if !strings.Contains(src, `id="`+iconID+`"`) {
			t.Fatalf("overview icon %q is not available", iconID)
		}
	}
	if strings.Contains(src, "☰") || strings.Contains(js, "☰") {
		t.Fatal("admin UI must not contain emoji or text menu glyphs")
	}
	if strings.Contains(js, "Harbor 本身只提供") {
		t.Fatal("GET /admin leftover 'no frontend' copy")
	}
	if strings.Contains(js, `x-show="editVisible" x-cloak @click.self="closeEdit()"`) {
		t.Fatal("provider edit modal must not close when clicking outside")
	}
	if strings.Contains(js, "style=") {
		t.Fatal("GET /admin.js must not emit inline style attributes (CSP style-src 'self')")
	}
	if !strings.Contains(js, "<progress") {
		t.Fatal("GET /admin.js queue meter must use native <progress>")
	}
	if !strings.Contains(js, "model.copy") || !strings.Contains(js, "copyText") {
		t.Fatal("GET /admin.js model name must support click-to-copy")
	}
	for _, want := range []string{"loadProxy", "loadLogs", "/api/providers/order", "pass_headers", "/api/logs", "req-detail", "req-section", "req-hero", "req-metric"} {
		if !strings.Contains(js, want) {
			t.Fatalf("GET /admin.js missing feature marker %q", want)
		}
	}
	for _, want := range []string{"排队中", "连接中", "等待响应", "响应中", "thinking_level", "withThinkingLevel"} {
		if !strings.Contains(js, want) {
			t.Fatalf("GET /admin.js missing request-log status/thinking marker %q", want)
		}
	}
	if strings.Contains(js, "不展示") {
		t.Fatal("GET /admin.js must not keep the removed hidden/not-visible model state")
	}
	if !strings.Contains(js, `d.getFullYear() < 1000`) {
		t.Fatal("fmtTime must render Go zero-value times (0001-01-01) as an em dash")
	}
	if strings.Contains(js, `"1-01-01"`) {
		t.Fatal("GET /admin.js must not format Go zero-value time as 1-01-01")
	}
	if !strings.Contains(js, `title: "密钥已创建"`) {
		t.Fatal("createKey must show secret in a centered modal")
	}
	if strings.Contains(js, `showToast("新 Key 已创建`) {
		t.Fatal("createKey must not use corner toast for one-time secret")
	}

	resp3, err := http.Get(ts.URL + "/no-such-page")
	if err != nil {
		t.Fatalf("404 probe: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 404 {
		t.Fatalf("/no-such-page status=%d want 404", resp3.StatusCode)
	}
}

func TestUI_ListRequestsInMemory(t *testing.T) {
	up := newMockUpstream(10 * time.Millisecond)
	ups := httptest.NewServer(up.Handler())
	defer ups.Close()
	handler, _, _ := buildTestServer(t, testConfig(), ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/api/requests", nil)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("no-token GET: %v", err)
	}
	r.Body.Close()
	if r.StatusCode != 401 {
		t.Fatalf("no-token status=%d want 401", r.StatusCode)
	}

	adminGet := func(path string) (*http.Response, []byte) {
		t.Helper()
		req, _ := http.NewRequest("GET", ts.URL+path, nil)
		req.Header.Set("Authorization", "Bearer admin-tok")
		rr, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("admin GET %s: %v", path, err)
		}
		defer rr.Body.Close()
		b, _ := io.ReadAll(rr.Body)
		return rr, b
	}

	r0, b0 := adminGet("/api/requests")
	if r0.StatusCode != 200 {
		t.Fatalf("empty list status=%d want 200", r0.StatusCode)
	}
	var l0 struct {
		Requests []*RequestLog `json:"requests"`
		Total    int           `json:"total"`
	}
	if err := json.Unmarshal(b0, &l0); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if len(l0.Requests) != 0 {
		t.Fatalf("expected empty list, got %d", len(l0.Requests))
	}

	doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	doChat(t, ts.URL, "hr-test-key-0001", "no-such-model", nil)

	r1, b1 := adminGet("/api/requests")
	if r1.StatusCode != 200 {
		t.Fatalf("list status=%d want 200", r1.StatusCode)
	}
	var l1 struct {
		Requests []*RequestLog `json:"requests"`
		Total    int           `json:"total"`
	}
	if err := json.Unmarshal(b1, &l1); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(l1.Requests) != 2 {
		t.Fatalf("len=%d want 2", len(l1.Requests))
	}
	if l1.Total != 2 {
		t.Fatalf("total=%d want 2", l1.Total)
	}
	if l1.Requests[0].RequestID == l1.Requests[1].RequestID {
		t.Fatal("duplicate request ids")
	}
	if l1.Requests[0].Status != "error" {
		t.Fatalf("newest status=%q want error", l1.Requests[0].Status)
	}
	if l1.Requests[0].Model != "no-such-model" {
		t.Fatalf("newest model=%q want no-such-model", l1.Requests[0].Model)
	}

	_, b2 := adminGet("/api/requests?limit=1")
	var l2 struct {
		Requests []*RequestLog `json:"requests"`
		Total    int           `json:"total"`
		Limit    int           `json:"limit"`
	}
	if err := json.Unmarshal(b2, &l2); err != nil {
		t.Fatalf("unmarshal limit: %v", err)
	}
	if len(l2.Requests) != 1 {
		t.Fatalf("limit=1 got %d", len(l2.Requests))
	}
	if l2.Total != 2 {
		t.Fatalf("limit=1 total=%d want 2", l2.Total)
	}

	_, b3 := adminGet("/api/requests?status=error")
	var l3 struct {
		Requests []*RequestLog `json:"requests"`
		Total    int           `json:"total"`
	}
	if err := json.Unmarshal(b3, &l3); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if l3.Total != 1 || len(l3.Requests) != 1 || l3.Requests[0].Status != "error" {
		t.Fatalf("status=error got %+v", l3)
	}

	_, b4 := adminGet("/api/requests?q=no-such-model")
	var l4 struct {
		Requests []*RequestLog `json:"requests"`
		Total    int           `json:"total"`
	}
	if err := json.Unmarshal(b4, &l4); err != nil {
		t.Fatalf("unmarshal q: %v", err)
	}
	if l4.Total != 1 || l4.Requests[0].Model != "no-such-model" {
		t.Fatalf("q=no-such-model got %+v", l4)
	}
}

func TestUI_ListRequestsPersisted(t *testing.T) {
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

	up := newMockUpstream(10 * time.Millisecond)
	ups := httptest.NewServer(up.Handler())
	defer ups.Close()
	cfg := testConfig()
	cfg.Storage.KeyPath = filepath.Join(dir, "harbor.key")
	handler, _ := buildTestServerWithStore(t, cfg, ups, store)
	keyStore := key.NewStore()
	for _, kc := range cfg.Keys {
		keyStore.Add(kc.Secret, kc.AllowedModels)
	}
	MirrorStartup(store, keyStore, testProviders(t, cfg, ups), encKey, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(handler)
	defer ts.Close()

	if resp, _ := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil); resp.StatusCode != 200 {
		t.Fatalf("chat status=%d want 200", resp.StatusCode)
	}

	// persist() 异步写库；轮询直到记录出现，避免偶发 0 条。
	deadline := time.Now().Add(3 * time.Second)
	var l struct {
		Requests []*RequestLog `json:"requests"`
		Total    int           `json:"total"`
	}
	for {
		req, _ := http.NewRequest("GET", ts.URL+"/api/requests", nil)
		req.Header.Set("Authorization", "Bearer admin-tok")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET requests: %v", err)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatalf("status=%d want 200 body=%s", r.StatusCode, b)
		}
		if err := json.Unmarshal(b, &l); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(l.Requests) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for persisted request; len=%d body=%s", len(l.Requests), b)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if l.Requests[0].Model != "mock-1" || l.Requests[0].Status != "success" {
		t.Fatalf("record=%+v", l.Requests[0])
	}
}
