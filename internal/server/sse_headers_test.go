// SSE 响应头与首帧回归测试：对齐标准网关的 Content-Type，且不长出注释帧。
package server

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"harbor/internal/config"
)

// TestSSEHeadersAndFirstFrame 验证：
//  1. Content-Type 带 charset=UTF-8；
//  2. 不下发 HTTP/2 非法的 Connection 头；
//  3. 关闭心跳时首帧直接是 data:，不会先出现 ": keepalive" 注释。
func TestSSEHeadersAndFirstFrame(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Stream bool `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || !request.Stream {
			http.Error(w, "stream flag missing", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl-sse\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"alpha\"}}]}\n\n")
		w.(http.Flusher).Flush()
		// 保持连接直到客户端收完首个事件，避免提前 EOF 干扰断言。
		<-r.Context().Done()
	}))
	t.Cleanup(ups.Close)

	off := false
	cfg := testConfig()
	cfg.Keepalive = config.KeepaliveConfig{Enabled: &off}
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	raw, err := json.Marshal(map[string]any{
		"model": "mock-1", "stream": true,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Errorf("Content-Type=%q, want text/event-stream; charset=utf-8", got)
	}
	if got := resp.Header.Get("Connection"); got != "" {
		t.Errorf("Connection header must be absent on streaming responses, got %q", got)
	}
	if !strings.Contains(resp.Header.Get("Cache-Control"), "no-transform") || resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Errorf("missing anti-buffering headers: %v", resp.Header)
	}

	first, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatalf("read first line: %v", err)
	}
	if !strings.HasPrefix(first, "data: ") {
		t.Fatalf("first frame must be data, got %q", first)
	}
	if !strings.Contains(first, `"alpha"`) {
		t.Fatalf("first frame lost the upstream delta: %q", first)
	}
}
