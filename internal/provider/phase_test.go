package provider

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestExecutePhaseHook 验证 httptrace 阶段：请求写出 → waiting，收到首字节 → responding。
func TestExecutePhaseHook(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","model":"m","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	t.Cleanup(upstream.Close)

	p := New("p", "openai-compatible", upstream.URL, []string{"m"}, []string{"sk-test"}, time.Second, time.Second, 1, 0, 0, 0, 0)
	phases := make(chan string, 8)
	req := &Request{
		Path:      "/v1/chat/completions",
		Body:      []byte(`{"model":"m","messages":[]}`),
		PhaseHook: func(phase string) { phases <- phase },
	}
	resp, err := p.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	seen := map[string]bool{}
	timeout := time.After(time.Second)
	for len(seen) < 2 {
		select {
		case phase := <-phases:
			seen[phase] = true
		case <-timeout:
			t.Fatalf("phases = %v, want waiting and responding", seen)
		}
	}
	if !seen["waiting"] || !seen["responding"] {
		t.Fatalf("phases = %v, want waiting and responding", seen)
	}
}
