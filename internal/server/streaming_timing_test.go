// 流式时序回归测试：使用会缓存 gzip 的真实 HTTP 上游验证首个增量不依赖后续输出或 EOF。
package server

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"harbor/internal/config"
)

// TestStreamingDoesNotWaitForUpstreamCompletion 覆盖 Chat、Responses、Messages 与心跳开关。
func TestStreamingDoesNotWaitForUpstreamCompletion(t *testing.T) {
	protocols := []struct {
		name     string
		path     string
		input    map[string]any
		terminal string
	}{
		{
			name: "chat", path: "/v1/chat/completions", terminal: "[DONE]",
			input: map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hi"}}},
		},
		{name: "responses", path: "/v1/responses", terminal: "response.completed", input: map[string]any{"input": "hi"}},
		{
			name: "messages", path: "/v1/messages", terminal: "message_stop",
			input: map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 32},
		},
	}
	for _, protocol := range protocols {
		for _, keepalive := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/keepalive_%t", protocol.name, keepalive), func(t *testing.T) {
				next := make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(next) }) }
				seen := make(chan http.Header, 1)
				ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					seen <- r.Header.Clone()
					var request struct {
						Stream bool `json:"stream"`
					}
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || !request.Stream {
						http.Error(w, "stream flag missing", http.StatusBadRequest)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					var writer io.Writer = w
					var compressed *gzip.Writer
					if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
						w.Header().Set("Content-Encoding", "gzip")
						compressed = gzip.NewWriter(w)
						writer = compressed
						defer compressed.Close()
					}
					_, _ = io.WriteString(writer, "data: {\"id\":\"chatcmpl-timing\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"alpha\"}}]}\n\n")
					// 只刷新 HTTP，不刷新压缩器：复现常见代理把增量攒到结束的行为。
					w.(http.Flusher).Flush()
					select {
					case <-next:
					case <-r.Context().Done():
						return
					}
					_, _ = io.WriteString(writer, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"omega\"},\"finish_reason\":\"stop\"}]}\n\n")
					_, _ = io.WriteString(writer, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\ndata: [DONE]\n\n")
				}))
				t.Cleanup(ups.Close)
				cfg := testConfig()
				cfg.Keepalive = config.KeepaliveConfig{Enabled: &keepalive, Interval: "50ms", Style: "comment"}
				handler, _, _ := buildTestServer(t, cfg, ups)
				ts := httptest.NewServer(handler)
				t.Cleanup(ts.Close)
				// 先解除上游等待，再关闭测试服务器，失败时也不会遗留挂起连接。
				t.Cleanup(release)

				input := map[string]any{"model": "mock-1", "stream": true}
				for k, v := range protocol.input {
					input[k] = v
				}
				raw, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				req, err := http.NewRequest(http.MethodPost, ts.URL+protocol.path, strings.NewReader(string(raw)))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
				start := time.Now()
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
					t.Fatalf("not SSE: status=%d headers=%v", resp.StatusCode, resp.Header)
				}
				if !strings.Contains(resp.Header.Get("Cache-Control"), "no-transform") || resp.Header.Get("X-Accel-Buffering") != "no" {
					t.Errorf("missing downstream anti-buffering headers: %v", resp.Header)
				}

				reader := bufio.NewReader(resp.Body)
				type firstDelta struct {
					line string
					err  error
					at   time.Duration
				}
				first := make(chan firstDelta, 1)
				go func() {
					for {
						line, err := reader.ReadString('\n')
						if strings.Contains(line, `"alpha"`) || err != nil {
							first <- firstDelta{line: line, err: err, at: time.Since(start)}
							return
						}
					}
				}()
				var delta firstDelta
				select {
				case delta = <-first:
				case <-time.After(time.Second):
					t.Error("first delta was buffered until upstream continuation/EOF")
					release()
					delta = <-first
				}
				if delta.err != nil || !strings.Contains(delta.line, `"alpha"`) {
					t.Fatalf("first delta: line=%q err=%v", delta.line, delta.err)
				}
				release()
				tail, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(tail), `"omega"`) || !strings.Contains(string(tail), protocol.terminal) {
					t.Fatalf("stream tail/terminal lost: %s", tail)
				}
				if header := <-seen; header.Get("Accept-Encoding") != "identity" {
					t.Errorf("upstream compression still enabled: %q", header.Get("Accept-Encoding"))
				}
				t.Logf("first delta received at %s while upstream was blocked; total=%s", delta.at.Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
			})
		}
	}
}
