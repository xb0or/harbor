// 流式请求头回归测试：禁止 SSE 协商压缩，同时保留普通 JSON 请求的协商行为。
package provider

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestExecuteStreamingHeaders 验证 SSE 的防压缩、防转换请求头不能被自定义头覆盖。
func TestExecuteStreamingHeaders(t *testing.T) {
	cases := []struct {
		name         string
		stream       bool
		headers      map[string]string
		wantEncoding string
		wantCache    string
	}{
		{name: "stream", stream: true, wantEncoding: "identity", wantCache: "no-cache, no-transform"},
		{
			name: "stream_custom_headers", stream: true,
			headers:      map[string]string{"accept-encoding": "gzip", "cache-control": "public"},
			wantEncoding: "identity", wantCache: "no-cache, no-transform",
		},
		{name: "json", wantEncoding: "gzip"},
		{
			name:         "json_custom_headers",
			headers:      map[string]string{"Accept-Encoding": "identity", "Cache-Control": "no-store"},
			wantEncoding: "identity", wantCache: "no-store",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan http.Header, 1)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Clone()
				if tc.stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: [DONE]\n\n")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"choices":[]}`)
			}))
			defer up.Close()

			p := New("stream-test", "openai-compatible", up.URL+"/v1", []string{"m"}, []string{"mock-secret"}, time.Second, time.Second, 1, 0, 0, 0, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resp, err := p.Execute(ctx, &Request{
				Path: "/v1/chat/completions", Body: []byte(`{"model":"m"}`),
				Stream: tc.stream, Headers: tc.headers,
			})
			if err != nil {
				t.Fatal(err)
			}
			if resp.StreamBody != nil {
				defer resp.StreamBody.Close()
				if _, err := io.Copy(io.Discard, resp.StreamBody); err != nil {
					t.Fatal(err)
				}
			}
			header := <-seen
			if got := header.Get("Accept-Encoding"); got != tc.wantEncoding {
				t.Errorf("Accept-Encoding=%q, want %q", got, tc.wantEncoding)
			}
			if got := header.Get("Cache-Control"); got != tc.wantCache {
				t.Errorf("Cache-Control=%q, want %q", got, tc.wantCache)
			}
			wantAccept := "application/json"
			if tc.stream {
				wantAccept = "text/event-stream"
			}
			if got := header.Get("Accept"); !strings.EqualFold(got, wantAccept) {
				t.Errorf("Accept=%q, want %q", got, wantAccept)
			}
		})
	}
}
