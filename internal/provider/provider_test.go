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

type chunkReadCloser struct {
	reader *strings.Reader
	chunk  int
}

func (r *chunkReadCloser) Read(p []byte) (int, error) {
	if len(p) > r.chunk {
		p = p[:r.chunk]
	}
	return r.reader.Read(p)
}

func (r *chunkReadCloser) Close() error { return nil }

func TestParseUsageDistinguishesMissingUsage(t *testing.T) {
	usage, known := parseUsage([]byte(`{"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`))
	if !known || usage.PromptTokens != 3 || usage.CompletionTokens != 4 || usage.TotalTokens != 7 {
		t.Fatalf("usage=%+v known=%v", usage, known)
	}
	if _, known := parseUsage([]byte(`{"id":"no-usage"}`)); known {
		t.Fatal("missing usage must not be treated as a known zero")
	}
}

func TestStreamUsageBodyTracksSplitSSEUsage(t *testing.T) {
	stream := "data: {\"choices\":[]}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":6,\"total_tokens\":11}}\n\n" +
		"data: [DONE]\n\n"
	body := newStreamUsageBody(&chunkReadCloser{reader: strings.NewReader(stream), chunk: 3})
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(got) != stream {
		t.Fatalf("stream changed during tracking: %q", got)
	}
	usage, known := body.StreamUsage()
	if !known || usage.PromptTokens != 5 || usage.CompletionTokens != 6 || usage.TotalTokens != 11 {
		t.Fatalf("usage=%+v known=%v", usage, known)
	}
}

// TestStreamUsageBodyBoundedPending 验证超长无换行 SSE 数据不会让
// 旁路解析缓冲无限增长，且后续合法 usage 行仍可被解析。
func TestStreamUsageBodyBoundedPending(t *testing.T) {
	junk := strings.Repeat("x", maxPendingLine+1024) // 无换行的超长行
	usageLine := "data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n"
	stream := junk + "\n" + usageLine
	body := newStreamUsageBody(&chunkReadCloser{reader: strings.NewReader(stream), chunk: 1 << 16})
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(got) != stream {
		t.Fatalf("stream changed during tracking")
	}
	usage, known := body.StreamUsage()
	if !known || usage.PromptTokens != 1 || usage.CompletionTokens != 2 || usage.TotalTokens != 3 {
		t.Fatalf("usage=%+v known=%v", usage, known)
	}
}

func TestJoinURL(t *testing.T) {
	cases := []struct {
		base, path, want string
	}{
		{"https://api.airouter.ch/v1", "/v1/chat/completions", "https://api.airouter.ch/v1/chat/completions"},
		{"https://api.airouter.ch/v1/", "/v1/chat/completions", "https://api.airouter.ch/v1/chat/completions"},
		{"https://api.airouter.ch/v1", "v1/chat/completions", "https://api.airouter.ch/v1/chat/completions"},
		{"https://api.airouter.ch/v1", "/chat/completions", "https://api.airouter.ch/v1/chat/completions"},
		{"https://api.openai.com/v1", "/v1/embeddings", "https://api.openai.com/v1/embeddings"},
		{"https://host.example", "/v1/chat/completions", "https://host.example/v1/chat/completions"},
		{"https://host.example/openai/v1", "/v1/chat/completions", "https://host.example/openai/v1/chat/completions"},
		{"https://host.example/v1beta", "/v1/chat/completions", "https://host.example/v1beta/v1/chat/completions"},
		{"https://host.example/v1", "/v1", "https://host.example/v1"},
		{"https://host.example/v1", "", "https://host.example/v1"},
	}
	for _, tc := range cases {
		got := JoinURL(tc.base, tc.path)
		if got != tc.want {
			t.Errorf("JoinURL(%q, %q)=%q, want %q", tc.base, tc.path, got, tc.want)
		}
	}
}

func TestExecuteJoinsV1Once(t *testing.T) {
	var gotPath string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"id":"x","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer up.Close()

	p := New("p", "openai-compatible", up.URL+"/v1", []string{"m"}, []string{"sk"}, time.Second, time.Second, 1, 0, 0, 0, 0)
	resp, err := p.Execute(context.Background(), &Request{
		Path: "/v1/chat/completions",
		Body: []byte(`{"model":"m"}`),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("resp=%+v", resp)
	}
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("upstream path=%q, want /v1/chat/completions (got /v1/v1 if JoinURL missing)", gotPath)
	}
}
