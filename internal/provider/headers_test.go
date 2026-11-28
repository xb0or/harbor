package provider

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUpstreamHeadersCustomAndPassthrough(t *testing.T) {
	var got http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"x","model":"m","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	t.Cleanup(upstream.Close)

	p := New("p", "openai-compatible", upstream.URL, []string{"m"}, []string{"sk-test"}, time.Second, time.Second, 1, 0, 0, 0, 0)
	p.UpdateHeaders(map[string]string{"X-Custom": "yes"}, true)
	req := &Request{
		Path: "/v1/chat/completions",
		Body: []byte(`{"model":"m","messages":[]}`),
		InboundHeaders: http.Header{
			"X-Client":      {"abc"},
			"User-Agent":    {"client-ua"},
			"Authorization": {"Bearer client-key"},
			"Cookie":        {"secret=1"},
		},
	}
	if _, err := p.Execute(context.Background(), req); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got.Get("X-Custom") != "yes" {
		t.Fatalf("custom header missing: %v", got)
	}
	if got.Get("X-Client") != "abc" || got.Get("User-Agent") != "client-ua" {
		t.Fatalf("passthrough headers missing: %v", got)
	}
	if got.Get("Authorization") != "Bearer sk-test" {
		t.Fatalf("client authorization leaked: %q", got.Get("Authorization"))
	}
	if got.Get("Cookie") != "" {
		t.Fatalf("cookie must not be passed through: %q", got.Get("Cookie"))
	}
}
