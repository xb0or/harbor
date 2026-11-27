package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSub2APIResponsesToolStream(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Tools         []struct{ Function struct{ Name string } }
			StreamOptions struct {
				IncludeUsage bool `json:"include_usage"`
			} `json:"stream_options"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || r.URL.Path != "/v1/chat/completions" || len(req.Tools) != 1 || req.Tools[0].Function.Name != "lookup" || !req.StreamOptions.IncludeUsage {
			http.Error(w, `{"error":{"message":"invalid tools/stream_options"}}`, 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for _, data := range []string{
			`{"id":"chatcmpl-sub2api","model":"mock-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_x","type":"function","function":{"name":"lookup","arguments":"{"}}]}}]}`,
			`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"}"}}]},"finish_reason":"tool_calls"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":4,"total_tokens":13}}`,
			`[DONE]`,
		} {
			fmt.Fprintf(w, "data: %s\n\n", data)
			f.Flush()
			time.Sleep(15 * time.Millisecond)
		}
	}))
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	resp, data := doJSON(t, ts.URL, "/v1/responses", cfg.Keys[0].Secret, map[string]any{
		"model": "mock-1", "stream": true, "input": "look it up",
		"tools": []any{map[string]any{"type": "function", "name": "lookup", "parameters": map[string]any{"type": "object"}}},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, data)
	}
	for _, want := range []string{"event: response.output_item.added", "event: response.function_call_arguments.delta", "event: response.completed", `"call_id":"call_x"`, `"input_tokens":9`, `"output_tokens":4`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("missing %s: %s", want, data)
		}
	}
	log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
	if log.Status != "success" || log.TokensIn != 9 || log.TokensOut != 4 {
		t.Fatalf("log=%+v", log)
	}
}

func TestSub2APIResponsesDelayedHTTPFailure(t *testing.T) {
	for _, status := range []int{400, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			mock := newMockUpstream(70 * time.Millisecond)
			mock.statusCode = status
			mock.body = []byte(`{"error":{"message":"PRIVATE_UPSTREAM_DATA"}}`)
			ups := httptest.NewServer(mock.Handler())
			defer ups.Close()
			cfg := testConfig()
			cfg.Keepalive.Interval = "5ms"
			cfg.Limits.MaxProviderAttempts = 1
			handler, _, _ := buildTestServer(t, cfg, ups)
			ts := httptest.NewServer(handler)
			defer ts.Close()
			resp, data := doJSON(t, ts.URL, "/v1/responses", cfg.Keys[0].Secret, map[string]any{"model": "mock-1", "input": "hi", "stream": true})
			if resp.StatusCode != 200 || !strings.Contains(string(data), ": keepalive") {
				t.Fatalf("expected committed SSE: %d %s", resp.StatusCode, data)
			}
			if !strings.Contains(string(data), "event: response.failed") || strings.Contains(string(data), "response.completed") || strings.Contains(string(data), "PRIVATE_UPSTREAM_DATA") {
				t.Fatalf("wrong failure protocol: %s", data)
			}
			want := "invalid_request"
			if status == 429 {
				want = "rate_limit_exceeded"
			}
			if !strings.Contains(string(data), `"code":"`+want+`"`) {
				t.Fatalf("lost error code: %s", data)
			}
		})
	}
}

func TestSub2APIResponsesBrokenStreamIsNotSuccess(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
	}))
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	resp, data := doJSON(t, ts.URL, "/v1/responses", cfg.Keys[0].Secret, map[string]any{"model": "mock-1", "input": "hi", "stream": true})
	if !strings.Contains(string(data), "response.failed") || strings.Contains(string(data), "response.completed") {
		t.Fatalf("truncated stream became success: %s", data)
	}
	log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
	if log.Status != "error" || log.ErrorCode != "upstream_error" {
		t.Fatalf("stream failure recorded as %+v", log)
	}
}

func TestSub2APIResponsesInvalidJSONIsNotSuccess(t *testing.T) {
	mock := newMockUpstream(0)
	mock.body = []byte(`{"error":{"message":"PRIVATE_UPSTREAM_DATA"}}`)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	resp, data := doJSON(t, ts.URL, "/v1/responses", cfg.Keys[0].Secret, map[string]any{"model": "mock-1", "input": "hi"})
	if resp.StatusCode != 502 || strings.Contains(string(data), "PRIVATE_UPSTREAM_DATA") {
		t.Fatalf("invalid upstream returned success/leaked body: %d %s", resp.StatusCode, data)
	}
}

// TestUpstreamErrorDetailBackendVisible：上游 400 带结构化错误码时——
//  1. 对外响应只暴露收敛码 + request_id，不含上游原始 code/type/message；
//  2. 后台 /api/requests/{id} 能看到上游原始错误码/类型/HTTP 状态/详情。
func TestUpstreamErrorDetailBackendVisible(t *testing.T) {
	mock := newMockUpstream(0)
	mock.statusCode = 400
	mock.body = []byte(`{"error":{"message":"bad param: max_tokens too large (PRIVATE_UPSTREAM_DATA)","type":"invalid_request_error","code":"invalid_request"}}`)
	ups := httptest.NewServer(mock.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, data := doChat(t, ts.URL, "hr-test-key-0001", "mock-1", nil)
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d body=%s, want 400", resp.StatusCode, data)
	}
	reqID := resp.Header.Get("X-Request-Id")
	if reqID == "" {
		t.Fatalf("missing X-Request-Id")
	}
	// 对外错误应携带 request_id 供定位，且绝不含上游原始错误细节。
	if !strings.Contains(string(data), `"request_id":"`+reqID+`"`) {
		t.Fatalf("response body missing request_id %q: %s", reqID, data)
	}
	// 对外错误只暴露收敛码 + request_id，绝不含上游原始 message 内容。
	// （收敛 type 本身就是 invalid_request_error，属正常对外字段。）
	for _, secret := range []string{"PRIVATE_UPSTREAM_DATA", "max_tokens", "too large"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("response leaked upstream detail %q: %s", secret, data)
		}
	}

	// 后台审计可看到完整的上游原始错误详情。
	log := sub2APIRequestLog(t, ts.URL, cfg.Admin.Token, reqID)
	if log.Status != "error" || log.ErrorCode != "invalid_request" {
		t.Fatalf("log=%+v", log)
	}
	if log.UpstreamCode != "invalid_request" || log.UpstreamType != "invalid_request_error" || log.UpstreamHTTP != 400 {
		t.Fatalf("upstream error details not persisted: %+v", log)
	}
	if !strings.Contains(log.ErrorDetail, "provider=mock") || !strings.Contains(log.ErrorDetail, "status=400") {
		t.Fatalf("error_detail missing provider/status: %+v", log)
	}
}

func sub2APIRequestLog(t *testing.T, base, token, id string) RequestLog {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/api/requests/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var log RequestLog
	if resp.StatusCode != 200 {
		t.Fatalf("request log status=%d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&log); err != nil {
		t.Fatal(err)
	}
	return log
}
