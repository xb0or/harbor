package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestLiveRequestStatusAndThinkingLevel 请求进行中即可在日志里看到阶段，
// 完成后保留 final 状态；reasoning_effort 写入审计并随日志返回。
func TestLiveRequestStatusAndThinkingLevel(t *testing.T) {
	mock := newMockUpstream(300 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	t.Cleanup(ups.Close)
	cfg := testConfig()
	srv, _, _ := buildTestServerWithServer(t, cfg, ups)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	done := make(chan error, 1)
	go func() {
		body := `{"model":"mock-1","reasoning_effort":"max","messages":[{"role":"user","content":"hi"}]}`
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- err
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		done <- nil
	}()

	var live *RequestLog
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, item := range listRequestLogs(t, ts.URL, cfg.Admin.Token) {
			if item.Model == "mock-1" {
				live = item
				break
			}
		}
		if live != nil && isLiveStatus(live.Status) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if live == nil || !isLiveStatus(live.Status) {
		t.Fatalf("live request not visible: %+v", live)
	}
	if live.ThinkingLevel != "max" {
		t.Fatalf("live thinking level = %q, want max", live.ThinkingLevel)
	}
	if !live.FinishedAt.IsZero() {
		t.Fatalf("live request must not have finished_at: %+v", live)
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var final *RequestLog
	for _, item := range listRequestLogs(t, ts.URL, cfg.Admin.Token) {
		if item.Model == "mock-1" {
			final = item
			break
		}
	}
	if final == nil || final.Status != "success" || final.ThinkingLevel != "max" {
		t.Fatalf("final request = %+v", final)
	}
}

func listRequestLogs(t *testing.T, base, token string) []*RequestLog {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/api/requests?limit=20", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Requests []*RequestLog `json:"requests"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Requests
}

func TestThinkingLevelOf(t *testing.T) {
	cases := []struct {
		name string
		meta openAIReqMeta
		want string
	}{
		{"reasoning_effort", openAIReqMeta{ReasoningEffort: "HIGH"}, "high"},
		{"responses reasoning", openAIReqMeta{Reasoning: json.RawMessage(`{"effort":"medium"}`)}, "medium"},
		{"thinking level", openAIReqMeta{Thinking: json.RawMessage(`{"level":"low"}`)}, "low"},
		{"thinking type", openAIReqMeta{Thinking: json.RawMessage(`{"type":"max"}`)}, "max"},
		{"model suffix", openAIReqMeta{Model: "deepseek-v4.1-flash(max)"}, "max"},
		{"model suffix fullwidth", openAIReqMeta{Model: "deepseek（high）"}, "high"},
		{"unknown", openAIReqMeta{Model: "deepseek(custom)"}, ""},
	}
	for _, tc := range cases {
		if got := thinkingLevelOf(tc.meta, tc.meta.Model); got != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
