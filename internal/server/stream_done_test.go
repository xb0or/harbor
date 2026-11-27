// 流结束判定回归测试：
//   - 上游发出 [DONE] 后不关连接，Harbor 必须立即结束转发、释放并发槽并记为成功；
//   - 客户端收到 [DONE] 后立即断开（OpenAI SDK / Snow App 的常见行为），
//     不能被误记为 cancelled / client_disconnected。
package server

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const doneTestFrames = "data: {\"id\":\"chatcmpl-done\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n" +
	"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\n" +
	"data: [DONE]\n\n"

// waitAuditStatus 轮询内存审计，直到出现非空状态或超时。
func waitAuditStatus(t *testing.T, base, token, id string) RequestLog {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodGet, base+"/api/requests/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return sub2APIRequestLog(t, base, token, id)
		}
		if resp != nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("audit row %s not found", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestStreamEndsAtDoneEvenIfUpstreamKeepsConnection 上游 [DONE] 后挂住连接时，
// 读到 EOF 才结束的客户端不能被拖到空闲超时。
func TestStreamEndsAtDoneEvenIfUpstreamKeepsConnection(t *testing.T) {
	upstreamReleased := make(chan struct{}, 1)
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, doneTestFrames)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			upstreamReleased <- struct{}{}
		case <-time.After(10 * time.Second):
		}
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Limits.StreamIdleTimeout = "5s"
	cfg.Keepalive = disabledKeepalive()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	start := time.Now()
	resp, err := (&http.Client{Timeout: 4 * time.Second}).Do(newStreamRequest(t, context.Background(), ts.URL, streamRequestBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("stream did not end after [DONE]: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("stream ended %s after start; must end right after [DONE]", elapsed)
	}
	if !strings.HasSuffix(string(body), "data: [DONE]\n\n") {
		t.Fatalf("terminal frame missing: %q", body)
	}
	select {
	case <-upstreamReleased:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream connection still held after [DONE]")
	}
	waitSchedulerIdle(t, sched, 2*time.Second)
	log := waitAuditStatus(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id"))
	if log.Status != "success" || log.TokensOut != 1 || log.Provider == "" {
		t.Fatalf("completed stream recorded as %+v", log)
	}
}

// TestClientClosingAfterSplitDoneLineIsSuccess 上游把 "data: [DONE]\n" 与结尾空行分开发送，
// 客户端读到 [DONE] 行立即断开，仍应记为成功，且下游拿到完整的 [DONE] 事件。
func TestClientClosingAfterSplitDoneLineIsSuccess(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = io.WriteString(w, strings.TrimSuffix(doneTestFrames, "\n"))
		f.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
		_, _ = io.WriteString(w, "\n")
		f.Flush()
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Keepalive = disabledKeepalive()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		resp, err := (&http.Client{}).Do(newStreamRequest(t, ctx, ts.URL, streamRequestBody(t)))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		requestID := resp.Header.Get("X-Request-Id")
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			if strings.Contains(line, "[DONE]") {
				break
			}
			if err != nil {
				cancel()
				t.Fatalf("stream ended before [DONE]: %v", err)
			}
		}
		cancel()
		resp.Body.Close()
		waitSchedulerIdle(t, sched, 2*time.Second)
		log := waitAuditStatus(t, ts.URL, cfg.Admin.Token, requestID)
		if log.Status != "success" || log.ErrorCode != "" {
			t.Fatalf("run %d: client closing after split [DONE] recorded as %+v", i, log)
		}
	}

	// 读到 EOF 的客户端：Harbor 应自行补齐结尾空行并立即结束，不等上游的 "\n"。
	start := time.Now()
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(newStreamRequest(t, context.Background(), ts.URL, streamRequestBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(body), "data: [DONE]\n\n") {
		t.Fatalf("terminal event incomplete: %q", body)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("waited %s for upstream blank line after [DONE]", elapsed)
	}
}

// TestClientClosingAfterDoneIsSuccess 客户端读到 [DONE] 立即断开，不能记成取消。
func TestClientClosingAfterDoneIsSuccess(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, doneTestFrames)
		w.(http.Flusher).Flush()
		// 模拟上游关闭连接前的短暂延迟（线上实测约 90ms）。
		select {
		case <-r.Context().Done():
		case <-time.After(300 * time.Millisecond):
		}
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Keepalive = disabledKeepalive()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := (&http.Client{}).Do(newStreamRequest(t, ctx, ts.URL, streamRequestBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	requestID := resp.Header.Get("X-Request-Id")
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if strings.Contains(line, "[DONE]") {
			break
		}
		if err != nil {
			t.Fatalf("stream ended before [DONE]: %v", err)
		}
	}
	// 与 OpenAI SDK 一致：收到 [DONE] 立即中止连接。
	cancel()
	resp.Body.Close()

	waitSchedulerIdle(t, sched, 2*time.Second)
	log := waitAuditStatus(t, ts.URL, cfg.Admin.Token, requestID)
	if log.Status != "success" || log.ErrorCode != "" || log.TokensOut != 1 {
		t.Fatalf("client closing after [DONE] recorded as %+v", log)
	}
}
