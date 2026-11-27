// 流式超时模型回归测试：
//   - 长流不套整段 request_timeout；
//   - 空闲读超时立即中止上游并释放并发槽；
//   - 客户端主动断开立即关闭上游并释放并发槽；
//   - 审计记录入站路径 / 上游响应头耗时 / Content-Type。
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harbor/internal/config"
	"harbor/internal/db"
	"harbor/internal/queue"
)

func streamRequestBody(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"model": "mock-1", "stream": true,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newStreamRequest(t *testing.T, ctx context.Context, base string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/chat/completions", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer hr-test-key-0001")
	return req
}

func waitSchedulerIdle(t *testing.T, sched *queue.Scheduler, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sched.Stats().Running == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("scheduler still has %d running jobs after %s", sched.Stats().Running, timeout)
}

func disabledKeepalive() config.KeepaliveConfig {
	off := false
	return config.KeepaliveConfig{Enabled: &off}
}

// TestStreamOutlivesRequestTimeout 长时间流式输出不能被 request_timeout 截断。
func TestStreamOutlivesRequestTimeout(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for i := 0; i < 8; i++ {
			_, _ = fmt.Fprintf(w, "data: {\"id\":\"chatcmpl-long\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"t%d\"}}]}\n\n", i)
			flusher.Flush()
			select {
			case <-time.After(60 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Limits.RequestTimeout = "100ms"
	cfg.Limits.StreamTTFBTimeout = "1s"
	cfg.Limits.StreamIdleTimeout = "2s"
	cfg.Keepalive = disabledKeepalive()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(newStreamRequest(t, context.Background(), ts.URL, streamRequestBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	text := string(body)
	for i := 0; i < 8; i++ {
		if !strings.Contains(text, fmt.Sprintf(`"t%d"`, i)) {
			t.Fatalf("delta t%d lost (stream truncated by request_timeout?): %s", i, text)
		}
	}
	if !strings.Contains(text, "data: [DONE]") {
		t.Fatalf("terminal frame lost: %s", text)
	}
	waitSchedulerIdle(t, sched, 2*time.Second)
}

// TestStreamIdleTimeoutReleasesSlot 上游静默超过 idle 阈值时立即关闭上游并释放槽位。
func TestStreamIdleTimeoutReleasesSlot(t *testing.T) {
	upstreamCanceled := make(chan struct{}, 1)
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl-idle\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		// 之后保持静默，等待 Harbor 因空闲超时主动关闭连接。
		<-r.Context().Done()
		select {
		case upstreamCanceled <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Limits.RequestTimeout = "30s"
	cfg.Limits.StreamTTFBTimeout = "1s"
	cfg.Limits.StreamIdleTimeout = "120ms"
	cfg.Keepalive = disabledKeepalive()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	start := time.Now()
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(newStreamRequest(t, context.Background(), ts.URL, streamRequestBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	if !strings.Contains(string(body), `"first"`) {
		t.Fatalf("first delta lost: %q", body)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("idle timeout took %s, expected about 120ms", elapsed)
	}
	select {
	case <-upstreamCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream connection was not closed after idle timeout")
	}
	waitSchedulerIdle(t, sched, 2*time.Second)
}

// TestStreamTTFBTimeout 上游迟迟不返回响应头时，按 TTFB 超时收敛并释放槽位。
func TestStreamTTFBTimeout(t *testing.T) {
	upstreamCanceled := make(chan struct{}, 1)
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 先读完请求体：net/http 会在 body EOF 后启动后台读，
		// 从而能在客户端取消连接时及时取消 r.Context()。
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
			select {
			case upstreamCanceled <- struct{}{}:
			default:
			}
			return
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Limits.MaxProviderAttempts = 1
	cfg.Limits.RequestTimeout = "30s"
	cfg.Limits.StreamTTFBTimeout = "80ms"
	cfg.Limits.StreamIdleTimeout = "2s"
	cfg.Keepalive = disabledKeepalive()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	start := time.Now()
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(newStreamRequest(t, context.Background(), ts.URL, streamRequestBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status=%d body=%s, want 504", resp.StatusCode, body)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("TTFB timeout took %s", elapsed)
	}
	select {
	case <-upstreamCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream connection was not closed after TTFB timeout")
	}
	waitSchedulerIdle(t, sched, 2*time.Second)
}

// TestClientDisconnectClosesUpstreamAndReleasesSlot 客户端断开时立即关闭上游并释放槽位。
func TestClientDisconnectClosesUpstreamAndReleasesSlot(t *testing.T) {
	upstreamCanceled := make(chan struct{}, 1)
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for {
			_, err := io.WriteString(w, "data: {\"id\":\"chatcmpl-gone\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"tick\"}}]}\n\n")
			if err != nil {
				break
			}
			flusher.Flush()
			select {
			case <-time.After(30 * time.Millisecond):
			case <-r.Context().Done():
				select {
				case upstreamCanceled <- struct{}{}:
				default:
				}
				return
			}
		}
		select {
		case upstreamCanceled <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Limits.RequestTimeout = "30s"
	cfg.Limits.StreamTTFBTimeout = "1s"
	cfg.Limits.StreamIdleTimeout = "5s"
	cfg.Keepalive = disabledKeepalive()
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	resp, err := (&http.Client{}).Do(newStreamRequest(t, ctx, ts.URL, streamRequestBody(t)))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		cancel()
		resp.Body.Close()
		t.Fatalf("read first frame: %v", err)
	}
	cancel()
	resp.Body.Close()

	select {
	case <-upstreamCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream connection was not closed after client disconnect")
	}
	waitSchedulerIdle(t, sched, 2*time.Second)
	// 未收到终止事件就断开，仍必须记为客户端取消。
	if log := waitAuditStatus(t, ts.URL, cfg.Admin.Token, resp.Header.Get("X-Request-Id")); log.Status != "cancelled" || log.ErrorCode != "client_disconnected" {
		t.Fatalf("mid-stream disconnect recorded as %+v", log)
	}
}

// TestRequestAuditRecordsStreamFields 审计表必须记录入站路径、上游响应头耗时与 Content-Type。
func TestRequestAuditRecordsStreamFields(t *testing.T) {
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "harbor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Migrate(); err != nil {
		t.Fatal(err)
	}

	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 人为延迟响应头，确保 upstream_header_ms 可观测。
		time.Sleep(30 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl-audit\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		w.(http.Flusher).Flush()
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Limits.RequestTimeout = "30s"
	cfg.Limits.StreamTTFBTimeout = "2s"
	cfg.Limits.StreamIdleTimeout = "2s"
	cfg.Keepalive = disabledKeepalive()
	handler, _ := buildTestServerWithStore(t, cfg, ups, store)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(newStreamRequest(t, context.Background(), ts.URL, streamRequestBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	requestID := resp.Header.Get("X-Request-Id")
	if requestID == "" {
		t.Fatal("missing X-Request-Id")
	}

	var rec *db.Request
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec, err = store.GetRequest(requestID)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rec == nil {
		t.Fatalf("audit row not persisted: %v", err)
	}
	if rec.InboundPath != "/v1/chat/completions" {
		t.Errorf("inbound_path=%q", rec.InboundPath)
	}
	if !strings.Contains(rec.UpstreamContentType, "text/event-stream") {
		t.Errorf("upstream_content_type=%q", rec.UpstreamContentType)
	}
	if rec.UpstreamHeaderMs < 20 {
		t.Errorf("upstream_header_ms=%d, want >= 20", rec.UpstreamHeaderMs)
	}
	if rec.FirstTokenMs <= 0 {
		t.Errorf("first_token_ms=%d, want > 0", rec.FirstTokenMs)
	}
	if !rec.Stream {
		t.Error("stream flag not recorded")
	}
}
