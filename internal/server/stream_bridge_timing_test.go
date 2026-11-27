// 协议桥时序回归：上游只发 data 行、不发空行时，Responses/Messages 桥
// 必须逐行成帧立即下发，不能攒到 EOF。
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMessagesBridgeFlushesWithoutBlankLines(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for i := 0; i < 6; i++ {
			_, _ = fmt.Fprintf(w, "data: {\"id\":\"chatcmpl-noblank\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"t%d\"}}]}\n", i)
			f.Flush()
			time.Sleep(80 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl-noblank\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":6,\"total_tokens\":9}}\n")
		_, _ = io.WriteString(w, "data: [DONE]\n")
		f.Flush()
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Keepalive = disabledKeepalive()
	cfg.Limits.StreamIdleTimeout = "5s"
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	payload, _ := json.Marshal(map[string]any{
		"model": "mock-1", "stream": true, "max_tokens": 64,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/v1/messages", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
	start := time.Now()
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	var firstDelta time.Duration
	var all strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			all.WriteString(line)
			if firstDelta == 0 && strings.Contains(line, "event: content_block_delta") {
				firstDelta = time.Since(start)
			}
		}
		if err != nil {
			break
		}
	}
	if firstDelta == 0 || firstDelta > 600*time.Millisecond {
		t.Fatalf("first Anthropic delta at %s; bridge buffered: %s", firstDelta, all.String())
	}
	if !strings.Contains(all.String(), "event: message_stop") {
		t.Fatalf("terminal event lost: %s", all.String())
	}
	waitSchedulerIdle(t, sched, 2*time.Second)
}

func TestResponsesBridgeFlushesWithoutBlankLines(t *testing.T) {
	ups := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		// 关键：每帧只写 "data: ...\n"，不发 SSE 空行。
		for i := 0; i < 6; i++ {
			_, _ = fmt.Fprintf(w, "data: {\"id\":\"chatcmpl-noblank\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"t%d\"}}]}\n", i)
			f.Flush()
			time.Sleep(80 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl-noblank\",\"model\":\"mock-1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":6,\"total_tokens\":9}}\n")
		_, _ = io.WriteString(w, "data: [DONE]\n")
		f.Flush()
	}))
	t.Cleanup(ups.Close)

	cfg := testConfig()
	cfg.Keepalive = disabledKeepalive()
	cfg.Limits.StreamIdleTimeout = "5s"
	handler, sched, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	payload, _ := json.Marshal(map[string]any{"model": "mock-1", "input": "hi", "stream": true})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/v1/responses", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
	start := time.Now()
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}

	reader := bufio.NewReader(resp.Body)
	var firstDelta time.Duration
	var all strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			all.WriteString(line)
			if firstDelta == 0 && strings.Contains(line, "response.output_text.delta") {
				firstDelta = time.Since(start)
			}
		}
		if err != nil {
			break
		}
	}
	if firstDelta == 0 {
		t.Fatalf("no response.output_text.delta received: %s", all.String())
	}
	if firstDelta > 600*time.Millisecond {
		t.Fatalf("first delta arrived after %s; bridge buffered until EOF", firstDelta)
	}
	if !strings.Contains(all.String(), `"delta":"t0"`) || !strings.Contains(all.String(), `"delta":"t5"`) {
		t.Fatalf("deltas lost: %s", all.String())
	}
	if !strings.Contains(all.String(), "response.completed") {
		t.Fatalf("terminal event lost: %s", all.String())
	}
	waitSchedulerIdle(t, sched, 2*time.Second)
}
