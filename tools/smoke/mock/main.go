// mock 上游：OpenAI 兼容 /v1/chat/completions 最小实现（冒烟/联调用）。
// 用法: go run ./tools/smoke/mock [-addr 127.0.0.1:18090] [-delay-ms 120] [-fail-429-rate 0] [-fail-500-rate 0]
package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const respBody = `{"id":"chatcmpl-smoke","object":"chat.completion","created":1,"model":"smoke-1","choices":[{"index":0,"message":{"role":"assistant","content":"hi from smoke upstream"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":6,"total_tokens":18}}`

const err429Body = `{"error":{"message":"smoke upstream rate limited","type":"rate_limit_exceeded","code":"insufficient_quota"}}`
const err500Body = `{"error":{"message":"smoke upstream exploded","type":"server_error","code":"server_error"}}`

func main() {
	addr := flag.String("addr", "127.0.0.1:18090", "listen address")
	delayMs := flag.Int("delay-ms", 120, "upstream processing delay; leave a queue observation window")
	fail429Rate := flag.Float64("fail-429-rate", 0, "probability (0..1) of returning 429 with Retry-After: 1")
	fail500Rate := flag.Float64("fail-500-rate", 0, "probability (0..1) of returning 500")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		delay := time.Duration(*delayMs) * time.Millisecond
		if delay > 0 {
			time.Sleep(delay)
		}
		status := 200

		// 是否流式请求（粗略看 body 是否含 "stream":true）
		stream := false
		if r.Body != nil {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
				if b, _ := body["stream"].(bool); b {
					stream = true
				}
			}
		}

		switch {
		case *fail429Rate > 0 && rand.Float64() < *fail429Rate:
			status = 429
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(err429Body))
		case *fail500Rate > 0 && rand.Float64() < *fail500Rate:
			status = 500
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(err500Body))
		case stream:
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			fl, _ := w.(http.Flusher)
			chunks := []string{
				`data: {"id":"chatcmpl-smoke","object":"chat.completion.chunk","created":1,"model":"smoke-1","choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":null}]}`,
				`data: {"id":"chatcmpl-smoke","object":"chat.completion.chunk","created":1,"model":"smoke-1","choices":[{"index":0,"delta":{"content":" from smoke"},"finish_reason":null}]}`,
				`data: {"id":"chatcmpl-smoke","object":"chat.completion.chunk","created":1,"model":"smoke-1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
				`data: [DONE]`,
			}
			for _, c := range chunks {
				_, _ = w.Write([]byte(c + "\n\n"))
				if fl != nil {
					fl.Flush()
				}
			}
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(respBody))
		}

		path := r.URL.Path
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		log.Printf("[mock] %s %s status=%d latency=%s stream=%v",
			r.Method, path, status, time.Since(t0).Round(time.Millisecond), stream)
	})
	log.Println("smoke upstream on", *addr, "(delay-ms", *delayMs, "fail-429", *fail429Rate, "fail-500", *fail500Rate, ")")
	if err := http.ListenAndServe(*addr, mux); err != nil {
		panic(err)
	}
}
