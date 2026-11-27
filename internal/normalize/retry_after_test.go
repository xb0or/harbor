package normalize

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFinalizePreservesRetryMetadata(t *testing.T) {
	for _, status := range []int{429, 529, 500, 502, 503, 504, 401, 400} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			last := Upstream("private-provider", status, `{"error":{"message":"private-prompt"}}`)
			last.RetryAt = time.Now().Add(time.Minute)
			original := *last
			got := Finalize([]*Error{Upstream("earlier", 504, ""), last})
			if got.Class != last.Class || got.Provider != last.Provider || got.UpstreamHTTP != status || !got.RetryAt.Equal(last.RetryAt) || got.Reason != last.Reason {
				t.Fatalf("metadata lost: got %+v, want %+v", got, last)
			}
			if got.Retryable || *last != original || got == last {
				t.Fatalf("finalization must stop retries without mutating last: got %+v last %+v", got, last)
			}
			want := ErrUpstream
			if last.Class == ClassRateLimit {
				want = ErrRateLimited
			} else if last.Class == ClassTimeout {
				want = ErrTimeout
			}
			if got.Status != want.Status || string(Body(got)) != string(Body(want)) {
				t.Fatalf("final mapping changed: %+v", got)
			}
		})
	}
	if got := Finalize(nil); got != ErrInternal {
		t.Fatalf("empty finalization = %+v", got)
	}
}

func TestWriteRetryAfter(t *testing.T) {
	cases := []struct {
		name   string
		status int
		delay  time.Duration
		want   string
	}{
		{"429_future_ceil", 429, 3500 * time.Millisecond, "4"},
		{"503_future_ceil", 503, 3500 * time.Millisecond, "4"},
		{"504_future_ceil", 504, 3500 * time.Millisecond, "4"},
		{"429_subsecond", 429, 500 * time.Millisecond, "1"},
		{"429_missing", 429, 0, "2"},
		{"429_past", 429, -time.Minute, "2"},
		{"503_missing", 503, 0, "2"},
		{"504_past", 504, -time.Minute, ""},
		{"502_unchanged", 502, time.Minute, ""},
		{"400_unchanged", 400, time.Minute, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New(ErrRateLimited, "private-reason")
			e.Status = tc.status
			w := httptest.NewRecorder()
			if tc.delay != 0 {
				e.RetryAt = time.Now().Add(tc.delay)
			}
			Write(w, e, "request-123")
			if got := w.Header().Get("Retry-After"); got != tc.want {
				t.Fatalf("Retry-After = %q, want %q", got, tc.want)
			}
			if w.Code != tc.status || w.Header().Get("X-Request-Id") != "request-123" || w.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("status/headers = %d %v", w.Code, w.Header())
			}
			if w.Body.String() != string(BodyWithRequestID(e, "request-123")) {
				t.Fatalf("unexpected wire body: %s", w.Body.String())
			}
			// 回复体应携带 request_id 供客户端/运维定位请求。
			if !strings.Contains(w.Body.String(), `"request_id":"request-123"`) {
				t.Fatalf("missing request_id in wire body: %s", w.Body.String())
			}
		})
	}
}

func TestRetryMetadataStaysInternal(t *testing.T) {
	e := Upstream("private-provider", 429, `{"error":{"message":"private-prompt sk-private-secret"}}`)
	e.RetryAt = time.Now().Add(time.Hour)
	if strings.Contains(e.Reason, "private-prompt") || strings.Contains(e.Error(), "sk-private-secret") {
		t.Fatalf("raw response leaked to error: %v", e)
	}
	e.Reason = "private-reason sk-private-secret"
	for name, raw := range map[string][]byte{"body": Body(e), "stream": StreamData(e)} {
		t.Run(name, func(t *testing.T) {
			for _, secret := range []string{"private-", "sk-", "RetryAt", "retry_at", "Provider", "UpstreamHTTP", "Retryable", e.RetryAt.Format(time.RFC3339)} {
				if strings.Contains(string(raw), secret) {
					t.Fatalf("internal metadata leaked: %s", raw)
				}
			}
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(raw, &wire); err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(wire["error"], &fields); err != nil || len(fields) != 3 {
				t.Fatalf("wire error changed: %s (%v)", raw, err)
			}
		})
	}
}

func TestClassifyRetryFeedback(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		class     Class
		retryable bool
	}{
		{"http429", 429, `{"error":{"message":"busy"}}`, ClassRateLimit, true},
		{"http529", 529, "overloaded", ClassRateLimit, true},
		{"structured_code", 400, `{"error":{"code":"rate_limit_exceeded"}}`, ClassRateLimit, true},
		{"structured_type", 400, `{"error":{"type":"rate_limit_error"}}`, ClassRateLimit, true},
		{"hard_quota_code", 429, `{"error":{"code":"insufficient_quota","type":"rate_limit_error"}}`, ClassRateLimit, false},
		{"hard_quota_type", 400, `{"error":{"code":"rate_limit_exceeded","type":"insufficient_quota"}}`, ClassRateLimit, false},
		{"prompt_field", 400, `{"prompt":"explain rate_limit and insufficient_quota"}`, ClassBadRequest, false},
		{"echoed_prompt", 400, `{"error":{"code":"invalid_request","message":"user prompt: rate limit"}}`, ClassBadRequest, false},
		{"quota_message", 400, `{"error":{"message":"prompt contains insufficient_quota"}}`, ClassBadRequest, false},
		{"plain_prompt", 400, "invalid prompt: explain rate_limit", ClassBadRequest, false},
		{"json_string", 400, `"rate_limit"`, ClassBadRequest, false},
		{"plain_error", 503, "Rate limit exceeded", ClassRateLimit, true},
		{"bad_request", 400, "invalid request", ClassBadRequest, false},
		{"service_unavailable", 503, "busy", ClassUpstream5xx, true},
		{"unknown_unchanged", 501, "not implemented", ClassUnknown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class, retryable := Classify(tc.status, tc.body)
			if class != tc.class || retryable != tc.retryable {
				t.Fatalf("Classify = (%v, %v), want (%v, %v)", class, retryable, tc.class, tc.retryable)
			}
		})
	}
}

// 停机收敛为 503 server_restarting：必须带 Retry-After，客户端才能安全重试。
func TestWriteRestartingSetsRetryAfter(t *testing.T) {
	e := New(ErrRestarting, "server restarting: scheduler stopped")
	w := httptest.NewRecorder()
	Write(w, e, "req-restarting")
	if w.Code != 503 {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want 2", got)
	}
	if !strings.Contains(w.Body.String(), "server_restarting") {
		t.Fatalf("unexpected body: %s", w.Body.String())
	}
}
