package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"harbor/internal/normalize"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
	date := now.Add(2 * time.Minute)
	maxSeconds := int64((1<<63 - 1) / time.Second)
	cases := []struct {
		name, header string
		want         time.Time
	}{
		{"seconds", "90", now.Add(90 * time.Second)},
		{"zero", "0", now},
		{"whitespace", " 30 ", now.Add(30 * time.Second)},
		{"date", date.Format(http.TimeFormat), date},
		{"legacy_date", date.Format(time.RFC850), date},
		{"missing", "", time.Time{}},
		{"invalid", "later", time.Time{}},
		{"negative", "-3", time.Time{}},
		{"signed", "+3", time.Time{}},
		{"fraction", "1.5", time.Time{}},
		{"multiple", "1, 2", time.Time{}},
		{"past_date", now.Add(-time.Minute).Format(http.TimeFormat), time.Time{}},
		{"duration_max", fmt.Sprint(maxSeconds), now.Add(time.Duration(maxSeconds) * time.Second)},
		{"duration_overflow", fmt.Sprint(maxSeconds + 1), time.Time{}},
		{"integer_overflow", "184467440737095516160", time.Time{}},
		{"date_overflow", time.Date(9999, time.January, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat), time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryAfter(tc.header, now); !got.Equal(tc.want) {
				t.Fatalf("parseRetryAfter(%q) = %v, want %v", tc.header, got, tc.want)
			}
		})
	}
}

func TestExecuteRetryAfterFeedback(t *testing.T) {
	date := time.Now().UTC().Add(2 * time.Minute).Truncate(time.Second)
	cases := []struct {
		name, header string
		status       int
		delay        time.Duration
		date         time.Time
	}{
		{"429_seconds", "90", 429, 90 * time.Second, time.Time{}},
		{"503_seconds", "90", 503, 90 * time.Second, time.Time{}},
		{"500_seconds", "90", 500, 90 * time.Second, time.Time{}},
		{"502_seconds", "90", 502, 90 * time.Second, time.Time{}},
		{"504_seconds", "90", 504, 90 * time.Second, time.Time{}},
		{"529_seconds", "90", 529, 90 * time.Second, time.Time{}},
		{"429_date", date.Format(http.TimeFormat), 429, 0, date},
		{"503_date", date.Format(http.TimeFormat), 503, 0, date},
		{"shorter_than_config", "1", 429, 5 * time.Second, time.Time{}},
		{"zero", "0", 429, 5 * time.Second, time.Time{}},
		{"missing", "", 503, 5 * time.Second, time.Time{}},
		{"invalid", "nonsense", 429, 5 * time.Second, time.Time{}},
		{"negative", "-7", 429, 5 * time.Second, time.Time{}},
		{"overflow", "9223372036854775807", 503, 5 * time.Second, time.Time{}},
		{"past_date", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), 429, 5 * time.Second, time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", tc.header)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":{"message":"private-prompt sk-private-secret"}}`))
			}))
			defer up.Close()
			p := New("test-provider", "openai-compatible", up.URL, nil, []string{"sk-private-secret"}, time.Second, 5*time.Second, 1, 0, 0, 0, 0)
			before := time.Now()
			_, err := p.Execute(context.Background(), &Request{Path: "/v1/chat/completions", Body: []byte(`{}`)})
			after := time.Now()
			he, ok := err.(*normalize.Error)
			if !ok || !he.Retryable || he.Provider != p.Name || he.UpstreamHTTP != tc.status {
				t.Fatalf("unexpected error metadata: %v", err)
			}
			if !tc.date.IsZero() {
				if !he.RetryAt.Equal(tc.date) {
					t.Fatalf("RetryAt = %v, want date %v", he.RetryAt, tc.date)
				}
			} else if he.RetryAt.Before(before.Add(tc.delay)) || he.RetryAt.After(after.Add(tc.delay)) {
				t.Fatalf("RetryAt = %v, want [%v, %v]", he.RetryAt, before.Add(tc.delay), after.Add(tc.delay))
			}
			cred := p.Credentials()[0]
			if cred.Status != CredCooling || !cred.CooldownUntil.Equal(he.RetryAt) || !p.NextRecover().Equal(he.RetryAt) {
				t.Fatalf("cooldown differs from error: status=%v until=%v RetryAt=%v", cred.Status, cred.CooldownUntil, he.RetryAt)
			}
			for _, text := range []string{he.Error(), string(normalize.Body(he)), string(normalize.StreamData(he))} {
				if strings.Contains(text, "private-prompt") || strings.Contains(text, "sk-private-secret") {
					t.Fatalf("raw upstream response leaked: %s", text)
				}
			}
		})
	}
}

func TestExecuteHardQuotaAndBadRequestAreNotRetryable(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		class      normalize.Class
		credStatus CredStatus
	}{
		{"hard_quota", `{"error":{"code":"insufficient_quota"}}`, 429, normalize.ClassRateLimit, CredCooling},
		{"prompt_echo", `{"error":{"code":"invalid_request","message":"prompt: rate_limit"}}`, 400, normalize.ClassBadRequest, CredActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "90")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer up.Close()
			p := New("p", "openai-compatible", up.URL, nil, []string{"sk"}, time.Second, time.Second, 1, 0, 0, 0, 0)
			_, err := p.Execute(context.Background(), &Request{Path: "/v1/chat/completions", Body: []byte(`{}`)})
			he, ok := err.(*normalize.Error)
			if !ok || he.Retryable || he.Class != tc.class || p.Credentials()[0].Status != tc.credStatus {
				t.Fatalf("unexpected error/state: %v credential status=%v", err, p.Credentials()[0].Status)
			}
		})
	}
}

func TestMarkFailPreservesCooldownAndDisabledState(t *testing.T) {
	p := &Provider{Cooldown: time.Second}
	later := time.Now().Add(time.Hour)
	cred := &Credential{Status: CredCooling, CooldownUntil: later}
	for _, class := range []normalize.Class{normalize.ClassRateLimit, normalize.ClassUpstream5xx, normalize.ClassTimeout} {
		p.markFail(cred, class)
		if !cred.CooldownUntil.Equal(later) {
			t.Errorf("class %v shortened cooldown: %v, want %v", class, cred.CooldownUntil, later)
		}
	}
	p.markFail(cred, normalize.ClassUpstreamKey)
	for _, class := range []normalize.Class{normalize.ClassRateLimit, normalize.ClassUpstream5xx, normalize.ClassTimeout} {
		p.markFail(cred, class)
		if cred.Status != CredDisabled {
			t.Errorf("class %v revived disabled credential: %v", class, cred.Status)
		}
	}
}
