package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"harbor/internal/config"
)

// flushRecorder 是带 Flush 的假 ResponseWriter。
type flushRecorder struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	hdr   http.Header
	flush int
}

func newFlushRecorder() *flushRecorder { return &flushRecorder{hdr: http.Header{}} }

func (f *flushRecorder) Header() http.Header { return f.hdr }
func (f *flushRecorder) WriteHeader(int)     {}
func (f *flushRecorder) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(p)
}
func (f *flushRecorder) Flush() {
	f.mu.Lock()
	f.flush++
	f.mu.Unlock()
}
func (f *flushRecorder) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.String()
}

// slowReader 先给一块数据，静默一段时间，再给一块，然后 EOF。
type slowReader struct {
	step  int
	gap   time.Duration
	first string
	last  string
}

func (s *slowReader) Read(p []byte) (int, error) {
	switch s.step {
	case 0:
		s.step++
		return copy(p, s.first), nil
	case 1:
		s.step++
		time.Sleep(s.gap)
		return copy(p, s.last), nil
	default:
		return 0, io.EOF
	}
}

func TestCopyStreamEmitsKeepaliveWhenIdle(t *testing.T) {
	s := &Server{}
	s.SetKeepalive(config.KeepaliveConfig{
		Interval: "40ms",
		Style:    "comment",
		Comment:  "keepalive",
	})
	w := newFlushRecorder()
	src := &slowReader{gap: 200 * time.Millisecond, first: "data: one\n\n", last: "data: two\n\n"}

	if err := s.copyStream(context.Background(), w, src, w, nil); err != nil {
		t.Fatalf("copyStream: %v", err)
	}
	out := w.String()
	if !strings.Contains(out, "data: one") || !strings.Contains(out, "data: two") {
		t.Fatalf("payload lost: %q", out)
	}
	if n := strings.Count(out, ": keepalive\n\n"); n < 2 {
		t.Fatalf("expected at least 2 keepalive frames in the 200ms gap, got %d: %q", n, out)
	}
	if !strings.HasPrefix(out, "data: one\n\n") {
		t.Fatalf("payload order changed: %q", out)
	}
}

func TestCopyStreamKeepaliveDisabled(t *testing.T) {
	s := &Server{}
	off := false
	s.SetKeepalive(config.KeepaliveConfig{Enabled: &off, Interval: "20ms"})
	w := newFlushRecorder()
	src := &slowReader{gap: 120 * time.Millisecond, first: "data: one\n\n", last: "data: two\n\n"}
	if err := s.copyStream(context.Background(), w, src, w, nil); err != nil {
		t.Fatalf("copyStream: %v", err)
	}
	if strings.Contains(w.String(), "keepalive") {
		t.Fatalf("keepalive must be silent when disabled: %q", w.String())
	}
}

func TestKeepaliveFrameVariants(t *testing.T) {
	comment := config.KeepaliveConfig{Style: "comment", Comment: "  pi\nng  "}
	if got := comment.Frame(); got != ": pi ng\n\n" {
		t.Fatalf("comment frame = %q", got)
	}
	event := config.KeepaliveConfig{Style: "event"}
	if got := event.Frame(); got != "data: {\"type\":\"keepalive\"}\n\n" {
		t.Fatalf("event frame = %q", got)
	}
	def := config.KeepaliveConfig{}
	if !def.IsEnabled() || def.IntervalDuration() != 15*time.Second {
		t.Fatalf("defaults: enabled=%v interval=%s", def.IsEnabled(), def.IntervalDuration())
	}
	if def.Frame() != ": keepalive\n\n" {
		t.Fatalf("default frame = %q", def.Frame())
	}
}

// 心跳只能落在完整事件之后：上游把一条 data 拆成两段时，
// 中途插入注释会把 JSON 截断（历史上真实的流式失效原因）。
func TestCopyStreamKeepaliveWaitsForEventBoundary(t *testing.T) {
	s := &Server{}
	s.SetKeepalive(config.KeepaliveConfig{
		Interval: "30ms",
		Style:    "comment",
		Comment:  "keepalive",
	})
	w := newFlushRecorder()
	src := &slowReader{gap: 120 * time.Millisecond, first: "data: {\"content\":\"he", last: "llo\"}\n\n"}

	if err := s.copyStream(context.Background(), w, src, w, nil); err != nil {
		t.Fatalf("copyStream: %v", err)
	}
	out := w.String()
	if strings.Contains(out, "he: keepalive") {
		t.Fatalf("keepalive injected inside an unfinished event: %q", out)
	}
	if !strings.Contains(out, "data: {\"content\":\"hello\"}\n\n") {
		t.Fatalf("event bytes corrupted: %q", out)
	}
}
