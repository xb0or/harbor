package logbuf

import (
	"log/slog"
	"testing"
	"time"
)

func TestBufferCapturesAndFilters(t *testing.T) {
	buf := New(10)
	h := NewHandlerWithBuffer(nil, buf)
	log := slog.New(h)
	log.With("request_id", "hr_1").Info("hello", "status", 200)
	log.Warn("warned")
	log.Debug("debugged")
	time.Sleep(5 * time.Millisecond)

	all := buf.List("", time.Time{}, 10)
	if len(all) != 3 {
		t.Fatalf("entries = %d, want 3", len(all))
	}
	if all[0].Message != "debugged" || all[1].Message != "warned" {
		t.Fatalf("order = %+v", all)
	}
	if all[2].Attrs["request_id"] != "hr_1" || all[2].Attrs["status"] != "200" {
		t.Fatalf("attrs = %+v", all[2].Attrs)
	}
	warn := buf.List("warn", time.Time{}, 10)
	if len(warn) != 1 || warn[0].Level != "warn" {
		t.Fatalf("warn filter = %+v", warn)
	}
	if got := buf.List("", time.Now().Add(time.Second), 10); len(got) != 0 {
		t.Fatalf("since filter = %+v", got)
	}
}

func TestBufferCapacity(t *testing.T) {
	buf := New(2)
	h := NewHandlerWithBuffer(nil, buf)
	log := slog.New(h)
	log.Info("one")
	log.Info("two")
	log.Info("three")
	got := buf.List("", time.Time{}, 10)
	if len(got) != 2 || got[0].Message != "three" || got[1].Message != "two" {
		t.Fatalf("capacity = %+v", got)
	}
}
