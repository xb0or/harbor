// Package logbuf 在内存中保留最近日志，供管理台查看系统日志。
package logbuf

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Entry 是管理台展示的一条日志。
type Entry struct {
	Time    time.Time         `json:"time"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

// Buffer 是固定容量的日志环形缓冲。
type Buffer struct {
	mu      sync.Mutex
	max     int
	entries []Entry
}

// Default 是进程共享的日志缓冲（容量 2000 条）。
var Default = New(2000)

// New 创建指定容量的缓冲。
func New(max int) *Buffer {
	if max <= 0 {
		max = 1000
	}
	return &Buffer{max: max}
}

// Add 写入一条 slog 记录。
func (b *Buffer) Add(rec slog.Record) {
	entry := Entry{
		Time:    rec.Time,
		Level:   strings.ToLower(rec.Level.String()),
		Message: rec.Message,
		Attrs:   map[string]string{},
	}
	rec.Attrs(func(a slog.Attr) bool {
		entry.Attrs[a.Key] = fmt.Sprint(a.Value.Any())
		return true
	})
	if len(entry.Attrs) == 0 {
		entry.Attrs = nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) >= b.max {
		copy(b.entries, b.entries[1:])
		b.entries[len(b.entries)-1] = entry
		return
	}
	b.entries = append(b.entries, entry)
}

// List 按时间倒序返回最近的日志。
func (b *Buffer) List(level string, since time.Time, limit int) []Entry {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	level = strings.ToLower(strings.TrimSpace(level))
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Entry, 0, limit)
	for i := len(b.entries) - 1; i >= 0 && len(out) < limit; i-- {
		e := b.entries[i]
		if level != "" && e.Level != level {
			continue
		}
		if !since.IsZero() && e.Time.Before(since) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// Handler 把日志同时写入 Buffer 和下游 handler。
type Handler struct {
	next  slog.Handler
	buf   *Buffer
	attrs []slog.Attr
}

// NewHandler 包装一个 slog.Handler。
func NewHandler(next slog.Handler) *Handler {
	return &Handler{next: next, buf: Default}
}

// NewHandlerWithBuffer 用指定缓冲包装，便于测试。
func NewHandlerWithBuffer(next slog.Handler, buf *Buffer) *Handler {
	if buf == nil {
		buf = Default
	}
	return &Handler{next: next, buf: buf}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.next == nil {
		return true
	}
	return h.next.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, rec slog.Record) error {
	if len(h.attrs) > 0 {
		bufRec := rec.Clone()
		for _, a := range h.attrs {
			bufRec.AddAttrs(a)
		}
		h.buf.Add(bufRec)
	} else {
		h.buf.Add(rec)
	}
	if h.next == nil {
		return nil
	}
	return h.next.Handle(ctx, rec)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := h.next
	if next != nil {
		next = next.WithAttrs(attrs)
	}
	merged := append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &Handler{next: next, buf: h.buf, attrs: merged}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	next := h.next
	if next != nil {
		next = next.WithGroup(name)
	}
	return &Handler{next: next, buf: h.buf, attrs: h.attrs}
}
