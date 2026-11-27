// Package limits 实现 Harbor 的额度 Gate：并发限制与 RPM 滑动窗口。
// 所有 Gate 遵循同一接口，Scheduler 通过 TryAcquire/Release/NextAvailable
// 做事件驱动调度，避免轮询空转 CPU。
package limits

import (
	"sync"
	"time"
)

// Gate 是一个可检查 / 可释放的额度单元。
type Gate interface {
	// Name 唯一标识（如 key:abc:concurrency）
	Name() string
	// TryAcquire 非阻塞尝试占用；成功返回 true。
	TryAcquire() bool
	// Release 释放一个占用（仅对可释放的 gate 有意义）。
	Release()
	// NextAvailable 返回可再次成功 TryAcquire 的最早时间。
	// ok=false 表示释放时间不可预测（只能等 Release 事件），不应设置定时器。
	NextAvailable() (time.Time, bool)
}

// ConcurrencyGate 是信号量式并发限制。
type ConcurrencyGate struct {
	name  string
	limit int
	used  int
	mu    sync.Mutex
}

// NewConcurrency 构造并发 Gate。limit<=0 表示不限制。
func NewConcurrency(name string, limit int) *ConcurrencyGate {
	return &ConcurrencyGate{name: name, limit: limit}
}

func (g *ConcurrencyGate) Name() string { return g.name }

func (g *ConcurrencyGate) TryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.limit > 0 && g.used >= g.limit {
		return false
	}
	// Unlimited still tracks in-flight requests so enabling a limit cannot
	// forget work already sent upstream.
	g.used++
	return true
}

// UpdateLimit 更新并发上限；已有占用保留，降限后待已有请求释放再放行。
func (g *ConcurrencyGate) UpdateLimit(limit int) {
	g.mu.Lock()
	g.limit = limit
	g.mu.Unlock()
}

func (g *ConcurrencyGate) Release() {
	g.mu.Lock()
	if g.used > 0 {
		g.used--
	}
	g.mu.Unlock()
}

// NextAvailable：并发满时释放时刻不可预测（依赖事件），返回 ok=false。
func (g *ConcurrencyGate) NextAvailable() (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.limit <= 0 || g.used < g.limit {
		return time.Now(), true
	}
	return time.Time{}, false
}

// Used 当前占用数。
func (g *ConcurrencyGate) Used() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.used
}

// RateTicket 表示一次已计入 RPM 窗口的请求，可在尚未提交时撤销。
type RateTicket struct {
	gate  *RateGate
	stamp time.Time
	mu    sync.Mutex
	used  bool
}

func (g *RateGate) TryAcquireTicket() (*RateTicket, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if g.limit <= 0 {
		return &RateTicket{gate: g, stamp: now, used: true}, true
	}
	g.sweep(now)
	if g.settled >= g.limit {
		return nil, false
	}
	g.stamps = append(g.stamps, now)
	g.settled++
	return &RateTicket{gate: g, stamp: now, used: true}, true
}

func (t *RateTicket) Cancel() {
	if t == nil || t.gate == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.used {
		return
	}
	t.gate.mu.Lock()
	defer t.gate.mu.Unlock()
	if t.gate.limit <= 0 {
		t.used = false
		return
	}
	for i, stamp := range t.gate.stamps {
		if stamp.Equal(t.stamp) {
			copy(t.gate.stamps[i:], t.gate.stamps[i+1:])
			last := len(t.gate.stamps) - 1
			t.gate.stamps[last] = time.Time{}
			t.gate.stamps = t.gate.stamps[:last]
			if t.gate.settled > 0 {
				t.gate.settled--
			}
			break
		}
	}
	t.used = false
}

// 窗口按时间自然滑出，不依赖显式 Release。
type RateGate struct {
	name    string
	limit   int
	window  time.Duration
	mu      sync.Mutex
	stamps  []time.Time // 窗口内的请求开始时间
	settled int         // 已获准的请求数（含未执行完的）
}

// NewRate 构造 RPM Gate。limit<=0 表示不限制。
func NewRate(name string, limit int, window time.Duration) *RateGate {
	return &RateGate{name: name, limit: limit, window: window}
}

func (g *RateGate) Name() string { return g.name }

func (g *RateGate) TryAcquire() bool {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.limit <= 0 {
		return true
	}
	g.sweep(now)
	if g.settled >= g.limit {
		return false
	}
	g.stamps = append(g.stamps, now)
	g.settled++
	return true
}

// UpdateLimit 更新 RPM 上限并保留当前窗口内的计数。
func (g *RateGate) UpdateLimit(limit int) {
	g.mu.Lock()
	g.limit = limit
	g.mu.Unlock()
}

// Release 对 RPM 无操作（窗口自然滑出）；保留接口一致性。
func (g *RateGate) Release() {}

// NextAvailable：RPM 满时返回窗口内最老记录的滑出时间，可精确设置定时器。
func (g *RateGate) NextAvailable() (time.Time, bool) {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.limit <= 0 {
		return now, true
	}
	g.sweep(now)
	if g.settled < g.limit {
		return now, true
	}
	if len(g.stamps) == 0 {
		return now, true
	}
	return g.stamps[0].Add(g.window), true
}

// Sweep 移除过期记录（公开给 scheduler 定时调用也无妨）。
func (g *RateGate) Sweep() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweep(time.Now())
}

func (g *RateGate) sweep(now time.Time) {
	cutoff := now.Add(-g.window)
	kept := g.stamps[:0]
	for _, t := range g.stamps {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	g.stamps = kept
	g.settled = len(kept)
}

// Count 当前窗口内计数。
func (g *RateGate) Count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweep(time.Now())
	return g.settled
}
