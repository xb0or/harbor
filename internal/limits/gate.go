// Package limits 实现 Harbor 的额度 Gate：并发限制与 RPM 滑动窗口。
// 所有 Gate 遵循同一接口，Scheduler 通过 TryAcquire/Release/NextAvailable
// 做事件驱动调度，避免轮询空转 CPU。
package limits

import (
	"math"
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

// concurrencyUnit 把浮点占用系数放大成整数，避免 0.25 / 1.2 这类系数
// 在多次加减后产生累计误差。
const concurrencyUnit int64 = 1000

// ConcurrencyGate 是加权信号量式并发限制。
//
// 一个请求可以占用 0.25 / 0.5 / 1 / 1.2 / 3 个槽：
//   - 轻量模型 0.25：4 个并发请求才占满 1 个槽；
//   - 默认 1.0：与改造前完全一致；
//   - 重型模型 3：一个请求占 3 个槽。
type ConcurrencyGate struct {
	name      string
	limit     int   // 槽数；<=0 表示不限制
	usedUnits int64 // 已占用槽数 × concurrencyUnit
	mu        sync.Mutex
}

// NewConcurrency 构造并发 Gate。limit<=0 表示不限制。
func NewConcurrency(name string, limit int) *ConcurrencyGate {
	return &ConcurrencyGate{name: name, limit: limit}
}

func (g *ConcurrencyGate) Name() string { return g.name }

// costUnits 把浮点系数转成整数单位；非法/非正系数按 1 处理。
func costUnits(cost float64) int64 {
	if cost <= 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return concurrencyUnit
	}
	units := int64(math.Round(cost * float64(concurrencyUnit)))
	if units < 1 {
		units = 1
	}
	return units
}

func (g *ConcurrencyGate) TryAcquire() bool { return g.TryAcquireCost(1) }

// TryAcquireCost 尝试占用 cost 个并发槽；成功返回 true。
func (g *ConcurrencyGate) TryAcquireCost(cost float64) bool {
	need := costUnits(cost)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.limit > 0 && g.usedUnits+need > int64(g.limit)*concurrencyUnit {
		return false
	}
	// 不限制时也累计占用，便于之后开启上限时保留已在途的请求。
	g.usedUnits += need
	return true
}

// UpdateLimit 更新并发上限；已有占用保留，降限后待已有请求释放再放行。
func (g *ConcurrencyGate) UpdateLimit(limit int) {
	g.mu.Lock()
	g.limit = limit
	g.mu.Unlock()
}

func (g *ConcurrencyGate) Release() { g.ReleaseCost(1) }

// ReleaseCost 释放 cost 个并发槽；释放量超过已占用时归零。
func (g *ConcurrencyGate) ReleaseCost(cost float64) {
	need := costUnits(cost)
	g.mu.Lock()
	if g.usedUnits >= need {
		g.usedUnits -= need
	} else {
		g.usedUnits = 0
	}
	g.mu.Unlock()
}

// NextAvailable：并发满时释放时刻不可预测（依赖事件），返回 ok=false。
func (g *ConcurrencyGate) NextAvailable() (time.Time, bool) {
	return g.NextAvailableCost(1)
}

// NextAvailableCost 判断 cost 个槽是否有可预期的唤醒时间。
func (g *ConcurrencyGate) NextAvailableCost(cost float64) (time.Time, bool) {
	need := costUnits(cost)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.limit <= 0 || g.usedUnits+need <= int64(g.limit)*concurrencyUnit {
		return time.Now(), true
	}
	return time.Time{}, false
}

// Used 当前占用槽数（向上取整，兼容原有整数调用方）。
func (g *ConcurrencyGate) Used() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return int(math.Ceil(float64(g.usedUnits) / float64(concurrencyUnit)))
}

// UsedCost 当前占用槽数（保留小数，供加权调度与诊断使用）。
func (g *ConcurrencyGate) UsedCost() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return float64(g.usedUnits) / float64(concurrencyUnit)
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
