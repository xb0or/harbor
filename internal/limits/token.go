package limits

import (
	"sync"
	"time"
)

// TokenAcquirer 是带量的 Token Gate 增强接口。
// Scheduler 通过 type-assert 识别：普通 Gate 无参数语义（并发/RPM），
// Token Gate 需要按 job 预扣（TryAcquireTokens）与结算（SettleTokens）。
// 不实现该接口的 Gate 保持原 TryAcquire/Release 行为。
type TokenAcquirer interface {
	Gate
	// TryAcquireTokens 按预扣量 n 占用额度（jobID 用于后续结算定位）。
	TryAcquireTokens(jobID string, n int) bool
	// SettleTokens 按实际用量结算：actual < 预扣 → 回补；actual > 预扣 → 补扣。
	SettleTokens(jobID string, actual int)
}

// tokenEvent 是一次 token 预扣/结算记录。
type tokenEvent struct {
	jobID  string
	tokens int
	at     time.Time
}

// TokenGate 是 Provider 的 Token 额度 Gate：
// TPM（每分钟）/ TPH（每小时）/ TPD（每 24 小时）三滑动窗口。
//
// 语义（计划书 P1）：
//   - 预扣：请求执行前按估计 token 数占用额度；额度不足 → 排队等窗口滑出；
//   - 结算：请求完成后按实际 usage 调整（差量回补/补扣），保证窗口计数精确；
//   - limit<=0 表示该窗口不限量。
//
// 与 RateGate 不同：TokenGate 的占用是"带量"的，且支持按 job 精确结算，
// 因此实现 TokenAcquirer 接口而非纯 Gate 语义。
type TokenGate struct {
	name     string
	tpm      int64 // 60s 窗口上限；<=0 不限
	tph      int64 // 1h 窗口上限；<=0 不限
	tpd      int64 // 24h 窗口上限；<=0 不限
	minuteW  time.Duration
	hourW    time.Duration
	dayW     time.Duration
	mu       sync.Mutex
	minute   []tokenEvent
	hour     []tokenEvent
	day      []tokenEvent
	dayStart time.Time // 用于 TPD 窗口的基准（结算时保证窗口连续）
}

// NewToken 构造 Token Gate。tpm/tph/tpd <=0 表示不限。
func NewToken(name string, tpm, tph, tpd int64) *TokenGate {
	return &TokenGate{
		name:    name,
		tpm:     tpm,
		tph:     tph,
		tpd:     tpd,
		minuteW: time.Minute,
		hourW:   time.Hour,
		dayW:    24 * time.Hour,
	}
}

func (g *TokenGate) Name() string { return g.name }

// TryAcquire 无参占用恒为 true（正常路径由 scheduler 走 TryAcquireTokens）。
func (g *TokenGate) TryAcquire() bool { return true }

// Release 无操作：Token 额度按 job 结算，不靠 Release。
func (g *TokenGate) Release() {}

// TryAcquireTokens 预扣 n 个 token；三窗口均满足才成功。
func (g *TokenGate) TryAcquireTokens(jobID string, n int) bool {
	if n < 0 {
		n = 0
	}
	n64 := int64(n)
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tpm <= 0 && g.tph <= 0 && g.tpd <= 0 {
		return true
	}
	g.sweepLocked(now)
	if g.tpm > 0 && exceeds(g.windowTotalLocked(g.minute), n64, g.tpm) {
		return false
	}
	if g.tph > 0 && exceeds(g.windowTotalLocked(g.hour), n64, g.tph) {
		return false
	}
	if g.tpd > 0 && exceeds(g.windowTotalLocked(g.day), n64, g.tpd) {
		return false
	}
	g.minute = append(g.minute, tokenEvent{jobID: jobID, tokens: n, at: now})
	g.hour = append(g.hour, tokenEvent{jobID: jobID, tokens: n, at: now})
	g.day = append(g.day, tokenEvent{jobID: jobID, tokens: n, at: now})
	return true
}

func exceeds(used, requested, limit int64) bool {
	return requested > 0 && used > limit-requested || requested == 0 && used > limit
}

// SettleTokens 按实际用量结算指定 job 的预扣事件。
// actual < 预扣 → 事件值减小（回补）；actual > 预扣 → 事件值增大（补扣）。
// 事件已被 sweep（窗口滑出）则忽略——该消耗已不参与当前窗口判定。
func (g *TokenGate) SettleTokens(jobID string, actual int) {
	if actual < 0 {
		actual = 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.sweepLocked(now)
	setLocked := func(evs []tokenEvent) {
		for i := range evs {
			if evs[i].jobID == jobID {
				evs[i].tokens = actual
				return
			}
		}
	}
	setLocked(g.minute)
	setLocked(g.hour)
	setLocked(g.day)
}

// NextAvailable 返回最早可能重新获得额度的时刻。
// 哪个窗口满，就等它最老的事件滑出；都未满 → now。
func (g *TokenGate) NextAvailable() (time.Time, bool) {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tpm <= 0 && g.tph <= 0 && g.tpd <= 0 {
		return now, true
	}
	g.sweepLocked(now)
	var next time.Time
	found := false
	if g.tpm > 0 && g.windowTotalLocked(g.minute) >= g.tpm && len(g.minute) > 0 {
		next = g.minute[0].at.Add(g.minuteW)
		found = true
	}
	if g.tph > 0 && g.windowTotalLocked(g.hour) >= g.tph && len(g.hour) > 0 {
		t := g.hour[0].at.Add(g.hourW)
		if !found || t.Before(next) {
			next = t
			found = true
		}
	}
	if g.tpd > 0 && g.windowTotalLocked(g.day) >= g.tpd && len(g.day) > 0 {
		t := g.day[0].at.Add(g.dayW)
		if !found || t.Before(next) {
			next = t
			found = true
		}
	}
	if !found {
		return now, true
	}
	return next, true
}

// NextAvailableTokens predicts recovery for the actual reservation, including
// a partially used window (e.g. used=60, limit=100, next request=50).
func (g *TokenGate) NextAvailableTokens(n int) (time.Time, bool) {
	if n < 0 {
		n = 0
	}
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweepLocked(now)
	next := now
	for _, window := range []struct {
		events   []tokenEvent
		limit    int64
		duration time.Duration
	}{
		{g.minute, g.tpm, g.minuteW}, {g.hour, g.tph, g.hourW}, {g.day, g.tpd, g.dayW},
	} {
		if window.limit <= 0 {
			continue
		}
		if int64(n) > window.limit {
			return time.Time{}, false
		}
		used := g.windowTotalLocked(window.events)
		for _, event := range window.events {
			if !exceeds(used, int64(n), window.limit) {
				break
			}
			used -= int64(event.tokens)
			if at := event.at.Add(window.duration); at.After(next) {
				next = at
			}
		}
	}
	return next, true
}

// UpdateLimits 更新窗口上限并保留当前窗口中的已用量。
func (g *TokenGate) UpdateLimits(tpm, tph, tpd int64) {
	g.mu.Lock()
	g.tpm = tpm
	g.tph = tph
	g.tpd = tpd
	g.mu.Unlock()
}

// Used 当前分钟窗口内已占用 token 数（管理 API/测试断言用）。
func (g *TokenGate) Used() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweepLocked(time.Now())
	return g.windowTotalLocked(g.minute)
}

// UsedHour 当前小时窗口内已占用 token 数。
func (g *TokenGate) UsedHour() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweepLocked(time.Now())
	return g.windowTotalLocked(g.hour)
}

// UsedDay 当前 24h 窗口内已占用 token 数。
func (g *TokenGate) UsedDay() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sweepLocked(time.Now())
	return g.windowTotalLocked(g.day)
}

func (g *TokenGate) windowTotalLocked(evs []tokenEvent) int64 {
	const maxInt64 = int64(1<<63 - 1)
	var total int64
	for _, e := range evs {
		tokens := int64(e.tokens)
		if tokens > 0 && total > maxInt64-tokens {
			return maxInt64
		}
		total += tokens
	}
	return total
}

func (g *TokenGate) sweepLocked(now time.Time) {
	minCutoff := now.Add(-g.minuteW)
	hourCutoff := now.Add(-g.hourW)
	dayCutoff := now.Add(-g.dayW)
	g.minute = sweepEvents(g.minute, minCutoff)
	g.hour = sweepEvents(g.hour, hourCutoff)
	g.day = sweepEvents(g.day, dayCutoff)
}

func sweepEvents(evs []tokenEvent, cutoff time.Time) []tokenEvent {
	kept := evs[:0]
	for _, e := range evs {
		if e.at.After(cutoff) {
			kept = append(kept, e)
		}
	}
	return kept
}
