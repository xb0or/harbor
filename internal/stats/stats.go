// Package stats 维护 Harbor 的自用统计（计划书 §12）：
// 今日请求数 / Token 用量 / 排队等待分位数。
// P0 内存实现，重启清零。
package stats

import (
	"sort"
	"sync"
	"time"
)

// Stats 是统计聚合器。
type Stats struct {
	mu        sync.Mutex
	startedAt time.Time

	requests   int64 // 今日总请求
	success    int64
	failed     int64
	tokensIn   int64
	tokensOut  int64
	tokensCach int64
	queued     int64 // 曾排队请求数
	waits      []time.Duration
	maxWait    time.Duration
}

// New 构造统计器。
func New() *Stats { return &Stats{startedAt: time.Now()} }

// RecordRequest 记录一次请求结束。
// queued 表示是否经历排队；wait 为排队时长；usage 为 token 用量（0 表示未知）。
func (s *Stats) RecordRequest(queued bool, wait time.Duration, ok bool, prompt, completion, total int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	if ok {
		s.success++
	} else {
		s.failed++
	}
	if queued {
		s.queued++
		s.waits = append(s.waits, wait)
		if wait > s.maxWait {
			s.maxWait = wait
		}
		if len(s.waits) > 2000 {
			s.waits = s.waits[len(s.waits)-2000:]
		}
	}
	s.tokensIn += int64(prompt)
	s.tokensOut += int64(completion)
	s.tokensCach += int64(total - prompt - completion) // 缓存 token 估算
}

// NoteQueued 简单累计排队次数（不入等待分位）。
func (s *Stats) NoteQueued() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queued++
}

// Snapshot 返回统计快照。
type Snapshot struct {
	StartTime    time.Time
	Requests     int64
	Success      int64
	Failed       int64
	TokensIn     int64
	TokensOut    int64
	TokensCached int64
	TokensTotal  int64
	Queued       int64
	P50WaitMs    int64
	P95WaitMs    int64
	MaxWaitMs    int64
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := Snapshot{
		StartTime:    s.startedAt,
		Requests:     s.requests,
		Success:      s.success,
		Failed:       s.failed,
		TokensIn:     s.tokensIn,
		TokensOut:    s.tokensOut,
		TokensCached: s.tokensCach,
		TokensTotal:  s.tokensIn + s.tokensOut + s.tokensCach,
		Queued:       s.queued,
		MaxWaitMs:    s.maxWait.Milliseconds(),
	}
	n := len(s.waits)
	if n > 0 {
		cp := make([]time.Duration, n)
		copy(cp, s.waits)
		sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
		snap.P50WaitMs = cp[n/2].Milliseconds()
		// P95 索引必须是 [0, n-1]：n=1 时 int(0.95)-1 会算成 -1（回归点）。
		idx := int(float64(n) * 0.95)
		if idx < 1 {
			idx = 1
		}
		if idx > n {
			idx = n
		}
		snap.P95WaitMs = cp[idx-1].Milliseconds()
	}
	return snap
}
