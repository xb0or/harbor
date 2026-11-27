package stats

import (
	"testing"
	"time"
)

// TestSnapshotEmpty 空统计不得 panic，分位数为 0。
func TestSnapshotEmpty(t *testing.T) {
	s := New()
	snap := s.Snapshot()
	if snap.Requests != 0 || snap.P50WaitMs != 0 || snap.P95WaitMs != 0 {
		t.Fatalf("unexpected empty snapshot: %+v", snap)
	}
}

// TestSnapshotSingle 回归：单条排队记录时 P95 索引曾算出 -1 导致 panic。
func TestSnapshotSingle(t *testing.T) {
	s := New()
	s.RecordRequest(true, 120*time.Millisecond, true, 12, 6, 18)
	snap := s.Snapshot() // 此前在这里 panic: index out of range [-1]
	if snap.Queued != 1 || snap.P50WaitMs != 120 || snap.P95WaitMs != 120 {
		t.Fatalf("unexpected single snapshot: %+v", snap)
	}
	if snap.MaxWaitMs != 120 {
		t.Fatalf("max wait: %d", snap.MaxWaitMs)
	}
}

// TestSnapshotPercentiles 多数据时 P50/P95 单调不越界。
func TestSnapshotPercentiles(t *testing.T) {
	s := New()
	for i := 1; i <= 100; i++ {
		s.RecordRequest(true, time.Duration(i)*time.Millisecond, true, 0, 0, 0)
	}
	// 非排队请求只记 counts 不进 waits
	s.RecordRequest(false, 0, false, 0, 0, 0)
	snap := s.Snapshot()
	if snap.Requests != 101 || snap.Success != 100 || snap.Failed != 1 {
		t.Fatalf("counts wrong: %+v", snap)
	}
	if snap.P50WaitMs < 45 || snap.P50WaitMs > 55 {
		t.Fatalf("p50 out of range: %d", snap.P50WaitMs)
	}
	if snap.P95WaitMs < 90 || snap.P95WaitMs > 100 {
		t.Fatalf("p95 out of range: %d", snap.P95WaitMs)
	}
	if snap.MaxWaitMs != 100 {
		t.Fatalf("max wait: %d", snap.MaxWaitMs)
	}
}

// TestWaitsRingBuffer 超过 2000 条后只保留最近 2000 条。
func TestWaitsRingBuffer(t *testing.T) {
	s := New()
	for i := 0; i < 2100; i++ {
		s.RecordRequest(true, time.Duration(i)*time.Millisecond, true, 0, 0, 0)
	}
	snap := s.Snapshot()
	if snap.Queued != 2100 {
		t.Fatalf("queued count: %d", snap.Queued)
	}
	// 环形缓冲保留最近 2000 条（i=100..2099），中位数约 1100ms
	if snap.P50WaitMs < 1000 || snap.P50WaitMs > 1200 {
		t.Fatalf("ring buffer p50 wrong: %d", snap.P50WaitMs)
	}
	// 最大值仍在（2000ms 以内条目的 maxWait 是 2099ms）
	if snap.MaxWaitMs != 2099 {
		t.Fatalf("ring buffer max wait: %d", snap.MaxWaitMs)
	}
}
