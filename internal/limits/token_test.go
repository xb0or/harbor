package limits

import (
	"testing"
	"time"
)

// TestTokenGateTPMReserveAndSettle：TPM 窗口内预扣、超限拒绝、结算回补。
func TestTokenGateTPMReserveAndSettle(t *testing.T) {
	g := NewToken("test:tpm", 100, 0, 0) // TPM=100，TPD 不限

	if !g.TryAcquireTokens("job1", 60) {
		t.Fatal("job1 60/100 should pass")
	}
	if !g.TryAcquireTokens("job2", 40) {
		t.Fatal("job2 40/100 should pass (total 100)")
	}
	if g.TryAcquireTokens("job3", 1) {
		t.Fatal("job3 1/100 should be rejected (window full)")
	}
	if got := g.Used(); got != 100 {
		t.Fatalf("used=%d want 100", got)
	}

	// 结算回补：job1 实际只用 10 → 窗口空出 50
	g.SettleTokens("job1", 10)
	if got := g.Used(); got != 50 {
		t.Fatalf("used after settle=%d want 50", got)
	}
	if !g.TryAcquireTokens("job3", 50) {
		t.Fatal("job3 50/100 should pass after refund")
	}
	if g.TryAcquireTokens("job4", 1) {
		t.Fatal("job4 should be rejected (100 again)")
	}

	// 补扣：job2 实际 60 > 预扣 40 → 窗口 110，job3 被挤出但下一次仍拒绝
	g.SettleTokens("job2", 60)
	if g.TryAcquireTokens("job4", 1) {
		t.Fatal("job4 should be rejected after extra charge")
	}
}

// TestTokenGateSweep：窗口滑出后额度恢复。
func TestTokenGateSweep(t *testing.T) {
	g := NewToken("test:sweep", 50, 0, 0)
	// 直接把事件时间改到 61 秒前（模拟窗口滑出）
	if !g.TryAcquireTokens("old", 50) {
		t.Fatal("reserve should pass")
	}
	g.mu.Lock()
	for i := range g.minute {
		g.minute[i].at = time.Now().Add(-61 * time.Second)
	}
	g.mu.Unlock()

	if !g.TryAcquireTokens("new", 50) {
		t.Fatal("should pass after window slid out")
	}
}

// TestTokenGateTPD：24h 窗口独立限制。
func TestTokenGateTPD(t *testing.T) {
	g := NewToken("test:tpd", 0, 0, 1000) // TPM 不限，TPD=1000
	for i := 0; i < 10; i++ {
		if !g.TryAcquireTokens(string(rune('a'+i)), 100) {
			t.Fatalf("job %d should pass (day total=%d)", i, g.UsedDay())
		}
	}
	if g.TryAcquireTokens("job11", 1) {
		t.Fatal("job11 should be rejected (TPD reached)")
	}
	// 回补一半 → 又放行
	g.SettleTokens("a", 0)
	if !g.TryAcquireTokens("job11", 100) {
		t.Fatal("job11 should pass after refund")
	}
}

// TestTokenGateNextAvailable：TPM 满时返回最早滑出时刻。
func TestTokenGateNextAvailable(t *testing.T) {
	g := NewToken("test:next", 100, 0, 0)
	if !g.TryAcquireTokens("a", 60) {
		t.Fatal("reserve should pass")
	}
	next, ok := g.NextAvailable()
	if !ok {
		t.Fatal("NextAvailable should predict")
	}
	if next.Before(time.Now()) {
		t.Fatal("next should be in the future")
	}
	// 未满时立即可用
	g2 := NewToken("test:next2", 100, 0, 0)
	now, ok := g2.NextAvailable()
	if !ok || now.After(time.Now().Add(time.Second)) {
		t.Fatalf("unlimited window should be immediately available, got %v", now)
	}
}

// TestTokenGateUnlimited：0 = 不限。
func TestTokenGateUnlimited(t *testing.T) {
	g := NewToken("test:unlimited", 0, 0, 0)
	for i := 0; i < 1000; i++ {
		if !g.TryAcquireTokens("many", 999999) {
			t.Fatalf("unlimited should always pass (i=%d)", i)
		}
	}
}

// TestTokenGateRollback：tryAcquireLocked 回滚路径（Settle 0）恢复额度。
func TestTokenGateRollback(t *testing.T) {
	g := NewToken("test:rb", 100, 0, 0)
	if !g.TryAcquireTokens("job", 80) {
		t.Fatal("should pass")
	}
	// 模拟 scheduler 回滚：预扣 100 失败 → 前面 job 的预扣结算为 0
	g.SettleTokens("job", 0)
	if got := g.Used(); got != 0 {
		t.Fatalf("used after rollback=%d want 0", got)
	}
}

// TestTokenGateSweepKeepsDay：结算不因 sweep 丢事件（同窗口内 jobID 定位）。
func TestTokenGateSettleUnknownJob(t *testing.T) {
	g := NewToken("test:unknown", 100, 0, 0)
	if !g.TryAcquireTokens("a", 50) {
		t.Fatal("reserve a")
	}
	g.SettleTokens("does-not-exist", 0) // 无操作，不 panic
	if got := g.Used(); got != 50 {
		t.Fatalf("used=%d want 50 (unknown settle ignored)", got)
	}
}
