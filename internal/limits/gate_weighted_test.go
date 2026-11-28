package limits

import (
	"math"
	"testing"
)

// TestConcurrencyGateFractionalCosts 0.25 的轻量模型 4 个才占满 1 个槽。
func TestConcurrencyGateFractionalCosts(t *testing.T) {
	g := NewConcurrency("fractional", 1)
	for i := 0; i < 4; i++ {
		if !g.TryAcquireCost(0.25) {
			t.Fatalf("acquire 0.25 #%d failed", i+1)
		}
	}
	if g.TryAcquireCost(0.25) {
		t.Fatal("fifth 0.25 request must not fit")
	}
	if got := g.UsedCost(); math.Abs(got-1) > 1e-9 {
		t.Fatalf("used = %v, want 1", got)
	}
	g.ReleaseCost(0.25)
	if !g.TryAcquireCost(0.25) {
		t.Fatal("released slot should be reusable")
	}
	if got := g.UsedCost(); math.Abs(got-1) > 1e-9 {
		t.Fatalf("used after reuse = %v, want 1", got)
	}
}

// TestConcurrencyGateHeavyCost 重型模型 3 个槽，剩余 1 个槽只够轻量模型。
func TestConcurrencyGateHeavyCost(t *testing.T) {
	g := NewConcurrency("heavy", 4)
	if !g.TryAcquireCost(3) {
		t.Fatal("heavy 3 should fit in 4")
	}
	if !g.TryAcquireCost(1) {
		t.Fatal("remaining 1 should fit")
	}
	if g.TryAcquireCost(0.01) {
		t.Fatal("full gate must reject any positive cost")
	}
	g.ReleaseCost(3)
	if !g.TryAcquireCost(2.5) {
		t.Fatal("2.5 should fit after releasing 3")
	}
}

// TestConcurrencyGateCostAboveLimit 单个系数大于上限时，Gate 本身拒绝；
// 路由层会把它收敛到 Provider 并发上限（见 server 测试）。
func TestConcurrencyGateCostAboveLimit(t *testing.T) {
	g := NewConcurrency("cap", 2)
	if g.TryAcquireCost(3) {
		t.Fatal("cost 3 must not fit a limit of 2")
	}
	if got := g.UsedCost(); got != 0 {
		t.Fatalf("failed acquire changed used = %v", got)
	}
}

// TestConcurrencyGateUnlimitedTracksFraction 不限并发时仍记录小数占用，
// 便于之后开启上限时保留在途请求。
func TestConcurrencyGateUnlimitedTracksFraction(t *testing.T) {
	g := NewConcurrency("unlimited", 0)
	if !g.TryAcquireCost(0.25) || !g.TryAcquireCost(1.2) {
		t.Fatal("unlimited gate should always acquire")
	}
	if got := g.UsedCost(); math.Abs(got-1.45) > 1e-9 {
		t.Fatalf("used = %v, want 1.45", got)
	}
	if _, ok := g.NextAvailableCost(100); !ok {
		t.Fatal("unlimited gate should always be available")
	}
}
