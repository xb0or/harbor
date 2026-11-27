package limits

import (
	"testing"
	"time"
)

func TestTokenWakeForPartialRemainingBudget(t *testing.T) {
	g := NewToken("partial", 100, 0, 0)
	if !g.TryAcquireTokens("used", 60) {
		t.Fatal("initial reserve")
	}
	if g.TryAcquireTokens("next", 50) {
		t.Fatal("should wait")
	}
	next, ok := g.NextAvailableTokens(50)
	if !ok || next.Before(time.Now().Add(50*time.Second)) {
		t.Fatalf("partial budget never schedules window wake: %v %v", next, ok)
	}
	if _, ok := g.NextAvailableTokens(101); ok {
		t.Fatal("request larger than capacity cannot recover via time")
	}
}

func TestConcurrencyEnableKeepsExistingRequests(t *testing.T) {
	g := NewConcurrency("dynamic", 0)
	g.TryAcquire()
	g.TryAcquire()
	g.UpdateLimit(1)
	if g.TryAcquire() {
		t.Fatal("enabling limit forgot active requests")
	}
	g.Release()
	if g.TryAcquire() {
		t.Fatal("one active request still owns the only slot")
	}
	g.Release()
	if !g.TryAcquire() {
		t.Fatal("slot not released")
	}
}
