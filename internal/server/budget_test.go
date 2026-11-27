package server

import (
	"context"
	"testing"
	"time"
)

func TestByteBudgetHonorsCapacityAndCancellation(t *testing.T) {
	budget := newByteBudget(2 << 20)
	units, ok := budget.Acquire(context.Background(), 2<<20)
	if !ok || units != 2 {
		t.Fatalf("first acquire units=%d ok=%v", units, ok)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if units, ok := budget.Acquire(ctx, 1); ok || units != 0 {
		t.Fatalf("blocked acquire units=%d ok=%v, want cancellation", units, ok)
	}

	budget.Release(2)
	if units, ok := budget.Acquire(context.Background(), 1); !ok || units != 1 {
		t.Fatalf("re-acquire units=%d ok=%v", units, ok)
	} else {
		budget.Release(units)
	}
}
