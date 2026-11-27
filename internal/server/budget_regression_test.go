package server

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestByteBudgetAtomicMultiUnitAcquire(t *testing.T) {
	budget := newByteBudget(2 * bodyBudgetQuantum)
	start := make(chan struct{})
	results := make(chan bool, 2)
	for range 2 {
		go func() {
			<-start
			units, ok := budget.Acquire(context.Background(), 2*bodyBudgetQuantum)
			if ok {
				budget.Release(units)
			}
			results <- ok
		}()
	}
	close(start)
	for range 2 {
		select {
		case ok := <-results:
			if !ok {
				t.Fatal("multi-unit acquire failed")
			}
		case <-time.After(time.Second):
			t.Fatal("multi-unit acquire deadlocked")
		}
	}
}

func TestByteBudgetWaiterCancellationDoesNotConsumeCapacity(t *testing.T) {
	budget := newByteBudget(2 * bodyBudgetQuantum)
	units, ok := budget.Acquire(context.Background(), 2*bodyBudgetQuantum)
	if !ok {
		t.Fatal("initial acquire failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got, ok := budget.Acquire(ctx, bodyBudgetQuantum); ok || got != 0 {
		t.Fatalf("cancelled acquire got units=%d ok=%v", got, ok)
	}
	budget.Release(units)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		got, ok := budget.Acquire(context.Background(), 2*bodyBudgetQuantum)
		if !ok {
			t.Error("acquire after cancellation failed")
			return
		}
		budget.Release(got)
	}()
	wg.Wait()
}
