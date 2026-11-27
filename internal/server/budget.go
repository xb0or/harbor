package server

import (
	"context"
	"sync"
)

const bodyBudgetQuantum int64 = 1 << 20

type byteBudget struct {
	mu        sync.Mutex
	available int
	notify    chan struct{}
}

func newByteBudget(capacity int64) *byteBudget {
	if capacity < bodyBudgetQuantum {
		capacity = bodyBudgetQuantum
	}
	units := capacity / bodyBudgetQuantum
	if capacity%bodyBudgetQuantum != 0 {
		units++
	}
	return &byteBudget{available: int(units), notify: make(chan struct{})}
}

func (b *byteBudget) Acquire(ctx context.Context, bytes int64) (int, bool) {
	if bytes <= 0 {
		return 0, true
	}
	units := bytes / bodyBudgetQuantum
	if bytes%bodyBudgetQuantum != 0 {
		units++
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		b.mu.Lock()
		if int(units) <= b.available {
			b.available -= int(units)
			b.mu.Unlock()
			return int(units), true
		}
		wait := b.notify
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, false
		case <-wait:
		}
	}
}

func (b *byteBudget) Release(units int) {
	if units <= 0 {
		return
	}
	b.mu.Lock()
	b.available += units
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
}
