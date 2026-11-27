package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"harbor/internal/limits"
)

type immediateNextGate struct {
	nextCalls atomic.Int32
}

func (g *immediateNextGate) Name() string     { return "test:immediate" }
func (g *immediateNextGate) TryAcquire() bool { return true }
func (g *immediateNextGate) Release()         {}
func (g *immediateNextGate) NextAvailable() (time.Time, bool) {
	g.nextCalls.Add(1)
	return time.Now(), true
}

func TestSchedulerDoesNotBusyLoopOnUnavailableGate(t *testing.T) {
	s := NewScheduler(10, time.Second)
	s.Start()
	defer s.Stop()

	blocker := limits.NewConcurrency("test:blocker", 1)
	release := make(chan struct{})
	first := &Job{
		ID:  "first",
		Ctx: context.Background(),
		Do: func(context.Context) *Result {
			<-release
			return &Result{Status: 200}
		},
	}
	if queued, _, err := s.Enqueue(first, []limits.Gate{blocker}); err != nil || queued {
		t.Fatalf("first enqueue: queued=%v err=%v", queued, err)
	}

	probe := &immediateNextGate{}
	second := &Job{
		ID:  "second",
		Ctx: context.Background(),
		Do:  func(context.Context) *Result { return &Result{Status: 200} },
	}
	if queued, _, err := s.Enqueue(second, []limits.Gate{blocker, probe}); err != nil || !queued {
		t.Fatalf("second enqueue: queued=%v err=%v", queued, err)
	}

	time.Sleep(50 * time.Millisecond)
	if calls := probe.nextCalls.Load(); calls > 20 {
		t.Fatalf("scheduler appears to busy-loop: NextAvailable called %d times", calls)
	}

	close(release)
	select {
	case res := <-second.ResultCh():
		if res == nil || res.Err != nil || res.Status != 200 {
			t.Fatalf("second result = %+v", res)
		}
	case <-time.After(time.Second):
		t.Fatal("second job was not dispatched after gate release")
	}
	<-first.ResultCh()
}

func TestQueuedJobHonorsContextDeadline(t *testing.T) {
	s := NewScheduler(10, 10*time.Second)
	s.Start()
	defer s.Stop()

	blocker := limits.NewConcurrency("test:blocker", 1)
	release := make(chan struct{})
	first := &Job{
		ID:  "first",
		Ctx: context.Background(),
		Do: func(context.Context) *Result {
			<-release
			return &Result{Status: 200}
		},
	}
	if queued, _, err := s.Enqueue(first, []limits.Gate{blocker}); err != nil || queued {
		t.Fatalf("first enqueue: queued=%v err=%v", queued, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var ran atomic.Int32
	second := &Job{
		ID:  "second",
		Ctx: ctx,
		Do: func(context.Context) *Result {
			ran.Add(1)
			return &Result{Status: 200}
		},
	}
	if queued, _, err := s.Enqueue(second, []limits.Gate{blocker}); err != nil || !queued {
		t.Fatalf("second enqueue: queued=%v err=%v", queued, err)
	}

	started := time.Now()
	select {
	case res := <-second.ResultCh():
		if res == nil || res.Err == nil || res.Err.Code != "timeout" {
			t.Fatalf("second result = %+v, want timeout", res)
		}
		if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
			t.Fatalf("deadline result took %v", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("queued job did not honor context deadline")
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("expired queued job ran %d times", got)
	}

	close(release)
	<-first.ResultCh()
}

func TestUnknownTokenUsageKeepsReservation(t *testing.T) {
	s := NewScheduler(10, time.Second)
	s.Start()
	defer s.Stop()

	gate := limits.NewToken("test:tokens", 1000, 0, 0)
	unknown := &Job{
		ID:           "unknown",
		Ctx:          context.Background(),
		TokenReserve: 40,
		Do:           func(context.Context) *Result { return &Result{Status: 200} },
	}
	if _, _, err := s.Enqueue(unknown, []limits.Gate{gate}); err != nil {
		t.Fatal(err)
	}
	if res := <-unknown.ResultCh(); res == nil || res.Err != nil {
		t.Fatalf("unknown usage result = %+v", res)
	}
	if got := gate.Used(); got != 40 {
		t.Fatalf("unknown usage settled to %d tokens, want reserved 40", got)
	}

	known := &Job{
		ID:           "known",
		Ctx:          context.Background(),
		TokenReserve: 40,
		Do: func(context.Context) *Result {
			return &Result{Status: 200, Usage: Usage{Total: 7}, UsageKnown: true}
		},
	}
	if _, _, err := s.Enqueue(known, []limits.Gate{gate}); err != nil {
		t.Fatal(err)
	}
	if res := <-known.ResultCh(); res == nil || res.Err != nil {
		t.Fatalf("known usage result = %+v", res)
	}
	if got := gate.Used(); got != 47 {
		t.Fatalf("known usage settled to %d tokens, want 47", got)
	}
}
