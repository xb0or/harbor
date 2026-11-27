package queue

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"harbor/internal/limits"
	"harbor/internal/normalize"
)

func awaitQueueResult(t *testing.T, job *Job) *Result {
	t.Helper()
	select {
	case res := <-job.ResultCh():
		return res
	case <-time.After(2 * time.Second):
		t.Fatal("result not delivered")
		return nil
	}
}

func waitQueueState(t *testing.T, s *Scheduler, depth, running int) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		st := s.Stats()
		if st.Depth == depth && st.Running == running {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state=%+v, want depth=%d running=%d", s.Stats(), depth, running)
}

func TestRequeueCancellationReleasesBodyOnlyAtEnd(t *testing.T) {
	s := NewScheduler(10, time.Second)
	s.Start()
	defer s.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts, freed atomic.Int32
	gate := limits.NewConcurrency("c", 1)
	job := &Job{ID: "retry", Ctx: ctx, Cancel: cancel, ReleaseResources: func() { freed.Add(1) }, Do: func(context.Context) *Result {
		attempts.Add(1)
		return &Result{Attempt: 1, Err: normalize.ErrRateLimited, RequeueAt: time.Now().Add(500 * time.Millisecond)}
	}}
	if _, _, err := s.Enqueue(job, []limits.Gate{gate}); err != nil {
		t.Fatal(err)
	}
	waitQueueState(t, s, 1, 0)
	if freed.Load() != 0 || gate.Used() != 0 {
		t.Fatalf("retained resources wrong: body=%d gate=%d", freed.Load(), gate.Used())
	}
	cancel()
	res := awaitQueueResult(t, job)
	waitQueueState(t, s, 0, 0)
	if res.Err == nil || res.Attempt != 1 || freed.Load() != 1 || attempts.Load() != 1 {
		t.Fatalf("res=%+v freed=%d attempts=%d", res, freed.Load(), attempts.Load())
	}
}

func TestRequeueDeadlineAndStatsAreNotReset(t *testing.T) {
	s := NewScheduler(10, 85*time.Millisecond)
	s.Start()
	defer s.Stop()
	var calls, freed atomic.Int32
	job := &Job{ID: "deadline", Ctx: context.Background(), ReleaseResources: func() { freed.Add(1) }, Do: func(context.Context) *Result {
		calls.Add(1)
		time.Sleep(12 * time.Millisecond)
		return &Result{Attempt: 1, Err: normalize.ErrRateLimited, RequeueAt: time.Now().Add(25 * time.Millisecond)}
	}}
	start := time.Now()
	if _, _, err := s.Enqueue(job, nil); err != nil {
		t.Fatal(err)
	}
	res := awaitQueueResult(t, job)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("deadline reset: %v", elapsed)
	}
	if res.Err == nil || res.Attempt != int(calls.Load()) || calls.Load() < 2 || freed.Load() != 1 {
		t.Fatalf("res=%+v calls=%d freed=%d", res, calls.Load(), freed.Load())
	}
	if res.QueueWait() >= time.Since(start)-time.Duration(calls.Load())*8*time.Millisecond {
		t.Fatalf("execution included in wait=%v", res.QueueWait())
	}
}

func TestRequeueFullDoesNotDoubleReleaseGate(t *testing.T) {
	s := NewScheduler(1, time.Second)
	// Enqueue dispatches synchronously; defer the background loop so the
	// full queue cannot drain between lease release and retry admission.
	defer s.Stop()
	gate := limits.NewConcurrency("c", 1)
	fail := make(chan struct{})
	first := &Job{ID: "first", Ctx: context.Background(), Do: func(context.Context) *Result {
		<-fail
		return &Result{Attempt: 1, Err: normalize.ErrRateLimited, RequeueAt: time.Now().Add(time.Second)}
	}}
	if _, _, err := s.Enqueue(first, []limits.Gate{gate}); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	second := &Job{ID: "second", Ctx: context.Background(), Do: func(context.Context) *Result { <-release; return &Result{Status: 200} }}
	if _, _, err := s.Enqueue(second, []limits.Gate{gate}); err != nil {
		t.Fatal(err)
	}
	close(fail)
	res := awaitQueueResult(t, first)
	s.Start()
	waitQueueState(t, s, 0, 1)
	used := gate.Used()
	close(release)
	awaitQueueResult(t, second)
	if res.Err != ErrQueueFull || used != 1 || gate.Used() != 0 {
		t.Fatalf("first=%+v active=%d final=%d", res, used, gate.Used())
	}
}

func TestRequeueStopTerminatesWaiter(t *testing.T) {
	s := NewScheduler(10, time.Hour)
	s.Start()
	defer s.Stop()
	var freed atomic.Int32
	job := &Job{ID: "stop", Ctx: context.Background(), ReleaseResources: func() { freed.Add(1) }, Do: func(context.Context) *Result {
		return &Result{Attempt: 1, Err: normalize.ErrRateLimited, RequeueAt: time.Now().Add(time.Hour)}
	}}
	if _, _, err := s.Enqueue(job, nil); err != nil {
		t.Fatal(err)
	}
	waitQueueState(t, s, 1, 0)
	s.Stop()
	res := awaitQueueResult(t, job)
	if res.Err == nil || res.Attempt != 1 || freed.Load() != 1 {
		t.Fatalf("stop lost retry state: %+v freed=%d", res, freed.Load())
	}
}

func TestRequeueTailKeepsEarlierQueueDeadline(t *testing.T) {
	s := NewScheduler(10, 120*time.Millisecond)
	s.Start()
	defer s.Stop()
	trigger := make(chan struct{})
	first := &Job{ID: "earlier", Ctx: context.Background(), Do: func(context.Context) *Result {
		<-trigger
		return &Result{Attempt: 1, Err: normalize.ErrRateLimited, RequeueAt: time.Now().Add(time.Second)}
	}}
	if _, _, err := s.Enqueue(first, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	block := limits.NewConcurrency("blocked", 1)
	block.TryAcquire()
	second := &Job{ID: "later", Ctx: context.Background(), Do: func(context.Context) *Result { return &Result{Status: 200} }}
	if _, _, err := s.Enqueue(second, []limits.Gate{block}); err != nil {
		t.Fatal(err)
	}
	close(trigger)
	res := awaitQueueResult(t, first)
	if time.Since(first.EnqueuedAt) > 165*time.Millisecond || res.Attempt != 1 {
		t.Fatalf("tail deadline blocked by later head: elapsed=%v res=%+v", time.Since(first.EnqueuedAt), res)
	}
}

func TestRequeueNeverReplaysStartedResponse(t *testing.T) {
	for _, stream := range []bool{false, true} {
		s := NewScheduler(10, time.Second)
		s.Start()
		var calls atomic.Int32
		job := &Job{ID: "started", Ctx: context.Background(), Do: func(context.Context) *Result {
			calls.Add(1)
			r := &Result{Attempt: 1, Err: normalize.ErrUpstream, RequeueAt: time.Now()}
			if stream {
				r.Stream = io.NopCloser(strings.NewReader("data: partial\n\n"))
			} else {
				r.Body = []byte("partial")
			}
			return r
		}}
		if _, _, err := s.Enqueue(job, nil); err != nil {
			t.Fatal(err)
		}
		res := awaitQueueResult(t, job)
		if res.Stream != nil {
			res.Stream.Close()
			s.ReleaseJob(job)
		}
		s.Stop()
		if calls.Load() != 1 {
			t.Fatalf("started response replayed %d times", calls.Load())
		}
	}
}
