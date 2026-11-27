package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"harbor/internal/limits"
)

// TestEnqueueSemantics 验证 Enqueue 返回 queued/position 语义。
func TestEnqueueSemantics(t *testing.T) {
	s := NewScheduler(100, 10*time.Second)
	s.Start()
	defer s.Stop()

	g := limits.NewConcurrency("test:conc", 1)
	var ran atomic.Int32
	mkJob := func(id string, block time.Duration) *Job {
		return &Job{
			ID:  id,
			Ctx: context.Background(),
			Do: func(ctx context.Context) *Result {
				ran.Add(1)
				time.Sleep(block)
				return &Result{Status: 200}
			},
		}
	}

	// job1：资源充足 → 立即执行，不排队
	q1, p1, err := s.Enqueue(mkJob("j1", 300*time.Millisecond), []limits.Gate{g})
	if err != nil {
		t.Fatal(err)
	}
	if q1 {
		t.Fatalf("j1 should not be queued, got queued")
	}
	_ = p1

	// job2：并发槽被占 → 排队
	time.Sleep(50 * time.Millisecond)
	q2, p2, err := s.Enqueue(mkJob("j2", 0), []limits.Gate{g})
	if err != nil {
		t.Fatal(err)
	}
	if !q2 {
		t.Fatalf("j2 should be queued, got not queued")
	}
	if p2 != 1 {
		t.Fatalf("j2 position = %d, want 1", p2)
	}

	// job3：继续排队，位置 2
	q3, p3, _ := s.Enqueue(mkJob("j3", 0), []limits.Gate{g})
	if !q3 || p3 != 2 {
		t.Fatalf("j3 queued=%v pos=%d, want queued=true pos=2", q3, p3)
	}

	// 等 job1 完成 → j2/j3 被调度执行
	deadline := time.Now().Add(3 * time.Second)
	for ran.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := ran.Load(); got != 3 {
		t.Fatalf("ran = %d, want 3 (auto-dispatch after release)", got)
	}
}

// TestQueueFIFOOrder FIFO 顺序：先入队先出队。
func TestQueueFIFOOrder(t *testing.T) {
	s := NewScheduler(100, 10*time.Second)
	s.Start()
	defer s.Stop()

	g := limits.NewConcurrency("test:conc", 1)
	order := make(chan string, 10)
	block := make(chan struct{})
	mkJob := func(id string) *Job {
		return &Job{
			ID:  id,
			Ctx: context.Background(),
			Do: func(ctx context.Context) *Result {
				if id == "first" {
					<-block // 占住槽直到手动放行
				}
				order <- id
				return &Result{Status: 200}
			},
		}
	}

	first := mkJob("first")
	if q, _, err := s.Enqueue(first, []limits.Gate{g}); err != nil || q {
		t.Fatalf("first enqueue err=%v queued=%v", err, q)
	}
	time.Sleep(30 * time.Millisecond)
	// 三个排队（FIFO: second < third < fourth）
	for _, id := range []string{"second", "third", "fourth"} {
		q, p, err := s.Enqueue(mkJob(id), []limits.Gate{g})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s queued=%v pos=%d", id, q, p)
		if !q {
			t.Fatalf("%s should be queued", id)
		}
	}
	close(block) // 释放 first

	got := []string{}
	deadline := time.Now().Add(2 * time.Second)
	for len(got) < 4 && time.Now().Before(deadline) {
		select {
		case id := <-order:
			got = append(got, id)
		case <-time.After(50 * time.Millisecond):
		}
	}
	want := []string{"first", "second", "third", "fourth"}
	if len(got) != 4 {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("FIFO order broken: got %v want %v", got, want)
		}
	}
}
