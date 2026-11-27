package queue

import (
	"testing"
	"time"
)

func TestJobExpiredSoftDeadline(t *testing.T) {
	now := time.Now()
	job := &Job{Deadline: now.Add(time.Minute), SoftDeadline: now.Add(-time.Second)}
	if !jobExpired(job, now) {
		t.Fatal("soft deadline should expire the job")
	}
	job.SoftDeadline = now.Add(time.Second)
	if jobExpired(job, now) {
		t.Fatal("job should still be queued before soft deadline")
	}
	job.SoftDeadline = time.Time{}
	if jobExpired(job, now) {
		t.Fatal("zero soft deadline must be ignored")
	}
	job.Deadline = now.Add(-time.Second)
	if !jobExpired(job, now) {
		t.Fatal("hard deadline must still expire the job")
	}
}

func TestWaitErrorEarlyThresholdIsRetryable429(t *testing.T) {
	now := time.Now()
	job := &Job{Deadline: now.Add(time.Minute), SoftDeadline: now.Add(-time.Second)}
	s := &Scheduler{}
	e := s.waitError(job)
	if e == nil {
		t.Fatal("nil error")
	}
	if e.Status != 429 {
		t.Fatalf("status = %d, want 429", e.Status)
	}
}
