package server

import (
	"testing"
	"time"

	"harbor/internal/limits"
)

func TestProviderGateRollsBackRPMWhenTokenAcquireFails(t *testing.T) {
	concurrency := limits.NewConcurrency("test:concurrency", 1)
	rpm := limits.NewRate("test:rpm", 1, time.Minute)
	token := limits.NewToken("test:token", 1, 0, 0)
	if tryAcquireProviderGates([]limits.Gate{concurrency, rpm, token}, "job", 2) {
		t.Fatal("acquisition should fail when token quota is exceeded")
	}
	if got := rpm.Count(); got != 0 {
		t.Fatalf("RPM phantom reservation = %d, want 0", got)
	}
	if got := concurrency.Used(); got != 0 {
		t.Fatalf("concurrency reservation = %d, want 0", got)
	}
}
