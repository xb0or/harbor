package server

import (
	"testing"
	"time"

	"harbor/internal/limits"
	"harbor/internal/provider"
)

func TestProviderGateRollsBackRPMWhenTokenAcquireFails(t *testing.T) {
	concurrency := limits.NewConcurrency("test:concurrency", 1)
	rpm := limits.NewRate("test:rpm", 1, time.Minute)
	token := limits.NewToken("test:token", 1, 0, 0)
	if tryAcquireProviderGates([]limits.Gate{concurrency, rpm, token}, "job", 2, 1) {
		t.Fatal("acquisition should fail when token quota is exceeded")
	}
	if got := rpm.Count(); got != 0 {
		t.Fatalf("RPM phantom reservation = %d, want 0", got)
	}
	if got := concurrency.Used(); got != 0 {
		t.Fatalf("concurrency reservation = %d, want 0", got)
	}
}

// TestProviderRouteGateWeightedModelCost 重型模型按 3 个槽占用，
// 轻量模型 0.25 在 3 槽上限下必须等重型释放后才能进入。
func TestProviderRouteGateWeightedModelCost(t *testing.T) {
	p := provider.New("up", "openai-compatible", "http://example.invalid/v1",
		[]string{"heavy", "light"}, []string{"sk-test"},
		time.Second, time.Second, 3, 1000, 0, 0, 0)
	p.SetModelSpecs([]provider.ModelSpec{
		{ID: "heavy", ConcurrencyCost: 3},
		{ID: "light", ConcurrencyCost: 0.25},
	})
	mgr := provider.NewManager([]*provider.Provider{p}, map[string][]provider.ModelRoute{
		"heavy": {{Provider: "up"}},
		"light": {{Provider: "up"}},
	})
	cfg := testConfig()
	srv := &Server{cfg: cfg, mgr: mgr, gateMap: map[string]*gateSet{}}

	concurrency := srv.providerGates(p)[0].(*limits.ConcurrencyGate)
	heavy := srv.newProviderLease("heavy")
	if heavy == nil || !heavy.TryAcquireTokens("job-heavy", 0) {
		t.Fatal("heavy lease should acquire 3 slots")
	}
	if got := concurrency.UsedCost(); got != 3 {
		t.Fatalf("heavy used = %v, want 3", got)
	}
	light := srv.newProviderLease("light")
	if light == nil {
		t.Fatal("light lease missing")
	}
	if light.TryAcquireTokens("job-light", 0) {
		t.Fatal("light lease must wait while heavy holds all 3 slots")
	}
	heavy.SettleTokens("job-heavy", 0)
	if got := concurrency.UsedCost(); got != 0 {
		t.Fatalf("after heavy release used = %v, want 0", got)
	}
	if !light.TryAcquireTokens("job-light", 0) {
		t.Fatal("light lease should acquire after heavy release")
	}
	if got := concurrency.UsedCost(); got != 0.25 {
		t.Fatalf("light used = %v, want 0.25", got)
	}
	light.SettleTokens("job-light", 0)
	if got := concurrency.UsedCost(); got != 0 {
		t.Fatalf("final used = %v, want 0", got)
	}
}

// TestProviderRouteGateCapsCostAtProviderConcurrency 系数大于 Provider 并发时，
// 按 Provider 上限占用，保证重型模型仍能单独运行而不是永远排队。
func TestProviderRouteGateCapsCostAtProviderConcurrency(t *testing.T) {
	p := provider.New("up", "openai-compatible", "http://example.invalid/v1",
		[]string{"heavy"}, []string{"sk-test"},
		time.Second, time.Second, 2, 1000, 0, 0, 0)
	p.SetModelSpecs([]provider.ModelSpec{{ID: "heavy", ConcurrencyCost: 3}})
	mgr := provider.NewManager([]*provider.Provider{p}, map[string][]provider.ModelRoute{
		"heavy": {{Provider: "up"}},
	})
	cfg := testConfig()
	srv := &Server{cfg: cfg, mgr: mgr, gateMap: map[string]*gateSet{}}

	lease := srv.newProviderLease("heavy")
	if lease == nil || !lease.TryAcquireTokens("job-heavy", 0) {
		t.Fatal("capped heavy lease should acquire")
	}
	concurrency := srv.providerGates(p)[0].(*limits.ConcurrencyGate)
	if got := concurrency.UsedCost(); got != 2 {
		t.Fatalf("used = %v, want provider cap 2", got)
	}
	lease.SettleTokens("job-heavy", 0)
	if got := concurrency.UsedCost(); got != 0 {
		t.Fatalf("used after release = %v, want 0", got)
	}
}
