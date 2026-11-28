package server

import (
	"testing"

	"harbor/internal/provider"
)

// TestProviderModelViewsStates 模型状态只取决于「启用/映射」：
// 无映射启用 = visible，有映射 = mapped，禁用 = disabled；不再有 hidden。
func TestProviderModelViewsStates(t *testing.T) {
	p := provider.New("p", "openai-compatible", "https://api.example.com/v1",
		[]string{"direct", "mapped:dev", "off"}, nil, 0, 0, 0, 0, 0, 0, 0)
	off := false
	p.SetModelSpecs([]provider.ModelSpec{
		{ID: "direct"},
		{ID: "mapped:dev"},
		{ID: "off", Enabled: &off},
	})
	mgr := provider.NewManager([]*provider.Provider{p}, map[string][]provider.ModelRoute{
		"unified": {{Provider: "p", UpstreamModel: "mapped:dev"}},
	})
	srv := &Server{mgr: mgr}
	views, ok := srv.providerModelViews("p")
	if !ok {
		t.Fatal("provider not found")
	}
	got := map[string]string{}
	for _, v := range views {
		got[v.ID] = v.State
	}
	if got["direct"] != "visible" {
		t.Fatalf("direct state = %q, want visible", got["direct"])
	}
	if got["mapped:dev"] != "mapped" {
		t.Fatalf("mapped state = %q, want mapped", got["mapped:dev"])
	}
	if got["off"] != "disabled" {
		t.Fatalf("off state = %q, want disabled", got["off"])
	}
	for id, state := range got {
		if state == "hidden" {
			t.Fatalf("model %s must not be hidden", id)
		}
	}
}
