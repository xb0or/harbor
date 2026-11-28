package provider

import "testing"

func boolPtr(v bool) *bool { return &v }

// PublicModels 只暴露统一名与「启用」的直通模型。
func TestPublicModelsHonorsMappings(t *testing.T) {
	mgr := NewManager(nil, nil)
	p := New("electr", "openai-compatible", "https://api.example.com/v1",
		[]string{"ds:dev", "glm:dev", "off-model", "direct", "unmapped"},
		nil, 0, 0, 0, 0, 0, 0, 0)
	p.SetModelSpecs([]ModelSpec{
		{ID: "ds:dev", Context: 1000},
		{ID: "glm:dev"},
		{ID: "off-model", Enabled: boolPtr(false)},
		{ID: "direct"},
		{ID: "unmapped"},
	})
	mgr.AddProvider(p)
	if !mgr.SetRoute("ds", []ModelRoute{{Provider: "electr", UpstreamModel: "ds:dev"}}) {
		t.Fatal("SetRoute failed")
	}
	if !mgr.SetRoute("glm", []ModelRoute{{Provider: "electr", UpstreamModel: "glm:dev"}}) {
		t.Fatal("SetRoute failed")
	}

	got := mgr.PublicModels()
	want := map[string]bool{"ds": true, "glm": true, "direct": true, "unmapped": true}
	if len(got) != len(want) {
		t.Fatalf("PublicModels() = %v, want %v", got, want)
	}
	for _, m := range got {
		if !want[m] {
			t.Fatalf("unexpected public model %q (all=%v)", m, got)
		}
	}
}

// 禁用模型不再参与自动路由；启用模型按名直连。
func TestDisabledModelStopsRouting(t *testing.T) {
	mgr := NewManager(nil, nil)
	p := New("electr", "openai-compatible", "https://api.example.com/v1",
		[]string{"a", "b"}, nil, 0, 0, 0, 0, 0, 0, 0)
	mgr.AddProvider(p)
	if len(mgr.ModelProviders("a")) != 1 {
		t.Fatal("model a should auto-route before disable")
	}
	p.SetModelSpecs([]ModelSpec{{ID: "a", Enabled: boolPtr(false)}, {ID: "b"}})
	if len(mgr.ModelProviders("a")) != 0 {
		t.Fatal("disabled model must not route")
	}
	if len(mgr.ModelProviders("b")) != 1 {
		t.Fatal("enabled model must still route by name")
	}
}

// 显式映射同样尊重「禁用」：禁用上游后统一名不再可调用。
func TestDisabledMappedTargetStopsRouting(t *testing.T) {
	mgr := NewManager(nil, nil)
	p := New("electr", "openai-compatible", "https://api.example.com/v1", []string{"ds:dev"}, nil, 0, 0, 0, 0, 0, 0, 0)
	mgr.AddProvider(p)
	if !mgr.SetRoute("ds", []ModelRoute{{Provider: "electr", UpstreamModel: "ds:dev"}}) {
		t.Fatal("SetRoute failed")
	}
	if len(mgr.ModelProviders("ds")) != 1 {
		t.Fatal("mapped model should route before being disabled")
	}
	p.SetModelSpecs([]ModelSpec{{ID: "ds:dev", Enabled: boolPtr(false)}})
	if len(mgr.ModelProviders("ds")) != 0 {
		t.Fatal("disabled upstream must stop routing even when mapped")
	}
}

// UpdateModels 保留已有模型的元数据（禁用仍禁用，上下文/并发系数不丢）。
func TestUpdateModelsKeepsSpecMetadata(t *testing.T) {
	p := New("p", "openai-compatible", "https://api.example.com/v1", []string{"a"}, nil, 0, 0, 0, 0, 0, 0, 0)
	p.SetModelSpecs([]ModelSpec{{ID: "a", Context: 128000, Capabilities: []string{"chat", "vision"}, ConcurrencyCost: 0.5}})
	p.UpdateModels([]string{"a", "b"})
	specs := p.ModelSpecs()
	if len(specs) != 2 {
		t.Fatalf("specs = %+v", specs)
	}
	if specs[0].ID != "a" || specs[0].Context != 128000 || specs[0].ConcurrencyCost != 0.5 {
		t.Fatalf("metadata lost on update: %+v", specs[0])
	}
	if specs[1].ID != "b" || !specs[1].IsEnabled() || len(specs[1].Capabilities) != 1 {
		t.Fatalf("new model defaults wrong: %+v", specs[1])
	}
}

// ModelConcurrencyCost 按「统一模型 → Provider → 上游模型」读取模型声明里的系数。
func TestModelConcurrencyCostLookup(t *testing.T) {
	p := New("p", "openai-compatible", "https://api.example.com/v1", []string{"ds:dev"}, nil, 0, 0, 0, 0, 0, 0, 0)
	p.SetModelSpecs([]ModelSpec{{ID: "ds:dev", ConcurrencyCost: 2.5}})
	mgr := NewManager([]*Provider{p}, map[string][]ModelRoute{
		"ds": {{Provider: "p", UpstreamModel: "ds:dev"}},
	})
	if got, ok := mgr.UpstreamModelFor("ds", "p"); !ok || got != "ds:dev" {
		t.Fatalf("upstream = %q ok=%v, want ds:dev", got, ok)
	}
	if got := mgr.ModelConcurrencyCost("ds", "p"); got != 2.5 {
		t.Fatalf("cost = %v, want 2.5", got)
	}
	if got := mgr.ModelConcurrencyCost("unknown", "p"); got != 0 {
		t.Fatalf("unknown model cost = %v, want 0", got)
	}
}
