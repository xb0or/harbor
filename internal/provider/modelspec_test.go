package provider

import "testing"

func boolPtr(v bool) *bool { return &v }

// PublicModels 只暴露统一名与「启用且可见」的直通模型。
func TestPublicModelsHonorsVisibilityAndMappings(t *testing.T) {
	mgr := NewManager(nil, nil)
	p := New("electr", "openai-compatible", "https://api.example.com/v1",
		[]string{"ds:dev", "glm:dev", "hidden-only", "off-model", "direct"},
		nil, 0, 0, 0, 0, 0, 0, 0)
	p.SetModelSpecs([]ModelSpec{
		{ID: "ds:dev", Context: 1000},
		{ID: "glm:dev"},
		{ID: "hidden-only", Visible: boolPtr(false)},
		{ID: "off-model", Enabled: boolPtr(false)},
		{ID: "direct"},
	})
	mgr.AddProvider(p)
	if !mgr.SetRoute("ds", []ModelRoute{{Provider: "electr", UpstreamModel: "ds:dev"}}) {
		t.Fatal("SetRoute failed")
	}
	if !mgr.SetRoute("glm", []ModelRoute{{Provider: "electr", UpstreamModel: "glm:dev"}}) {
		t.Fatal("SetRoute failed")
	}

	got := mgr.PublicModels()
	want := map[string]bool{"ds": true, "glm": true, "direct": true}
	if len(got) != len(want) {
		t.Fatalf("PublicModels() = %v, want %v", got, want)
	}
	for _, m := range got {
		if !want[m] {
			t.Fatalf("unexpected public model %q (all=%v)", m, got)
		}
	}
}

// 禁用模型不再参与自动路由；不展示模型仍可按名直调（只是不出现在列表）。
func TestDisabledModelStopsRouting(t *testing.T) {
	mgr := NewManager(nil, nil)
	p := New("electr", "openai-compatible", "https://api.example.com/v1",
		[]string{"a", "b"}, nil, 0, 0, 0, 0, 0, 0, 0)
	mgr.AddProvider(p)
	if len(mgr.ModelProviders("a")) != 1 {
		t.Fatal("model a should auto-route before disable")
	}
	p.SetModelSpecs([]ModelSpec{{ID: "a", Enabled: boolPtr(false)}, {ID: "b", Visible: boolPtr(false)}})
	if len(mgr.ModelProviders("a")) != 0 {
		t.Fatal("disabled model must not route")
	}
	if len(mgr.ModelProviders("b")) != 1 {
		t.Fatal("hidden model must still route by name")
	}
}

// 显式映射同样尊重「禁用」：禁用上游后统一名不再可调用（而不是只是隐藏）。
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

// UpdateModels 保留已有模型的元数据（禁用仍禁用，上下文不丢）。
func TestUpdateModelsKeepsSpecMetadata(t *testing.T) {
	p := New("p", "openai-compatible", "https://api.example.com/v1", []string{"a"}, nil, 0, 0, 0, 0, 0, 0, 0)
	p.SetModelSpecs([]ModelSpec{{ID: "a", Context: 128000, Visible: boolPtr(false), Capabilities: []string{"chat", "vision"}}})
	p.UpdateModels([]string{"a", "b"})
	specs := p.ModelSpecs()
	if len(specs) != 2 {
		t.Fatalf("specs = %+v", specs)
	}
	if specs[0].ID != "a" || specs[0].Context != 128000 || specs[0].IsVisible() {
		t.Fatalf("metadata lost on update: %+v", specs[0])
	}
	if specs[1].ID != "b" || !specs[1].IsEnabled() || len(specs[1].Capabilities) != 1 {
		t.Fatalf("new model defaults wrong: %+v", specs[1])
	}
}
