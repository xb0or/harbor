package db

import (
	"path/filepath"
	"testing"
)

// models 列必须同时兼容旧格式 ["a","b"] 与新格式 [{"id":"a",...}]。
func TestParseProviderModelsFormats(t *testing.T) {
	specs, enabled := parseProviderModels(`["gpt-4o","deepseek-chat"]`)
	if len(specs) != 2 || len(enabled) != 2 || enabled[0] != "gpt-4o" {
		t.Fatalf("legacy format: specs=%+v enabled=%v", specs, enabled)
	}

	specs, enabled = parseProviderModels(`[{"id":"a","visible":false},{"id":"b","enabled":false},{"id":"c","context":128000}]`)
	if len(specs) != 3 {
		t.Fatalf("spec count = %d", len(specs))
	}
	if len(enabled) != 2 || enabled[0] != "a" || enabled[1] != "c" {
		t.Fatalf("enabled = %v, want [a c] (only b disabled)", enabled)
	}
	if specs[1].Enabled == nil || *specs[1].Enabled {
		t.Fatalf("enabled flag lost: %+v", specs[1])
	}
	if specs[2].Context != 128000 {
		t.Fatalf("context lost: %+v", specs[2])
	}

	if specs, enabled := parseProviderModels(""); specs != nil || enabled != nil {
		t.Fatalf("empty column should parse to nil, got %+v %v", specs, enabled)
	}
}

// 声明写入后可原样读回（含禁用项与上下文）。
func TestProviderModelSpecsRoundTrip(t *testing.T) {
	d, err := Open(filepath.Join(t.TempDir(), "specs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Migrate(); err != nil {
		t.Fatal(err)
	}
	disabled := false
	if err := d.UpsertProvider(&ProviderRow{
		ID: "p", Name: "p", Type: "openai-compatible", BaseURL: "https://api.example.com/v1",
		Models: []string{"a"},
		ModelSpecs: []ModelSpec{
			{ID: "a", Context: 128000, Capabilities: []string{"chat"}, ConcurrencyCost: 0.25},
			{ID: "b", Enabled: &disabled, Context: 8192},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := d.ListProviders()
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	got := rows[0]
	if len(got.ModelSpecs) != 2 {
		t.Fatalf("specs = %+v", got.ModelSpecs)
	}
	if len(got.Models) != 1 || got.Models[0] != "a" {
		t.Fatalf("enabled models = %v, want [a]", got.Models)
	}
	if got.ModelSpecs[1].Enabled == nil || *got.ModelSpecs[1].Enabled {
		t.Fatalf("disabled flag lost: %+v", got.ModelSpecs[1])
	}
	if got.ModelSpecs[1].Context != 8192 {
		t.Fatalf("context lost: %+v", got.ModelSpecs[1])
	}
	if got.ModelSpecs[0].ConcurrencyCost != 0.25 {
		t.Fatalf("concurrency cost lost: %+v", got.ModelSpecs[0])
	}
}
