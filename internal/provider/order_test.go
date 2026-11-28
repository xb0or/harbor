package provider

import "testing"

func TestManagerProviderOrder(t *testing.T) {
	a := New("a", "openai-compatible", "https://a.example/v1", []string{"m"}, nil, 0, 0, 0, 0, 0, 0, 0)
	b := New("b", "openai-compatible", "https://b.example/v1", []string{"m"}, nil, 0, 0, 0, 0, 0, 0, 0)
	c := New("c", "openai-compatible", "https://c.example/v1", []string{"m"}, nil, 0, 0, 0, 0, 0, 0, 0)
	mgr := NewManager([]*Provider{a, b, c}, nil)

	names := func(list []*Provider) []string {
		out := make([]string, len(list))
		for i, p := range list {
			out[i] = p.Name
		}
		return out
	}
	if got := names(mgr.Providers()); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("initial order = %v", got)
	}
	if !mgr.ReorderProviders([]string{"c", "a", "b"}) {
		t.Fatal("reorder failed")
	}
	if got := names(mgr.Providers()); got[0] != "c" || got[1] != "a" || got[2] != "b" {
		t.Fatalf("reordered = %v", got)
	}
	if got := names(mgr.ModelProviders("m")); got[0] != "c" || got[1] != "a" || got[2] != "b" {
		t.Fatalf("auto route order = %v", got)
	}
	mgr.AddProvider(New("d", "openai-compatible", "https://d.example/v1", []string{"m"}, nil, 0, 0, 0, 0, 0, 0, 0))
	if got := names(mgr.Providers()); len(got) != 4 || got[3] != "d" {
		t.Fatalf("append order = %v", got)
	}
}
