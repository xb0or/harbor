package server

import "testing"

func TestMatchCatalog(t *testing.T) {
	entries := []catalogEntry{
		{ID: "gpt-5.1", Context: 400000},
		{ID: "deepseek-v4.1-flash", Context: 128000},
		{ID: "claude-sonnet-4.5", Context: 200000},
		{ID: "text-embedding-3-large"},
	}
	byID := map[string]catalogEntry{}
	for _, e := range entries {
		byID[e.ID] = e
	}
	cases := []struct {
		want string
		id   string
		ok   bool
	}{
		{"gpt-5.1", "gpt-5.1", true},
		{"deepseek-v4.1-flash:dev", "deepseek-v4.1-flash", true},
		{"openai/gpt-5.1", "gpt-5.1", true},
		{"GPT-5.1", "gpt-5.1", true},
		{"gpt-5.1-codex:free", "gpt-5.1", true},
		{"no-such-model", "", false},
	}
	for _, c := range cases {
		got, ok := matchCatalog(byID, entries, c.want)
		if ok != c.ok || got.ID != c.id {
			t.Fatalf("matchCatalog(%q) = (%q,%v), want (%q,%v)", c.want, got.ID, ok, c.id, c.ok)
		}
	}
}
