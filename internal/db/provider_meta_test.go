package db

import (
	"testing"
	"time"
)

func TestProviderMetaRoundTrip(t *testing.T) {
	d, _ := openTest(t)
	if err := d.UpsertProvider(&ProviderRow{
		ID: "p", Name: "p", Type: "openai-compatible", BaseURL: "https://api.example.com/v1",
		Models: []string{"m"}, ModelSpecs: []ModelSpec{{ID: "m"}},
		SortOrder: 3, Headers: map[string]string{"X-Title": "Harbor"}, PassHeaders: true,
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := d.ListProviders()
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	got := rows[0]
	if got.SortOrder != 3 || !got.PassHeaders || got.Headers["X-Title"] != "Harbor" {
		t.Fatalf("provider meta lost: %+v", got)
	}

	if err := d.UpsertProvider(&ProviderRow{
		ID: "q", Name: "q", Type: "openai-compatible", BaseURL: "https://q.example.com/v1",
		Models: []string{"m"}, ModelSpecs: []ModelSpec{{ID: "m"}}, SortOrder: 0, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.SetProviderOrder([]string{"q", "p"}); err != nil {
		t.Fatal(err)
	}
	rows, err = d.ListProviders()
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	if rows[0].Name != "q" || rows[1].Name != "p" {
		t.Fatalf("order = %v %v", rows[0].Name, rows[1].Name)
	}
}
