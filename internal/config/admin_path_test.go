package config

import "testing"

func TestNormalizeAdminPath(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", "/harbor-console", false},
		{"/harbor-console", "/harbor-console", false},
		{"secret-console", "/secret-console", false},
		{"/secret-console/", "/secret-console", false},
		{"/admin", "", true},
		{"/admin/console", "", true},
		{"/api/console", "", true},
		{"/v1/console", "", true},
		{"/bad path", "", true},
		{"//double", "", true},
	}
	for _, tc := range cases {
		got, err := NormalizeAdminPath(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%q: expected error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("%q: got %q err=%v, want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestAdminConfigPathValue(t *testing.T) {
	if got := (AdminConfig{}).PathValue(); got != "/harbor-console" {
		t.Fatalf("default path = %q", got)
	}
	if got := (AdminConfig{Path: "console"}).PathValue(); got != "/console" {
		t.Fatalf("path = %q", got)
	}
}
