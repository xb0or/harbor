package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdminPathRuntimeUpdate(t *testing.T) {
	up := newMockUpstream(10 * time.Millisecond)
	ups := httptest.NewServer(up.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	oldPath := cfg.Admin.PathValue()
	if status, _, _ := fetchBody(t, ts.URL+oldPath); status != 200 {
		t.Fatalf("initial admin path status=%d", status)
	}
	body := strings.NewReader(`{"admin_path":"/console-secret"}`)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/settings", body)
	req.Header.Set("Authorization", "Bearer "+cfg.Admin.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT settings status=%d body=%s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), `"/console-secret"`) {
		t.Fatalf("response missing new admin path: %s", raw)
	}
	if status, _, _ := fetchBody(t, ts.URL+oldPath); status != http.StatusNotFound {
		t.Fatalf("old admin path status=%d want 404", status)
	}
	if status, _, _ := fetchBody(t, ts.URL+"/console-secret/"); status != 200 {
		t.Fatalf("new admin path status=%d want 200", status)
	}
	if status, _, _ := fetchBody(t, ts.URL+"/admin"); status != http.StatusNotFound {
		t.Fatalf("legacy admin path status=%d want 404", status)
	}
}
