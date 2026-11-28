package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProxyAPIRoundTripAndMask(t *testing.T) {
	up := newMockUpstream(10 * time.Millisecond)
	ups := httptest.NewServer(up.Handler())
	defer ups.Close()
	cfg := testConfig()
	handler, _, _ := buildTestServer(t, cfg, ups)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	body := `{"mode":"direct","active":"n1","nodes":[{"id":"n1","name":"local","type":"http","host":"127.0.0.1","port":7890,"username":"u","password":"p"}],"clash_url":""}`
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/proxy", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+cfg.Admin.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT proxy status=%d body=%s", resp.StatusCode, raw)
	}

	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/api/proxy", nil)
	req.Header.Set("Authorization", "Bearer "+cfg.Admin.Token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	var view proxySettings
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Nodes) != 1 || view.Nodes[0].Password != "***" {
		t.Fatalf("proxy view = %+v body=%s", view, raw)
	}
	if view.Nodes[0].Host != "127.0.0.1" || view.Nodes[0].Port != 7890 {
		t.Fatalf("node lost: %+v", view.Nodes[0])
	}
}
