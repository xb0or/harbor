package server

import "testing"

func TestValidatePublicProviderURLRejectsLocalTargets(t *testing.T) {
	for _, raw := range []string{
		"file:///etc/passwd",
		"http://127.0.0.1:8080/v1",
		"http://[::1]:8080/v1",
		"http://10.0.0.1/v1",
		"http://user:pass@example.com/v1",
		"https://example.com/v1?api_key=secret",
	} {
		if _, err := validatePublicProviderURL(raw); err == nil {
			t.Errorf("validatePublicProviderURL(%q) accepted a restricted URL", raw)
		}
	}
}

func TestValidatePublicProviderURLAcceptsPublicLiteral(t *testing.T) {
	if _, err := validatePublicProviderURL("https://203.0.113.10/v1"); err != nil {
		t.Fatalf("public test-net URL rejected: %v", err)
	}
}
