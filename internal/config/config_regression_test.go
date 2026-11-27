package config

import (
	"strings"
	"testing"
)

func TestRejectsInvalidOperationalBounds(t *testing.T) {
	tests := []struct {
		name   string
		marker string
		patch  string
		want   string
	}{
		{"zero keepalive", "  max_depth: 10\n", "  max_depth: 10\n  keepalive: 0s\n", "queue.keepalive"},
		{"invalid body size", "  listen: \"0.0.0.0:8080\"\n", "  listen: \"0.0.0.0:8080\"\n  max_body_size: nope\n", "server.max_body_size"},
		{"body budget smaller than body", "  listen: \"0.0.0.0:8080\"\n", "  listen: \"0.0.0.0:8080\"\n  max_body_size: 2mb\n  max_body_bytes_in_flight: 1mb\n", "max_body_bytes_in_flight"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := strings.Replace(baseYAML, tt.marker, tt.patch, 1)
			if _, err := Parse([]byte(raw)); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Parse error = %v, want mention %q", err, tt.want)
			}
		})
	}
}
