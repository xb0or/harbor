package server

import (
	"testing"
)

func TestEstimatePromptTokensIncludesOutputBudget(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"1234567890"}],"max_tokens":100}`)
	if got, want := estimatePromptTokens(body), 118; got != want {
		t.Fatalf("estimated tokens = %d, want %d", got, want)
	}
}

func TestEstimatePromptTokensUsesLargestOutputField(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"1234567890"}],"max_completion_tokens":80,"max_output_tokens":120}`)
	if got, want := estimatePromptTokens(body), 138; got != want {
		t.Fatalf("estimated tokens = %d, want %d", got, want)
	}
}

func TestRejectDuplicateTopLevelFields(t *testing.T) {
	if err := rejectDuplicateTopLevelFields([]byte(`{"model":"m","model":"m"}`)); err == nil {
		t.Fatal("duplicate top-level fields must be rejected")
	}
	if err := rejectDuplicateTopLevelFields([]byte(`{"model":"m","messages":{"model":"nested"}}`)); err != nil {
		t.Fatalf("nested fields should remain valid: %v", err)
	}
}
