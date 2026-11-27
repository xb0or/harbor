package compat

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"harbor/internal/provider"
)

func TestRewriteResponsesStringInput(t *testing.T) {
	path, body, kind, err := RewriteRequest("/v1/responses", []byte(`{
		"model":"Qwen3.8",
		"input":"hi",
		"instructions":"be brief",
		"max_output_tokens":64,
		"store":false
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/chat/completions" || kind != KindResponses {
		t.Fatalf("path=%s kind=%d", path, kind)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["model"] != "Qwen3.8" {
		t.Fatalf("model=%v", got["model"])
	}
	if _, ok := got["input"]; ok {
		t.Fatalf("input leaked: %s", body)
	}
	if _, ok := got["store"]; ok {
		t.Fatalf("store leaked: %s", body)
	}
	if got["max_tokens"] != float64(64) {
		t.Fatalf("max_tokens=%v body=%s", got["max_tokens"], body)
	}
	msgs, _ := got["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("messages=%s", body)
	}
	sys := msgs[0].(map[string]any)
	user := msgs[1].(map[string]any)
	if sys["role"] != "system" || sys["content"] != "be brief" {
		t.Fatalf("system=%v", sys)
	}
	if user["role"] != "user" || user["content"] != "hi" {
		t.Fatalf("user=%v", user)
	}
}

func TestRewriteResponsesArrayInput(t *testing.T) {
	_, body, _, err := RewriteRequest("/v1/responses", []byte(`{
		"model":"m",
		"input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Messages []struct {
			Role    string
			Content string
		}
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 1 || got.Messages[0].Content != "hello" {
		t.Fatalf("got=%s", body)
	}
}

func TestRewriteChatUnchanged(t *testing.T) {
	in := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	path, body, kind, err := RewriteRequest("/v1/chat/completions", in)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/chat/completions" || kind != KindNone || string(body) != string(in) {
		t.Fatalf("path=%s kind=%d body=%s", path, kind, body)
	}
}

func TestChatToResponses(t *testing.T) {
	src := []byte(`{"id":"chatcmpl-abc","object":"chat.completion","created":1,"model":"Qwen3.8","choices":[{"index":0,"message":{"role":"assistant","content":"hi from mock"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`)
	out, err := chatToResponses(src)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if got["object"] != "response" || got["id"] != "resp_abc" || got["status"] != "completed" {
		t.Fatalf("got=%s", out)
	}
	output := got["output"].([]any)
	msg := output[0].(map[string]any)
	content := msg["content"].([]any)
	part := content[0].(map[string]any)
	if part["type"] != "output_text" || part["text"] != "hi from mock" {
		t.Fatalf("content=%v", part)
	}
	usage := got["usage"].(map[string]any)
	if usage["input_tokens"] != float64(10) || usage["output_tokens"] != float64(5) {
		t.Fatalf("usage=%v", usage)
	}
}

func TestRewriteAnthropic(t *testing.T) {
	path, body, kind, err := RewriteRequest("/v1/messages", []byte(`{
		"model":"m",
		"system":"sys",
		"max_tokens":20,
		"stop_sequences":["END"],
		"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/chat/completions" || kind != KindAnthropic {
		t.Fatalf("path=%s kind=%d", path, kind)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["system"]; ok {
		t.Fatalf("system leaked: %s", body)
	}
	if got["max_tokens"] != float64(20) {
		t.Fatalf("max_tokens=%v", got["max_tokens"])
	}
	stop, _ := got["stop"].([]any)
	if len(stop) != 1 || stop[0] != "END" {
		t.Fatalf("stop=%v body=%s", got["stop"], body)
	}
	msgs := got["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("messages=%s", body)
	}
}

func TestChatToAnthropic(t *testing.T) {
	src := []byte(`{"id":"chatcmpl-x","model":"m","choices":[{"message":{"role":"assistant","content":"yo"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)
	out, err := chatToAnthropic(src)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if got["type"] != "message" || got["stop_reason"] != "end_turn" {
		t.Fatalf("got=%s", out)
	}
}

func TestApplyResponseStreamWrapsUsageReader(t *testing.T) {
	inner := io.NopCloser(strings.NewReader(
		"data: {\"id\":\"chatcmpl-z\",\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n" +
			"data: [DONE]\n\n",
	))
	resp := &provider.Response{Stream: true, StreamBody: inner}
	ApplyResponse(KindResponses, resp)
	raw, err := io.ReadAll(resp.StreamBody)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "event: response.output_text.delta") {
		t.Fatalf("missing delta: %s", text)
	}
	if !strings.Contains(text, `"delta":"hi"`) {
		t.Fatalf("missing text: %s", text)
	}
	if !strings.Contains(text, "event: response.completed") {
		t.Fatalf("missing completed: %s", text)
	}
	if r, ok := resp.StreamBody.(provider.UsageReader); !ok {
		t.Fatal("wrapped stream lost UsageReader")
	} else if u, known := r.StreamUsage(); !known || u.TotalTokens != 2 {
		t.Fatalf("usage=%v known=%v", u, known)
	}
}
