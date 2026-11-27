// 思考字段兼容层回归测试：reasoning 必须同步为 reasoning_content，其它字节不动。
package compat

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"harbor/internal/provider"
)

// TestAliasReasoningBodyKeepsNumbersAndAddsContent 验证非流式响应补齐字段且不改动数字/其它字段。
func TestAliasReasoningBodyKeepsNumbersAndAddsContent(t *testing.T) {
	body := []byte(`{"id":"c1","created":1789043399,"model":"m","choices":[{"index":0,"message":{"role":"assistant","reasoning":"想一下","content":"答案"},"finish_reason":"stop"}]}`)
	got := AliasReasoningBody(body)
	for _, want := range []string{`"reasoning_content":"想一下"`, `"created":1789043399`, `"content":"答案"`, `"model":"m"`} {
		if !bytes.Contains(got, []byte(want)) {
			t.Fatalf("missing %s in %s", want, got)
		}
	}
	if bytes.Contains(got, []byte("e+09")) {
		t.Fatalf("number was rewritten: %s", got)
	}
}

// TestAliasReasoningBodyIdempotent 已经在 reasoning_content 时不再重复添加。
func TestAliasReasoningBodyWithoutReasoningIsUntouched(t *testing.T) {
	body := []byte(`{"choices":[{"index":0,"delta":{"content":"hi"}}]}`)
	if got := AliasReasoningBody(body); !bytes.Equal(got, body) {
		t.Fatalf("body changed unexpectedly: %s", got)
	}
	already := []byte(`{"choices":[{"index":0,"delta":{"reasoning":"x","reasoning_content":"x"}}]}`)
	if got := AliasReasoningBody(already); !bytes.Equal(got, already) {
		t.Fatalf("already aliased body changed: %s", got)
	}
}

// TestAliasReasoningStreamEmitsPerEvent 验证逐事件立即输出，且保留心跳、[DONE] 与用量接口。
func TestAliasReasoningStreamEmitsPerEvent(t *testing.T) {
	source := strings.Join([]string{
		": keepalive\n\n",
		`data: {"id":"c1","created":1789043399,"choices":[{"index":0,"delta":{"role":"assistant","reasoning":"第一步"}}]}` + "\n\n",
		`data: {"choices":[{"index":0,"delta":{"content":"正文"}}]}` + "\n\n",
		`data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}` + "\n\n",
		"data: [DONE]\n\n",
	}, "")
	usage := &usageSource{src: strings.NewReader(source), usage: provider.Usage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7}, known: true}
	stream := AliasReasoningStream(usage)
	defer stream.Close()

	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	text := string(got)
	for _, want := range []string{": keepalive", `"reasoning":"第一步"`, `"reasoning_content":"第一步"`, `"content":"正文"`, "data: [DONE]", `"usage"`, `"created":1789043399`} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %s in %s", want, text)
		}
	}
	if strings.Contains(text, `"content":"正文","reasoning_content"`) {
		t.Fatalf("content-only delta must not gain reasoning_content: %s", text)
	}
	if u, known := stream.(provider.UsageReader).StreamUsage(); !known || u.TotalTokens != 7 {
		t.Fatalf("usage not forwarded: %+v known=%v", u, known)
	}
	if strings.HasPrefix(text, "data: data:") {
		t.Fatalf("data prefix duplicated: %s", text)
	}
}

// usageSource 模拟 provider 的流式响应体（带用量旁路）。
type usageSource struct {
	src   io.Reader
	usage provider.Usage
	known bool
}

func (s *usageSource) Read(p []byte) (int, error) { return s.src.Read(p) }
func (s *usageSource) Close() error               { return nil }
func (s *usageSource) StreamUsage() (provider.Usage, bool) {
	return s.usage, s.known
}
