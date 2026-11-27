package compat

import (
	"io"
	"strings"
	"testing"
)

// TestChatStreamFramesLinesWithoutBlankLines 上游只发 data 行、不发空行时，
// 聊天直通必须为每行补齐事件边界，客户端才能逐帧解析。
func TestChatStreamFramesLinesWithoutBlankLines(t *testing.T) {
	raw := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"b\"}}]}\n" +
		"data: [DONE]\n"
	want := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"b\"}}]}\n\n" +
		"data: [DONE]\n\n"
	out, err := io.ReadAll(wrapReasoningStream(io.NopCloser(strings.NewReader(raw))))
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(out) != want {
		t.Fatalf("chat lines not framed:\n want %q\n got  %q", want, string(out))
	}
}

// TestReasoningStreamHandlesFragmentedLongLine 覆盖 bufio 缓冲被打满
// （ErrBufferFull）时仍能完成 reasoning → reasoning_content 标准化。
func TestReasoningStreamHandlesFragmentedLongLine(t *testing.T) {
	payload := strings.Repeat("x", 20<<10) // 大于 16KB 的读取缓冲
	raw := `data: {"choices":[{"index":0,"delta":{"reasoning":"` + payload + `"}}]}` + "\n\ndata: [DONE]\n\n"
	out, err := io.ReadAll(wrapReasoningStream(io.NopCloser(strings.NewReader(raw))))
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	text := string(out)
	if strings.Contains(text, `"reasoning":`) {
		t.Fatal("redundant reasoning field survived long-line normalization")
	}
	if !strings.Contains(text, `"reasoning_content":"`+payload+`"`) {
		t.Fatal("long reasoning payload was lost or corrupted")
	}
	if !strings.Contains(text, `"content":null`) {
		t.Fatal("thinking delta lost content:null")
	}
	if !strings.Contains(text, "data: [DONE]") {
		t.Fatal("terminal frame lost")
	}
}
