// 思考字段兼容层。
//
// 部分 OpenAI 兼容上游把思考文本放在 message.reasoning，
// 而 OpenAI 标准与多数客户端只认 reasoning_content。
// 这里只对「非流式」响应补齐字段；流式增量必须原样透传上游字节，
// 因为重新序列化会改变 delta 的字段集合（多出 reasoning_content、
// 丢掉 "content": null），可能让客户端认不出思考分片。
package compat

import (
	"bytes"
	"encoding/json"
)

// reasoningNeedle 用于快速跳过不含思考字段的响应。
var reasoningNeedle = []byte(`"reasoning"`)

// AliasReasoningBody 为普通（非流式）响应补 reasoning_content。
// 解析失败或无需修改时原样返回，绝不破坏上游内容。
func AliasReasoningBody(body []byte) []byte {
	if len(body) == 0 || !bytes.Contains(body, reasoningNeedle) {
		return body
	}
	doc, ok := decodePreservingNumbers(body)
	if !ok {
		return body
	}
	changed := false
	if choices, ok := doc["choices"].([]any); ok {
		for _, item := range choices {
			choice, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if message, ok := choice["message"].(map[string]any); ok && aliasDelta(message) {
				changed = true
			}
			if delta, ok := choice["delta"].(map[string]any); ok && aliasDelta(delta) {
				changed = true
			}
		}
	}
	if !changed {
		return body
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return body
	}
	return encoded
}

// aliasDelta 在某条消息对象上补齐 reasoning_content，返回是否修改。
func aliasDelta(node map[string]any) bool {
	if _, exists := node["reasoning_content"]; exists {
		return false
	}
	reasoning, ok := node["reasoning"].(string)
	if !ok || reasoning == "" {
		return false
	}
	node["reasoning_content"] = reasoning
	return true
}

// decodePreservingNumbers 解析 JSON 且保留数字字面量，避免 created 等字段被改写成科学计数法。
func decodePreservingNumbers(body []byte) (map[string]any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		return nil, false
	}
	return doc, true
}
