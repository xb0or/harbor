package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// models.dev 模型目录：用于给上游模型补齐上下文窗口与能力。
// 目录在进程内缓存 6 小时；拉取失败时保留旧数据并返回错误说明。

const (
	catalogURL = "https://models.dev/api.json"
	catalogTTL = 6 * time.Hour
)

type catalogEntry struct {
	ID           string   `json:"id"`
	Name         string   `json:"name,omitempty"`
	Provider     string   `json:"provider,omitempty"`
	Context      int      `json:"context,omitempty"`
	MaxOutput    int      `json:"max_output,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
}

type catalogRawModel struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Reasoning  bool   `json:"reasoning"`
	ToolCall   bool   `json:"tool_call"`
	Attachment bool   `json:"attachment"`
	Modalities struct {
		Input  []string `json:"input"`
		Output []string `json:"output"`
	} `json:"modalities"`
	Limit struct {
		Context int `json:"context"`
		Output  int `json:"output"`
	} `json:"limit"`
}

var catalogState = struct {
	mu      sync.Mutex
	fetched time.Time
	entries []catalogEntry
	err     string
}{}

var catalogClient = &http.Client{Timeout: 15 * time.Second}

// loadCatalog 返回缓存的目录；过期则重新拉取（失败时保留旧数据）。
func loadCatalog() ([]catalogEntry, string) {
	catalogState.mu.Lock()
	defer catalogState.mu.Unlock()
	if len(catalogState.entries) > 0 && time.Since(catalogState.fetched) < catalogTTL {
		return catalogState.entries, catalogState.err
	}
	entries, err := fetchCatalog()
	if err != nil {
		catalogState.err = err.Error()
		return catalogState.entries, catalogState.err
	}
	catalogState.entries = entries
	catalogState.fetched = time.Now()
	catalogState.err = ""
	return entries, ""
}

func fetchCatalog() ([]catalogEntry, error) {
	req, err := http.NewRequest(http.MethodGet, catalogURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "harbor/1.0 (+https://models.dev)")
	resp, err := catalogClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch models.dev: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("models.dev returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("read models.dev: %w", err)
	}
	var raw map[string]struct {
		ID     string                     `json:"id"`
		Name   string                     `json:"name"`
		Models map[string]catalogRawModel `json:"models"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode models.dev: %w", err)
	}

	seen := make(map[string]bool)
	out := make([]catalogEntry, 0, 4096)
	for key, provider := range raw {
		providerName := provider.ID
		if providerName == "" {
			providerName = key
		}
		for id, m := range provider.Models {
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			caps := []string{"chat"}
			if hasAny(m.Modalities.Input, "image") || m.Attachment {
				caps = append(caps, "vision")
			}
			if m.Reasoning {
				caps = append(caps, "reasoning")
			}
			if m.ToolCall {
				caps = append(caps, "tools")
			}
			if hasAny(m.Modalities.Output, "embedding") {
				caps = []string{"embedding"}
			}
			out = append(out, catalogEntry{
				ID:           id,
				Name:         m.Name,
				Provider:     providerName,
				Context:      m.Limit.Context,
				MaxOutput:    m.Limit.Output,
				Capabilities: caps,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func hasAny(items []string, want string) bool {
	for _, item := range items {
		if strings.EqualFold(item, want) {
			return true
		}
	}
	return false
}

// matchCatalog 用宽松规则把上游模型 id 对到目录条目：
// 精确 → 去掉 ":tag" 后缀 → 去掉 "provider/" 前缀 → 忽略大小写。
func matchCatalog(byID map[string]catalogEntry, entries []catalogEntry, want string) (catalogEntry, bool) {
	want = strings.TrimSpace(want)
	if want == "" {
		return catalogEntry{}, false
	}
	candidates := []string{want}
	if i := strings.LastIndex(want, ":"); i > 0 {
		candidates = append(candidates, want[:i])
	}
	if i := strings.Index(want, "/"); i >= 0 && i+1 < len(want) {
		candidates = append(candidates, want[i+1:])
	}
	for _, c := range candidates {
		if e, ok := byID[c]; ok {
			return e, true
		}
	}
	lowered := make([]string, 0, len(candidates))
	for _, c := range candidates {
		lowered = append(lowered, strings.ToLower(c))
	}
	for _, e := range entries {
		id := strings.ToLower(e.ID)
		for _, c := range lowered {
			if id == c {
				return e, true
			}
		}
	}
	// 形如 gpt-5.1-codex:free → gpt-5.1-codex 的宽松前缀匹配
	for _, e := range entries {
		id := strings.ToLower(e.ID)
		if id == "" {
			continue
		}
		for _, c := range lowered {
			if strings.HasPrefix(c, id) && len(c) > len(id) && (c[len(id)] == ':' || c[len(id)] == '-') {
				return e, true
			}
		}
	}
	return catalogEntry{}, false
}

// handleModelCatalog GET /api/model-catalog?q=&ids=a,b,c
// q 用于模糊搜索；ids 用于按已声明模型批量匹配（返回命中的目录条目）。
func (s *Server) handleModelCatalog(w http.ResponseWriter, r *http.Request) {
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	idsParam := strings.TrimSpace(r.URL.Query().Get("ids"))
	entries, errText := loadCatalog()

	if idsParam != "" {
		byID := make(map[string]catalogEntry, len(entries))
		for _, e := range entries {
			byID[e.ID] = e
		}
		matches := make([]map[string]any, 0, 16)
		for _, want := range strings.Split(idsParam, ",") {
			want = strings.TrimSpace(want)
			if want == "" {
				continue
			}
			if e, ok := matchCatalog(byID, entries, want); ok {
				matches = append(matches, map[string]any{"requested": want, "model": e})
			} else {
				matches = append(matches, map[string]any{"requested": want, "model": nil})
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"matches": matches,
			"total":   len(entries),
			"error":   errText,
		})
		return
	}

	out := make([]catalogEntry, 0, 64)
	for _, e := range entries {
		if q != "" && !strings.Contains(strings.ToLower(e.ID), q) && !strings.Contains(strings.ToLower(e.Name), q) {
			continue
		}
		out = append(out, e)
		if len(out) >= 300 {
			break
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"models": out,
		"total":  len(entries),
		"error":  errText,
	})
}
