package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harbor/internal/db"
)

// TestLiveRequestVisibleWithStore 启用 SQLite 时，列表接口仍必须把内存中的
// 进行中请求合并到第一页（DB 只有已结束记录）。
func TestLiveRequestVisibleWithStore(t *testing.T) {
	store, err := db.Open(filepath.Join(t.TempDir(), "harbor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Migrate(); err != nil {
		t.Fatal(err)
	}

	mock := newMockUpstream(300 * time.Millisecond)
	ups := httptest.NewServer(mock.Handler())
	t.Cleanup(ups.Close)
	cfg := testConfig()
	handler, _ := buildTestServerWithStore(t, cfg, ups, store)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	done := make(chan struct{})
	go func() {
		defer close(done)
		body := `{"model":"mock-1","messages":[{"role":"user","content":"hi"}]}`
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+cfg.Keys[0].Secret)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, item := range listRequestLogs(t, ts.URL, cfg.Admin.Token) {
			if item.Model == "mock-1" && isLiveStatus(item.Status) {
				<-done
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	<-done
	t.Fatal("live request not merged into /api/requests while store is enabled")
}
