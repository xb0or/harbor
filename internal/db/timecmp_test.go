package db

import (
	"path/filepath"
	"testing"
	"time"
)

func TestTimeFilterFormat(t *testing.T) {
	d, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Migrate(); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := d.RecordRequest(&Request{
		RequestID: "r1", KeyID: "k", Model: "m", Status: "success",
		QueuedAt: now, FinishedAt: now, ClientUA: "ua",
	}); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := d.sql.QueryRow(`SELECT finished_at FROM requests WHERE request_id='r1'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	t.Logf("stored finished_at = %q", raw)
	var cnt int
	if err := d.sql.QueryRow(`SELECT COUNT(*) FROM requests WHERE substr(finished_at,1,10) >= ?`, now.In(time.Local).Format("2006-01-02")).Scan(&cnt); err != nil {
		t.Fatalf("date compare error: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("date compare count = %d, want 1", cnt)
	}
	out, total, err := d.ListRequests(RequestFilter{Since: now.Add(-time.Hour), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || total != 1 {
		t.Fatalf("ListRequests since-1h = %d rows, total=%d", len(out), total)
	}
}
