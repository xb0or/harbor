// check：直连 SQLite 查看持久化落库内容（只读查询，冒烟/排障用）。
// 用法: go run ./tools/smoke/check.go [-db path/to/harbor.db]
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := flag.String("db", "harbor.db", "path to harbor sqlite db")
	flag.Parse()
	db, err := sql.Open("sqlite", "file:"+*dbPath)
	if err != nil {
		fmt.Println("open err:", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("== api_keys ==")
	rows, err := db.Query(`SELECT id, prefix, key_hash, status, allowed_models FROM api_keys`)
	if err != nil {
		fmt.Println("keys query err:", err)
		os.Exit(1)
	}
	for rows.Next() {
		var id, prefix, hash, status, models string
		if err := rows.Scan(&id, &prefix, &hash, &status, &models); err != nil {
			fmt.Println("keys scan err:", err)
			os.Exit(1)
		}
		hs := hash
		if len(hs) > 24 {
			hs = hs[:24] + "..."
		}
		fmt.Printf("id=%s prefix=%s hash=%s status=%s allowed_models=%s\n",
			id, prefix, hs, status, models)
	}
	rows.Close()

	fmt.Println("== provider_credentials ==")
	rows, err = db.Query(`SELECT id, provider_id, secret_encrypted, secret_fingerprint, status FROM provider_credentials`)
	if err != nil {
		fmt.Println("creds query err:", err)
		os.Exit(1)
	}
	for rows.Next() {
		var id, pid, enc, fp, status string
		if err := rows.Scan(&id, &pid, &enc, &fp, &status); err != nil {
			fmt.Println("creds scan err:", err)
			os.Exit(1)
		}
		fmt.Printf("id=%s provider=%s enc_len=%d enc_head=%s... fp=%s status=%s\n",
			id, pid, len(enc), enc[:min(20, len(enc))], fp[:min(16, len(fp))], status)
	}
	rows.Close()

	fmt.Println("== requests (last 8) ==")
	rows, err = db.Query(`SELECT request_id, key_id, model, status, queue_wait_ms, attempts, tokens_in, tokens_out FROM requests ORDER BY finished_at DESC LIMIT 8`)
	if err != nil {
		fmt.Println("reqs query err:", err)
		os.Exit(1)
	}
	for rows.Next() {
		var rid, kid, model, status string
		var qw, att, tin, tout int
		if err := rows.Scan(&rid, &kid, &model, &status, &qw, &att, &tin, &tout); err != nil {
			fmt.Println("reqs scan err:", err)
			os.Exit(1)
		}
		fmt.Printf("request_id=%s key=%s model=%s status=%s wait_ms=%d attempts=%d tokens=%d/%d\n",
			rid, kid, model, status, qw, att, tin, tout)
	}
	rows.Close()
	fmt.Println("counts:")
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM api_keys`).Scan(&n)
	fmt.Printf("  api_keys=%d\n", n)
	_ = db.QueryRow(`SELECT COUNT(*) FROM provider_credentials`).Scan(&n)
	fmt.Printf("  provider_credentials=%d\n", n)
	_ = db.QueryRow(`SELECT COUNT(*) FROM requests`).Scan(&n)
	fmt.Printf("  requests=%d\n", n)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
