package netproxy

import "testing"

func TestParseClashHTTPAndSOCKS5(t *testing.T) {
	raw := []byte(`
proxies:
  - name: HK HTTP
    type: http
    server: 127.0.0.1
    port: 7890
    username: u
    password: p
  - name: JP SOCKS
    type: socks5
    server: proxy.example.com
    port: 1080
  - name: ss node
    type: ss
    server: 1.2.3.4
    port: 8388
  - name: duplicate
    type: http
    server: 127.0.0.1
    port: 7890
    username: u
`)
	nodes, err := ParseClash(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v", nodes)
	}
	if nodes[0].Type != "http" || nodes[0].Name != "HK HTTP" || nodes[0].Port != 7890 {
		t.Fatalf("http node = %+v", nodes[0])
	}
	if nodes[1].Type != "socks5" || nodes[1].Host != "proxy.example.com" {
		t.Fatalf("socks node = %+v", nodes[1])
	}
	if nodes[0].ID == "" || nodes[0].ID == nodes[1].ID {
		t.Fatalf("ids = %q %q", nodes[0].ID, nodes[1].ID)
	}
}

func TestDialerRejectsUnsupported(t *testing.T) {
	if _, err := Dialer(Node{Type: "ss", Host: "x", Port: 1}); err == nil {
		t.Fatal("expected unsupported proxy type error")
	}
}
