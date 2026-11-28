package netproxy

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestHTTPProxyDialer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		req, err := http.ReadRequest(br)
		if err != nil || req.Method != http.MethodConnect {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		_, _ = io.Copy(conn, conn)
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	dial, err := Dialer(Node{Type: "http", Host: "127.0.0.1", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := dial(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo = %q err=%v", buf, err)
	}
}

func TestSOCKS5ProxyDialer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		head := make([]byte, 2)
		if _, err := io.ReadFull(conn, head); err != nil {
			return
		}
		methods := make([]byte, int(head[1]))
		if _, err := io.ReadFull(conn, methods); err != nil {
			return
		}
		_, _ = conn.Write([]byte{0x05, 0x00})
		req := make([]byte, 4)
		if _, err := io.ReadFull(conn, req); err != nil {
			return
		}
		switch req[3] {
		case 0x01:
			_, _ = io.ReadFull(conn, make([]byte, 4))
		case 0x03:
			n := make([]byte, 1)
			_, _ = io.ReadFull(conn, n)
			_, _ = io.ReadFull(conn, make([]byte, int(n[0])))
		case 0x04:
			_, _ = io.ReadFull(conn, make([]byte, 16))
		}
		_, _ = io.ReadFull(conn, make([]byte, 2))
		_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		_, _ = io.Copy(conn, conn)
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	dial, err := Dialer(Node{Type: "socks5", Host: "127.0.0.1", Port: port})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := dial(context.Background(), "tcp", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo = %q err=%v", buf, err)
	}
}
