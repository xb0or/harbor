// Package netproxy 提供出站 HTTP / SOCKS5 代理拨号与 Clash 订阅解析。
package netproxy

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Node 是一个可用的出站代理节点。
type Node struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"` // http | socks5
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// Dialer 返回一个符合 net.Dialer.DialContext 签名的代理拨号器。
func Dialer(node Node) (func(ctx context.Context, network, addr string) (net.Conn, error), error) {
	switch strings.ToLower(strings.TrimSpace(node.Type)) {
	case "http", "https":
		return httpDialer(node)
	case "socks5", "socks":
		return socks5Dialer(node)
	default:
		return nil, fmt.Errorf("unsupported proxy type %q", node.Type)
	}
}

func proxyAddress(node Node) string {
	return net.JoinHostPort(node.Host, strconv.Itoa(node.Port))
}

func httpDialer(node Node) (func(context.Context, string, string) (net.Conn, error), error) {
	if node.Host == "" || node.Port <= 0 || node.Port > 65535 {
		return nil, fmt.Errorf("invalid http proxy address")
	}
	proxyAddr := proxyAddress(node)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := &net.Dialer{Timeout: 15 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, err
		}
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		var b strings.Builder
		fmt.Fprintf(&b, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", addr, addr)
		if node.Username != "" || node.Password != "" {
			auth := base64.StdEncoding.EncodeToString([]byte(node.Username + ":" + node.Password))
			fmt.Fprintf(&b, "Proxy-Authorization: Basic %s\r\n", auth)
		}
		b.WriteString("\r\n")
		if _, err := io.WriteString(conn, b.String()); err != nil {
			conn.Close()
			return nil, err
		}
		br := bufio.NewReader(conn)
		resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("http proxy handshake: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			conn.Close()
			return nil, fmt.Errorf("http proxy returned %d", resp.StatusCode)
		}
		_ = conn.SetDeadline(time.Time{})
		return conn, nil
	}, nil
}

func socks5Dialer(node Node) (func(context.Context, string, string) (net.Conn, error), error) {
	if node.Host == "" || node.Port <= 0 || node.Port > 65535 {
		return nil, fmt.Errorf("invalid socks5 proxy address")
	}
	proxyAddr := proxyAddress(node)
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := &net.Dialer{Timeout: 15 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, err
		}
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		if err := socks5Handshake(conn, node, addr); err != nil {
			conn.Close()
			return nil, err
		}
		_ = conn.SetDeadline(time.Time{})
		return conn, nil
	}, nil
}

func socks5Handshake(conn net.Conn, node Node, target string) error {
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid target port")
	}
	methods := []byte{0x00}
	if node.Username != "" || node.Password != "" {
		methods = append(methods, 0x02)
	}
	greeting := append([]byte{0x05, byte(len(methods))}, methods...)
	if _, err := conn.Write(greeting); err != nil {
		return err
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if reply[0] != 0x05 {
		return fmt.Errorf("socks5 bad version")
	}
	switch reply[1] {
	case 0x00:
	case 0x02:
		user := []byte(node.Username)
		pass := []byte(node.Password)
		if len(user) > 255 || len(pass) > 255 {
			return fmt.Errorf("socks5 credential too long")
		}
		msg := append([]byte{0x01, byte(len(user))}, user...)
		msg = append(msg, byte(len(pass)))
		msg = append(msg, pass...)
		if _, err := conn.Write(msg); err != nil {
			return err
		}
		if _, err := io.ReadFull(conn, reply); err != nil {
			return err
		}
		if reply[0] != 0x01 || reply[1] != 0x00 {
			return fmt.Errorf("socks5 auth failed")
		}
	default:
		return fmt.Errorf("socks5 no acceptable auth method")
	}

	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			req = append(req, 0x01)
			req = append(req, v4...)
		} else {
			req = append(req, 0x04)
			req = append(req, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return fmt.Errorf("socks5 target host too long")
		}
		req = append(req, 0x03, byte(len(host)))
		req = append(req, []byte(host)...)
	}
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		return err
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	if head[0] != 0x05 || head[1] != 0x00 {
		return fmt.Errorf("socks5 connect failed (code=%d)", head[1])
	}
	var addrLen int
	switch head[3] {
	case 0x01:
		addrLen = 4
	case 0x04:
		addrLen = 16
	case 0x03:
		n := make([]byte, 1)
		if _, err := io.ReadFull(conn, n); err != nil {
			return err
		}
		addrLen = int(n[0])
	default:
		return fmt.Errorf("socks5 bad address type")
	}
	if _, err := io.ReadFull(conn, make([]byte, addrLen+2)); err != nil {
		return err
	}
	return nil
}

type clashDocument struct {
	Proxies []struct {
		Name     string `yaml:"name"`
		Type     string `yaml:"type"`
		Server   string `yaml:"server"`
		Port     int    `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"proxies"`
}

// ParseClash 解析 Clash 订阅里的 http / socks5 节点。
func ParseClash(data []byte) ([]Node, error) {
	var doc clashDocument
	if err := yaml.Unmarshal(data, &doc); err != nil || len(doc.Proxies) == 0 {
		// 部分订阅返回 base64 包裹的 YAML，做一次兼容解码。
		if decoded, derr := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data))); derr == nil {
			var wrapped clashDocument
			if yaml.Unmarshal(decoded, &wrapped) == nil && len(wrapped.Proxies) > 0 {
				doc = wrapped
			}
		}
	}
	if len(doc.Proxies) == 0 {
		return nil, fmt.Errorf("parse clash subscription: no proxies")
	}
	out := make([]Node, 0, len(doc.Proxies))
	seen := map[string]bool{}
	for _, p := range doc.Proxies {
		typ := strings.ToLower(strings.TrimSpace(p.Type))
		if typ != "http" && typ != "socks5" && typ != "socks" {
			continue
		}
		if typ == "socks" {
			typ = "socks5"
		}
		if p.Server == "" || p.Port <= 0 || p.Port > 65535 {
			continue
		}
		id := NodeID(typ, p.Server, p.Port, p.Username)
		if seen[id] {
			continue
		}
		seen[id] = true
		name := strings.TrimSpace(p.Name)
		if name == "" {
			name = net.JoinHostPort(p.Server, strconv.Itoa(p.Port))
		}
		out = append(out, Node{
			ID: id, Name: name, Type: typ, Host: p.Server, Port: p.Port,
			Username: p.Username, Password: p.Password,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("clash subscription has no http/socks5 nodes")
	}
	return out, nil
}

// FetchClash 拉取并解析 Clash 订阅。
func FetchClash(ctx context.Context, rawURL string, timeout time.Duration) ([]Node, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, fmt.Errorf("clash subscription url is required")
	}
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch clash subscription: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clash subscription returned %d", resp.StatusCode)
	}
	const maxBody = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBody {
		return nil, fmt.Errorf("clash subscription too large")
	}
	return ParseClash(body)
}

// NodeID 生成稳定的节点 ID（用于 UI 选择与去重）。
func NodeID(typ, host string, port int, user string) string {
	sum := sha1.Sum([]byte(typ + "|" + host + "|" + strconv.Itoa(port) + "|" + user))
	return hex.EncodeToString(sum[:])[:12]
}
