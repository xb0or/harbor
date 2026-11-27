// Package provider 的连接安全辅助。
//
// PublicOnly Provider（管理台动态创建）的连接层 SSRF 防护：
// 所有拨号前解析并拒绝私网/本地/链路本地地址，禁止跨源重定向。
// 静态配置的 Provider 使用 upstreamHTTPClient，允许内网上游，
// 由 operator 在配置层负责可信性。
package provider

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// publicOnlyHTTPClient 只允许连接公网地址，用于 PublicOnly Provider。
var publicOnlyHTTPClient = &http.Client{
	Transport: func() http.RoundTripper {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.Proxy = nil
		t.DialContext = safeDialContext
		return t
	}(),
	Timeout: 0, // 由请求级 context / provider timeout 控制，与 upstreamHTTPClient 一致
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return fmt.Errorf("upstream redirect limit exceeded")
		}
		if len(via) > 0 {
			previous := via[len(via)-1].URL
			if req.URL.Scheme != previous.Scheme || req.URL.Host != previous.Host {
				return fmt.Errorf("cross-origin upstream redirect blocked")
			}
		}
		if err := validatePublicHost(req.URL.Hostname()); err != nil {
			return err
		}
		return nil
	},
}

// dialTimeout 是安全拨号的单次连接超时。
const dialTimeout = 10 * time.Second

func safeDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		if restrictedIP(ip) {
			return nil, fmt.Errorf("refusing private or local provider address")
		}
		return (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, network, address)
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, ip := range ips {
		if restrictedIP(ip) {
			continue
		}
		conn, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("provider host resolves only to private or local addresses")
}

func restrictedIP(ip net.IP) bool {
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return true
	}
	v4 := ip.To4()
	if v4 == nil {
		return false
	}
	// Carrier-grade NAT and benchmarking ranges are not public service targets.
	if v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return true
	}
	return v4[0] == 198 && v4[1] >= 18 && v4[1] <= 19
}

func validatePublicHost(host string) error {
	if ip := net.ParseIP(host); ip != nil {
		if restrictedIP(ip) {
			return fmt.Errorf("provider URL points to a private or local address")
		}
		return nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve provider URL host: %w", err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("provider URL host has no address")
	}
	for _, ip := range ips {
		if restrictedIP(ip) {
			return fmt.Errorf("provider URL points to a private or local address")
		}
	}
	return nil
}
