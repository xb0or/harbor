package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"harbor/internal/provider"
)

func validatePublicProviderURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid provider URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("provider URL scheme must be http or https")
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("provider URL host is required")
	}
	if u.User != nil {
		return nil, fmt.Errorf("provider URL must not contain user info")
	}
	if u.Fragment != "" || u.RawQuery != "" {
		return nil, fmt.Errorf("provider URL must not contain query or fragment")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("provider URL port is invalid")
		}
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return nil, fmt.Errorf("provider URL points to a private or local address")
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && restrictedIP(ip) {
		return nil, fmt.Errorf("provider URL points to a private or local address")
	}
	return u, nil
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

func newSafeProbeClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if dial := provider.ActiveProxyDialer(); dial != nil {
		transport.DialContext = dial
	} else {
		transport.DialContext = safeDialContext
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			if _, err := validatePublicProviderURL(req.URL.String()); err != nil {
				return err
			}
			if err := validatePublicHost(req.URL.Hostname()); err != nil {
				return err
			}
			return nil
		},
	}
}

func safeDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		if restrictedIP(ip) {
			return nil, fmt.Errorf("refusing private or local provider address")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
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
		conn, err := (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
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
