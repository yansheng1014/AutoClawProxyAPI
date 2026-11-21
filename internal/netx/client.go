package netx

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Factory 管理按代理地址复用的 http.Transport，避免频繁重建连接池。
type Factory struct {
	mu           sync.RWMutex
	proxyByKey   map[string]string
	transportMap map[string]*http.Transport
}

// NewFactory 创建代理客户端工厂。
func NewFactory(defaultProxy, cnProxy, globalProxy string) *Factory {
	f := &Factory{
		proxyByKey:   make(map[string]string, 3),
		transportMap: make(map[string]*http.Transport, 4),
	}
	f.UpdateProxies(defaultProxy, cnProxy, globalProxy)
	return f
}

// UpdateProxies 动态更新全局及各区域的代理配置。
func (f *Factory) UpdateProxies(defaultProxy, cnProxy, globalProxy string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proxyByKey["default"] = strings.TrimSpace(defaultProxy)
	f.proxyByKey["cn"] = strings.TrimSpace(cnProxy)
	f.proxyByKey["global"] = strings.TrimSpace(globalProxy)
}

// ProxyURL 返回某区域当前生效的代理字符串。
func (f *Factory) ProxyURL(region string) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	key := strings.ToLower(strings.TrimSpace(region))
	if key == "cn" && f.proxyByKey["cn"] != "" {
		return f.proxyByKey["cn"]
	}
	if key == "global" && f.proxyByKey["global"] != "" {
		return f.proxyByKey["global"]
	}
	return f.proxyByKey["default"]
}

// Client 获取适用于指定区域的 HTTP 客户端（timeout <= 0 时用于长连接流式响应）。
func (f *Factory) Client(region string, timeout time.Duration) *http.Client {
	proxyStr := f.ProxyURL(region)
	tr, err := f.getTransport(proxyStr)
	if err != nil {
		// 代理配置异常时返回默认直连传输并在首次请求报错提示
		return &http.Client{
			Timeout: timeout,
			Transport: &errorTransport{
				err: fmt.Errorf("代理地址配置无效 (%s): %w", proxyStr, err),
			},
		}
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}
}

func (f *Factory) getTransport(proxyStr string) (*http.Transport, error) {
	f.mu.RLock()
	if tr, ok := f.transportMap[proxyStr]; ok {
		f.mu.RUnlock()
		return tr, nil
	}
	f.mu.RUnlock()

	tr, err := buildTransport(proxyStr)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.transportMap[proxyStr]; ok {
		return existing, nil
	}
	f.transportMap[proxyStr] = tr
	return tr, nil
}

func buildTransport(proxyStr string) (*http.Transport, error) {
	baseDialer := &net.Dialer{
		Timeout:   20 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := &http.Transport{
		DialContext:           baseDialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       5 * time.Minute,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if proxyStr == "" {
		tr.Proxy = http.ProxyFromEnvironment
		return tr, nil
	}
	u, err := url.Parse(proxyStr)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		tr.Proxy = http.ProxyURL(u)
		return tr, nil
	case "socks5", "socks5h":
		d, err := newSOCKS5Dialer(u)
		if err != nil {
			return nil, err
		}
		tr.Proxy = nil
		tr.DialContext = d.DialContext
		return tr, nil
	default:
		return nil, fmt.Errorf("不支持的代理协议: %s（仅支持 http/https/socks5/socks5h）", u.Scheme)
	}
}

type errorTransport struct {
	err error
}

func (e *errorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, e.err
}
