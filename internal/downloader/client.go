package downloader

import (
	"net"
	"net/http"
	"time"
)

// Client wraps a tuned net/http client and exposes a stable interface for reuse.
type Client struct {
	HTTP *http.Client
}

func NewClient() *Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 45 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	return &Client{
		HTTP: &http.Client{
			Transport: transport,
			Timeout:   0, // cancellation via ctx; per-request timeouts are enforced by context.
		},
	}
}

