package vmon

import (
	"io"
	"net/http"
	"net/url"
)

// ProxyRequest describes one HTTP request forwarded to an exposed guest port.
type ProxyRequest struct {
	Method string
	Path   string
	Query  url.Values
	Header http.Header
	Body   io.Reader
}

func closeProxyBody(body io.Reader) {
	if closer, ok := body.(io.Closer); ok {
		_ = closer.Close()
	}
}
