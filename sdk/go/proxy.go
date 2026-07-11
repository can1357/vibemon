package vmon

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// ProxyRequest describes one HTTP request forwarded to an exposed guest port.
type ProxyRequest struct {
	// Method is an HTTP method supported by the proxy route.
	Method string
	// Path is the guest-relative path; each component is URL-escaped by the client.
	Path string
	// Query contains guest query parameters.
	Query url.Values
	// Header contains headers forwarded through the daemon's allowlist.
	Header http.Header
	// Body is consumed by the request and closed when it implements io.ReadCloser.
	Body io.Reader
}

// ProxyHTTP forwards an HTTP request to an exposed guest port.
//
// Non-2xx responses are returned as APIError values. On success, the caller
// owns and must close the returned response body.
func (client *Client) ProxyHTTP(
	ctx context.Context,
	id string,
	port uint16,
	connectToken string,
	proxy ProxyRequest,
) (*http.Response, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		closeProxyBody(proxy.Body)
		return nil, err
	}
	if port == 0 {
		closeProxyBody(proxy.Body)
		return nil, errors.New("vmon: proxy port must not be zero")
	}
	if connectToken == "" {
		closeProxyBody(proxy.Body)
		return nil, errors.New("vmon: connect token must not be empty")
	}
	if proxy.Method == "" {
		closeProxyBody(proxy.Body)
		return nil, errors.New("vmon: proxy method must not be empty")
	}
	path := sandboxPath(id) + "/ports/" + strconv.FormatUint(uint64(port), 10)
	if escapedRest := escapeRestPath(proxy.Path); escapedRest != "" {
		path += "/" + escapedRest
	}
	query := cloneValues(proxy.Query)
	query.Set("connect_token", connectToken)
	request, err := client.newRequest(ctx, proxy.Method, path, query, proxy.Body, "")
	if err != nil {
		closeProxyBody(proxy.Body)
		return nil, err
	}
	for name, values := range proxy.Header {
		request.Header.Del(name)
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	client.applyHeaders(request.Header)
	return client.do(request)
}

func closeProxyBody(body io.Reader) {
	if closer, ok := body.(io.Closer); ok {
		_ = closer.Close()
	}
}
