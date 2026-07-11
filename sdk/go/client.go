package vmon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	defaultMaxResponseBytes int64 = 64 << 20
	maxErrorResponseBytes   int64 = 64 << 10
	maxErrorMessageBytes          = 8 << 10
)

// Option configures a Client during construction.
type Option func(*clientConfig) error

type clientConfig struct {
	httpClient       *http.Client
	token            string
	userAgent        string
	maxResponseBytes int64
}

// WithToken adds an Authorization: Bearer header to HTTP and WebSocket requests.
func WithToken(token string) Option {
	return func(config *clientConfig) error {
		config.token = token
		return nil
	}
}

// WithHTTPClient makes the client use the supplied HTTP client for HTTP and WebSocket handshakes.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(config *clientConfig) error {
		if httpClient == nil {
			return errors.New("vmon: HTTP client must not be nil")
		}
		config.httpClient = httpClient
		return nil
	}
}

// WithUserAgent sets the User-Agent header sent by the client.
func WithUserAgent(userAgent string) Option {
	return func(config *clientConfig) error {
		config.userAgent = userAgent
		return nil
	}
}

// WithMaxResponseBytes sets the maximum size of a buffered successful response.
func WithMaxResponseBytes(limit int64) Option {
	return func(config *clientConfig) error {
		if limit <= 0 {
			return errors.New("vmon: maximum response size must be positive")
		}
		config.maxResponseBytes = limit
		return nil
	}
}

// Client is a client for one vmon API endpoint.
type Client struct {
	baseURL          *url.URL
	httpClient       *http.Client
	token            string
	userAgent        string
	maxResponseBytes int64
}

// NewClient constructs a client for an absolute HTTP or HTTPS API base URL.
func NewClient(baseURL string, options ...Option) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("vmon: parse base URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("vmon: unsupported base URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("vmon: base URL must include a host")
	}
	if parsed.User != nil {
		return nil, errors.New("vmon: base URL must not include user information")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("vmon: base URL must not include a query or fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = strings.TrimRight(parsed.RawPath, "/")

	config := clientConfig{
		httpClient:       &http.Client{},
		maxResponseBytes: defaultMaxResponseBytes,
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("vmon: nil client option")
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return &Client{
		baseURL:          parsed,
		httpClient:       config.httpClient,
		token:            config.token,
		userAgent:        config.userAgent,
		maxResponseBytes: config.maxResponseBytes,
	}, nil
}

// APIError is a structured error returned by the vmon API or a WebSocket protocol frame.
type APIError struct {
	// StatusCode is the HTTP status, or zero for an error delivered after a WebSocket upgrade.
	StatusCode int
	// Code is the daemon's machine-readable error code.
	Code string
	// Message is the daemon's human-readable error message.
	Message string
	// Truncated reports whether the HTTP error body exceeded the bounded read limit.
	Truncated bool
}

// Error implements error.
func (err *APIError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.StatusCode != 0 {
		return fmt.Sprintf("vmon API error %d (%s): %s", err.StatusCode, err.Code, err.Message)
	}
	return fmt.Sprintf("vmon API error (%s): %s", err.Code, err.Message)
}

// ProtocolError reports a response that did not conform to the vmon protocol.
type ProtocolError struct {
	// Operation identifies the operation whose response was invalid.
	Operation string
	// Message describes the protocol violation.
	Message string
	// Err is an optional underlying decoding error.
	Err error
}

// Error implements error.
func (err *ProtocolError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Operation == "" {
		return "vmon protocol error: " + err.Message
	}
	return "vmon protocol error during " + err.Operation + ": " + err.Message
}

// Unwrap exposes the underlying decoding error, if any.
func (err *ProtocolError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// ResponseTooLargeError reports that a buffered successful response exceeded its configured limit.
type ResponseTooLargeError struct {
	// Limit is the configured response limit in bytes.
	Limit int64
}

// Error implements error.
func (err *ResponseTooLargeError) Error() string {
	return fmt.Sprintf("vmon: response exceeds %d-byte limit", err.Limit)
}

func (client *Client) endpoint(escapedPath string, query url.Values) (string, error) {
	if !strings.HasPrefix(escapedPath, "/") {
		return "", errors.New("vmon: endpoint path must be absolute")
	}
	endpoint := *client.baseURL
	basePath := strings.TrimRight(endpoint.EscapedPath(), "/")
	fullPath := basePath + escapedPath
	decodedPath, err := url.PathUnescape(fullPath)
	if err != nil {
		return "", fmt.Errorf("vmon: build endpoint URL: %w", err)
	}
	endpoint.Path = decodedPath
	endpoint.RawPath = fullPath
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func escapePathSegment(value string) string {
	switch value {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return url.PathEscape(value)
	}
}

func escapeRestPath(value string) string {
	value = strings.TrimLeft(value, "/")
	if value == "" {
		return ""
	}
	parts := strings.Split(value, "/")
	for index := range parts {
		parts[index] = escapePathSegment(parts[index])
	}
	return strings.Join(parts, "/")
}

func cloneValues(values url.Values) url.Values {
	if values == nil {
		return make(url.Values)
	}
	cloned := make(url.Values, len(values))
	for key, entries := range values {
		cloned[key] = append([]string(nil), entries...)
	}
	return cloned
}

func requireIdentifier(kind, value string) error {
	if value == "" {
		return fmt.Errorf("vmon: %s must not be empty", kind)
	}
	return nil
}

func (client *Client) newRequest(
	ctx context.Context,
	method string,
	escapedPath string,
	query url.Values,
	body io.Reader,
	contentType string,
) (*http.Request, error) {
	endpoint, err := client.endpoint(escapedPath, query)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("vmon: create HTTP request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	client.applyHeaders(request.Header)
	return request, nil
}

func (client *Client) applyHeaders(header http.Header) {
	if client.token != "" {
		header.Set("Authorization", "Bearer "+client.token)
	}
	if client.userAgent != "" {
		header.Set("User-Agent", client.userAgent)
	}
}

func (client *Client) do(request *http.Request) (*http.Response, error) {
	response, err := client.httpClient.Do(request)
	if err != nil && response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		if contextErr := request.Context().Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, fmt.Errorf("vmon: HTTP request failed: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, apiErrorFromResponse(response)
	}
	return response, nil
}

func (client *Client) doJSON(
	ctx context.Context,
	method string,
	escapedPath string,
	query url.Values,
	body any,
	out any,
) error {
	var reader io.Reader
	contentType := ""
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("vmon: encode request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
		contentType = "application/json"
	}
	request, err := client.newRequest(ctx, method, escapedPath, query, reader, contentType)
	if err != nil {
		return err
	}
	response, err := client.do(request)
	if err != nil {
		return err
	}
	if out == nil {
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32<<10))
		return nil
	}
	encoded, err := client.readResponse(response)
	if err != nil {
		return err
	}
	if len(encoded) == 0 {
		return &ProtocolError{Operation: method + " " + escapedPath, Message: "empty JSON response"}
	}
	if err := json.Unmarshal(encoded, out); err != nil {
		return &ProtocolError{
			Operation: method + " " + escapedPath,
			Message:   "invalid JSON response",
			Err:       err,
		}
	}
	return nil
}

func (client *Client) readResponse(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	return readLimited(response.Body, client.maxResponseBytes)
}

func readLimited(reader io.Reader, limit int64) ([]byte, error) {
	limited := &io.LimitedReader{R: reader, N: limit + 1}
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("vmon: read response body: %w", err)
	}
	if int64(len(data)) > limit {
		return nil, &ResponseTooLargeError{Limit: limit}
	}
	return data, nil
}

func apiErrorFromResponse(response *http.Response) error {
	if response.Body == nil {
		return parseAPIError(response.StatusCode, nil, false)
	}
	defer response.Body.Close()
	limited := &io.LimitedReader{R: response.Body, N: maxErrorResponseBytes + 1}
	body, readErr := io.ReadAll(limited)
	truncated := int64(len(body)) > maxErrorResponseBytes
	if truncated {
		body = body[:maxErrorResponseBytes]
	}
	apiErr := parseAPIError(response.StatusCode, body, truncated)
	if readErr != nil && apiErr.Message == "" {
		apiErr.Message = "failed to read error response: " + readErr.Error()
	}
	return apiErr
}

func parseAPIError(statusCode int, body []byte, truncated bool) *APIError {
	code := strconv.Itoa(statusCode)
	if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
		code = "unauthorized"
	}
	message := strings.TrimSpace(string(body))
	var envelope struct {
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Detail  json.RawMessage `json:"detail"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		if envelope.Code != "" {
			code = envelope.Code
		}
		if envelope.Message != "" {
			message = envelope.Message
		}
		if len(envelope.Detail) != 0 {
			var detail struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if json.Unmarshal(envelope.Detail, &detail) == nil {
				if detail.Code != "" {
					code = detail.Code
				}
				if detail.Message != "" {
					message = detail.Message
				}
			} else {
				var detailText string
				if json.Unmarshal(envelope.Detail, &detailText) == nil && detailText != "" {
					message = detailText
				}
			}
		}
	}
	if message == "" {
		if statusCode != 0 {
			message = http.StatusText(statusCode)
		}
		if message == "" {
			message = "vmon API request failed"
		}
	}
	if len(message) > maxErrorMessageBytes {
		message = message[:maxErrorMessageBytes]
		truncated = true
	}
	return &APIError{
		StatusCode: statusCode,
		Code:       code,
		Message:    message,
		Truncated:  truncated,
	}
}
