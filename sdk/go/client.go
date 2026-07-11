package vmon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultMaxResponseBytes int64 = 64 << 20
	maxErrorResponseBytes   int64 = 64 << 10
	maxErrorMessageBytes          = 8 << 10
)

// Option configures Connect or NewClient.
type Option func(*clientConfig)

type clientConfig struct {
	httpClient       *http.Client
	token            string
	userAgent        string
	maxResponseBytes int64
	discovery        *bool
	timeout          time.Duration
}

// WithToken sets the bearer token.
func WithToken(token string) Option { return func(c *clientConfig) { c.token = token } }

// WithHTTPClient sets the HTTP client used by the mesh driver.
func WithHTTPClient(client *http.Client) Option {
	return func(c *clientConfig) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithUserAgent sets the HTTP User-Agent header.
func WithUserAgent(value string) Option { return func(c *clientConfig) { c.userAgent = value } }

// WithMaxResponseBytes limits buffered successful response bodies.
func WithMaxResponseBytes(value int64) Option {
	return func(c *clientConfig) {
		if value > 0 {
			c.maxResponseBytes = value
		}
	}
}

// WithDiscovery overrides DSN mesh discovery.
func WithDiscovery(enabled bool) Option { return func(c *clientConfig) { c.discovery = &enabled } }

// WithTimeout overrides the DSN request timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *clientConfig) {
		if timeout > 0 {
			c.timeout = timeout
		}
	}
}

func defaultClientConfig() clientConfig {
	return clientConfig{httpClient: &http.Client{}, maxResponseBytes: defaultMaxResponseBytes}
}

// Connect parses dsn, constructs its mesh driver, and returns a ready client.
func Connect(dsn string, options ...Option) (*Client, error) {
	config, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	settings := defaultClientConfig()
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if settings.discovery != nil {
		config.Discover = *settings.discovery
	}
	if settings.timeout > 0 {
		config.Timeout = settings.timeout
	}
	driver, err := newMeshDriver(config, withMeshHTTPClient(settings.httpClient), withMeshToken(firstNonempty(settings.token, config.Token)), withMeshUserAgent(settings.userAgent), withMeshMaxResponseBytes(settings.maxResponseBytes))
	if err != nil {
		return nil, err
	}
	return newClient(driver, settings), nil
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// Client is a driver-backed vmon API client.
type Client struct {
	driver           Driver
	maxResponseBytes int64
	Sandboxes        *SandboxService
	Snapshots        *SnapshotService
	Volumes          *VolumeService
	Pools            *PoolService
	Mesh             *MeshService
}

// NewClient binds the service object model to an existing driver.
func NewClient(driver Driver, options ...Option) *Client {
	settings := defaultClientConfig()
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	return newClient(driver, settings)
}

func newClient(driver Driver, settings clientConfig) *Client {
	client := &Client{driver: driver, maxResponseBytes: settings.maxResponseBytes}
	client.Sandboxes = &SandboxService{client: client}
	client.Snapshots = &SnapshotService{client: client}
	client.Volumes = &VolumeService{client: client}
	client.Pools = &PoolService{client: client}
	client.Mesh = &MeshService{client: client}
	return client
}

// Driver returns the transport backing this client.
func (client *Client) Driver() Driver { return client.driver }

// Close releases all resources associated with the client.
func (client *Client) Close() error {
	if client == nil || client.driver == nil {
		return nil
	}
	return client.driver.Close()
}

// APIError is a structured daemon or WebSocket error.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Truncated  bool
}

// Error returns the string representation of the API error.
func (err *APIError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.StatusCode != 0 {
		return fmt.Sprintf("vmon API error %d (%s): %s", err.StatusCode, err.Code, err.Message)
	}
	return fmt.Sprintf("vmon API error (%s): %s", err.Code, err.Message)
}

// ProtocolError represents a failure to communicate with the daemon or parse its response.
type ProtocolError struct {
	Operation string
	Message   string
	Err       error
}

// Error returns the string representation of the protocol error.
func (err *ProtocolError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Operation == "" {
		return "vmon protocol error: " + err.Message
	}
	return "vmon protocol error during " + err.Operation + ": " + err.Message
}

// Unwrap returns the underlying error, if any.
func (err *ProtocolError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// ResponseTooLargeError indicates that the server response exceeded the configured byte limit.
type ResponseTooLargeError struct{ Limit int64 }

// Error returns the string representation of the ResponseTooLargeError.
func (err *ResponseTooLargeError) Error() string {
	return fmt.Sprintf("vmon: response exceeds %d-byte limit", err.Limit)
}

func escapePathSegment(value string) string {
	if value == "." {
		return "%2E"
	}
	if value == ".." {
		return "%2E%2E"
	}
	return url.PathEscape(value)
}
func escapeRestPath(value string) string {
	value = strings.TrimLeft(value, "/")
	if value == "" {
		return ""
	}
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = escapePathSegment(parts[i])
	}
	return strings.Join(parts, "/")
}
func cloneValues(values url.Values) url.Values {
	result := make(url.Values, len(values))
	for key, entries := range values {
		result[key] = append([]string(nil), entries...)
	}
	return result
}
func requireIdentifier(kind, value string) error {
	if value == "" {
		return fmt.Errorf("vmon: %s must not be empty", kind)
	}
	return nil
}

func (client *Client) request(ctx context.Context, request DriverRequest) (*http.Response, string, error) {
	if client == nil || client.driver == nil {
		return nil, "", errors.New("vmon: client has no driver")
	}
	response, endpoint, err := client.driver.Do(ctx, request)
	if err != nil {
		return nil, "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, endpoint, apiErrorFromResponse(response)
	}
	return response, endpoint, nil
}
func (client *Client) doJSON(ctx context.Context, method, path string, query url.Values, body, out any) error {
	response, _, err := client.request(ctx, DriverRequest{Method: method, Path: path, Query: query, JSON: body})
	if err != nil {
		return err
	}
	if out == nil {
		_, err = client.readResponse(response)
		return err
	}
	return client.decodeJSONResponse(response, method+" "+path, out)
}
func (client *Client) decodeJSONResponse(response *http.Response, operation string, out any) error {
	body, err := client.readResponse(response)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return &ProtocolError{Operation: operation, Message: "empty JSON response"}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return &ProtocolError{Operation: operation, Message: "invalid JSON response", Err: err}
	}
	return nil
}
func (client *Client) readResponse(response *http.Response) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, &ProtocolError{Operation: "read response", Message: "response has no body"}
	}
	defer response.Body.Close()
	limit := client.maxResponseBytes
	if limit <= 0 {
		limit = defaultMaxResponseBytes
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, &ResponseTooLargeError{Limit: limit}
	}
	return body, nil
}
func apiErrorFromResponse(response *http.Response) error {
	if response == nil {
		return &APIError{Message: "empty response"}
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxErrorResponseBytes+1))
	truncated := int64(len(body)) > maxErrorResponseBytes
	if truncated {
		body = body[:maxErrorResponseBytes]
	}
	var wire struct {
		Code    string `json:"code"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &wire)
	message := firstNonempty(wire.Message, wire.Error, strings.TrimSpace(string(body)))
	if len(message) > maxErrorMessageBytes {
		message = message[:maxErrorMessageBytes]
		truncated = true
	}
	if readErr != nil && message == "" {
		message = readErr.Error()
	}
	return &APIError{StatusCode: response.StatusCode, Code: wire.Code, Message: message, Truncated: truncated}
}
