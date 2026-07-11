package vmon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	ws "github.com/coder/websocket"
)

const (
	rosterTTL        = 60 * time.Second
	endpointCooldown = 5 * time.Second
)

// DriverRequest describes one HTTP operation and its optional endpoint affinity.
type DriverRequest struct {
	Method   string
	Path     string
	Query    url.Values
	JSON     any
	Content  []byte
	Headers  http.Header
	Stream   bool
	Endpoint string
}

// EndpointInfo is a point-in-time view of one seed or discovered endpoint.
type EndpointInfo struct {
	URL     string
	Healthy bool
	Source  string
}

// Driver is the transport seam used by Client.
type Driver interface {
	Do(context.Context, DriverRequest) (*http.Response, string, error)
	Dial(context.Context, string, url.Values, string) (*WebSocketConn, string, error)
	Endpoints() []EndpointInfo
	Refresh(context.Context, bool) error
	Close() error
}

// TransportError reports a failure to reach an endpoint. Only this error drives failover.
type TransportError struct {
	Endpoint string
	Err      error
}

// Error returns the string representation of the TransportError.
func (err *TransportError) Error() string {
	if err == nil {
		return "<nil>"
	}
	message := "vmon transport failed"
	if err.Endpoint != "" {
		message += " for " + err.Endpoint
	}
	if err.Err != nil {
		message += ": " + err.Err.Error()
	}
	return message
}

// Unwrap returns the underlying error, if any.
func (err *TransportError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

type rosterEntry struct {
	url           string
	source        string
	healthy       bool
	cooldownUntil time.Time
}

type endpointTransport struct {
	client       *http.Client
	streamClient *http.Client
	uds          string
}

type meshDriverOptions struct {
	httpClient       *http.Client
	token            string
	userAgent        string
	maxResponseBytes int64
	now              func() time.Time
}

type meshDriverOption func(*meshDriverOptions) error

func withMeshHTTPClient(client *http.Client) meshDriverOption {
	return func(options *meshDriverOptions) error {
		if client == nil {
			return errors.New("vmon: HTTP client must not be nil")
		}
		options.httpClient = client
		return nil
	}
}

func withMeshToken(token string) meshDriverOption {
	return func(options *meshDriverOptions) error {
		options.token = token
		return nil
	}
}

func withMeshUserAgent(userAgent string) meshDriverOption {
	return func(options *meshDriverOptions) error {
		options.userAgent = userAgent
		return nil
	}
}

func withMeshMaxResponseBytes(limit int64) meshDriverOption {
	return func(options *meshDriverOptions) error {
		if limit <= 0 {
			return errors.New("vmon: maximum response size must be positive")
		}
		options.maxResponseBytes = limit
		return nil
	}
}

func withMeshClock(now func() time.Time) meshDriverOption {
	return func(options *meshDriverOptions) error {
		if now == nil {
			return errors.New("vmon: clock must not be nil")
		}
		options.now = now
		return nil
	}
}

type meshDriver struct {
	mu               sync.Mutex
	seeds            map[string]struct{}
	roster           []*rosterEntry
	preferred        string
	transports       map[string]*endpointTransport
	token            string
	userAgent        string
	timeout          time.Duration
	discover         bool
	maxResponseBytes int64
	httpClient       *http.Client
	now              func() time.Time
	lastRefresh      time.Time
	refreshing       bool
	closed           bool
	// meshStatus fetches the mesh roster document over gRPC
	// (SystemService.MeshStatus); installed by newClient.
	meshStatus func(context.Context) ([]byte, error)
}

func newMeshDriver(config DSNConfig, options ...meshDriverOption) (*meshDriver, error) {
	if len(config.Endpoints) == 0 {
		return nil, errors.New("DSN resolved to no endpoints")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	settings := meshDriverOptions{
		httpClient:       &http.Client{},
		token:            config.Token,
		maxResponseBytes: defaultMaxResponseBytes,
		now:              time.Now,
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("vmon: nil mesh driver option")
		}
		if err := option(&settings); err != nil {
			return nil, err
		}
	}
	driver := &meshDriver{
		seeds:            make(map[string]struct{}),
		transports:       make(map[string]*endpointTransport),
		token:            settings.token,
		userAgent:        settings.userAgent,
		timeout:          config.Timeout,
		discover:         config.Discover,
		maxResponseBytes: settings.maxResponseBytes,
		httpClient:       settings.httpClient,
		now:              settings.now,
	}
	for _, raw := range config.Endpoints {
		endpoint, err := normalizeDriverEndpoint(raw)
		if err != nil {
			return nil, err
		}
		if _, exists := driver.seeds[endpoint]; exists {
			continue
		}
		driver.seeds[endpoint] = struct{}{}
		driver.roster = append(driver.roster, &rosterEntry{url: endpoint, source: "seed", healthy: true})
	}
	if len(driver.roster) == 0 {
		return nil, errors.New("DSN resolved to no endpoints")
	}
	driver.preferred = driver.roster[0].url
	return driver, nil
}

func (driver *meshDriver) Do(ctx context.Context, request DriverRequest) (*http.Response, string, error) {
	return driver.do(ctx, request, true)
}

func (driver *meshDriver) do(ctx context.Context, request DriverRequest, triggerRefresh bool) (*http.Response, string, error) {
	body, err := requestBody(request)
	if err != nil {
		return nil, "", err
	}
	candidates := driver.candidates(request.Endpoint)
	var lastTransport *TransportError
	failedOver := false
	for _, endpoint := range candidates {
		response, requestErr := driver.doEndpoint(ctx, endpoint, request, body)
		if requestErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, "", ctxErr
			}
			var transportErr *TransportError
			if !errors.As(requestErr, &transportErr) {
				return nil, "", requestErr
			}
			lastTransport = transportErr
			failedOver = true
			driver.markFailed(endpoint)
			continue
		}
		driver.markSuccess(endpoint)
		if triggerRefresh {
			driver.refreshAfterRequest(ctx, failedOver)
		}
		return response, endpoint, nil
	}
	if lastTransport != nil {
		return nil, "", lastTransport
	}
	return nil, "", &TransportError{Err: errors.New("no vmon endpoints are currently available")}
}

func requestBody(request DriverRequest) ([]byte, error) {
	if request.JSON != nil && request.Content != nil {
		return nil, errors.New("vmon: request cannot contain both JSON and content")
	}
	if request.JSON != nil {
		encoded, err := json.Marshal(request.JSON)
		if err != nil {
			return nil, fmt.Errorf("vmon: encode request JSON: %w", err)
		}
		return encoded, nil
	}
	return request.Content, nil
}

func (driver *meshDriver) doEndpoint(ctx context.Context, endpoint string, request DriverRequest, body []byte) (*http.Response, error) {
	transport, err := driver.transport(endpoint)
	if err != nil {
		return nil, err
	}
	requestURL, err := endpointURL(endpoint, request.Path, request.Query, false)
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, request.Method, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("vmon: create HTTP request: %w", err)
	}
	httpRequest.Header = request.Headers.Clone()
	if httpRequest.Header == nil {
		httpRequest.Header = make(http.Header)
	}
	httpRequest.Header.Set("Accept", "application/json")
	if request.JSON != nil && httpRequest.Header.Get("Content-Type") == "" {
		httpRequest.Header.Set("Content-Type", "application/json")
	}
	driver.applyHeaders(httpRequest.Header)
	httpClient := transport.client
	if request.Stream {
		httpClient = transport.streamClient
	}
	response, err := httpClient.Do(httpRequest)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, &TransportError{Endpoint: endpoint, Err: err}
	}
	return response, nil
}

func (driver *meshDriver) Dial(ctx context.Context, path string, query url.Values, hint string) (*WebSocketConn, string, error) {
	candidates := driver.candidates(hint)
	var lastTransport *TransportError
	failedOver := false
	for _, endpoint := range candidates {
		transport, err := driver.transport(endpoint)
		if err != nil {
			return nil, "", err
		}
		websocketURL, err := endpointURL(endpoint, path, query, true)
		if err != nil {
			return nil, "", err
		}
		header := make(http.Header)
		driver.applyHeaders(header)
		connection, response, dialErr := ws.Dial(ctx, websocketURL, &ws.DialOptions{HTTPClient: transport.client, HTTPHeader: header})
		if dialErr != nil {
			if connection != nil {
				_ = connection.CloseNow()
			}
			if response != nil {
				driver.markSuccess(endpoint)
				return nil, "", apiErrorFromResponse(response)
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, "", ctxErr
			}
			lastTransport = &TransportError{Endpoint: endpoint, Err: dialErr}
			failedOver = true
			driver.markFailed(endpoint)
			continue
		}
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		connection.SetReadLimit(driver.maxResponseBytes)
		driver.markSuccess(endpoint)
		driver.refreshAfterRequest(ctx, failedOver)
		return &WebSocketConn{conn: connection}, endpoint, nil
	}
	if lastTransport != nil {
		return nil, "", lastTransport
	}
	return nil, "", &TransportError{Err: errors.New("no vmon endpoints are currently available")}
}

func (driver *meshDriver) Endpoints() []EndpointInfo {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	now := driver.now()
	result := make([]EndpointInfo, 0, len(driver.roster))
	for _, entry := range driver.roster {
		healthy := entry.healthy || !now.Before(entry.cooldownUntil)
		result = append(result, EndpointInfo{URL: entry.url, Healthy: healthy, Source: entry.source})
	}
	return result
}

func (driver *meshDriver) Refresh(ctx context.Context, force bool) error {
	if !driver.discover {
		return nil
	}
	now := driver.now()
	driver.mu.Lock()
	if driver.closed || driver.refreshing || (!force && !driver.lastRefresh.IsZero() && now.Sub(driver.lastRefresh) < rosterTTL) {
		driver.mu.Unlock()
		return nil
	}
	driver.refreshing = true
	driver.mu.Unlock()
	defer func() {
		driver.mu.Lock()
		driver.lastRefresh = driver.now()
		driver.refreshing = false
		driver.mu.Unlock()
	}()

	driver.mu.Lock()
	meshStatus := driver.meshStatus
	driver.mu.Unlock()
	if meshStatus == nil {
		return nil
	}
	raw, err := meshStatus(ctx)
	if err != nil {
		return nil
	}
	var status struct {
		Self  map[string]any   `json:"self"`
		Peers []map[string]any `json:"peers"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil
	}
	advertised := make([]string, 0, 1+len(status.Peers))
	if value, ok := status.Self["advertise"].(string); ok {
		advertised = append(advertised, value)
	}
	for _, peer := range status.Peers {
		if value, ok := peer["advertise"].(string); ok {
			advertised = append(advertised, value)
		}
	}
	driver.mergeDiscovered(advertised)
	return nil
}

func (driver *meshDriver) Close() error {
	driver.mu.Lock()
	if driver.closed {
		driver.mu.Unlock()
		return nil
	}
	driver.closed = true
	transports := make([]*endpointTransport, 0, len(driver.transports))
	for _, transport := range driver.transports {
		transports = append(transports, transport)
	}
	driver.transports = make(map[string]*endpointTransport)
	driver.mu.Unlock()
	for _, transport := range transports {
		transport.client.CloseIdleConnections()
	}
	return nil
}

func (driver *meshDriver) refreshAfterRequest(ctx context.Context, failedOver bool) {
	driver.mu.Lock()
	stale := driver.lastRefresh.IsZero() || driver.now().Sub(driver.lastRefresh) >= rosterTTL
	driver.mu.Unlock()
	if failedOver || stale {
		_ = driver.Refresh(ctx, failedOver)
	}
}

func (driver *meshDriver) candidates(hint string) []string {
	if hint != "" {
		if normalized, err := normalizeDriverEndpoint(hint); err == nil {
			hint = normalized
		} else {
			hint = ""
		}
	}
	now := driver.now()
	driver.mu.Lock()
	defer driver.mu.Unlock()
	ordered := make([]*rosterEntry, 0, len(driver.roster))
	for _, target := range []string{hint, driver.preferred} {
		if target == "" {
			continue
		}
		for _, entry := range driver.roster {
			if entry.url == target && !containsRosterEntry(ordered, entry) {
				ordered = append(ordered, entry)
			}
		}
	}
	for _, entry := range driver.roster {
		if !containsRosterEntry(ordered, entry) {
			ordered = append(ordered, entry)
		}
	}
	available := make([]string, 0, len(ordered))
	for _, entry := range ordered {
		if !now.Before(entry.cooldownUntil) {
			available = append(available, entry.url)
		}
	}
	if len(available) != 0 {
		return available
	}
	if len(ordered) == 0 {
		return nil
	}
	earliest := ordered[0]
	for _, entry := range ordered[1:] {
		if entry.cooldownUntil.Before(earliest.cooldownUntil) {
			earliest = entry
		}
	}
	return []string{earliest.url}
}

func containsRosterEntry(entries []*rosterEntry, target *rosterEntry) bool {
	for _, entry := range entries {
		if entry == target {
			return true
		}
	}
	return false
}

func (driver *meshDriver) transport(endpoint string) (*endpointTransport, error) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.closed {
		return nil, &TransportError{Endpoint: endpoint, Err: errors.New("mesh driver is closed")}
	}
	transport := driver.transports[endpoint]
	if transport == nil {
		transport = newEndpointTransport(endpoint, driver.httpClient, driver.timeout)
		driver.transports[endpoint] = transport
	}
	return transport, nil
}

func newEndpointTransport(endpoint string, template *http.Client, timeout time.Duration) *endpointTransport {
	client := *template
	client.Timeout = timeout
	streamClient := client
	streamClient.Timeout = 0
	result := &endpointTransport{client: &client, streamClient: &streamClient}
	if strings.HasPrefix(endpoint, "vmon+unix://") {
		result.uds = strings.TrimPrefix(endpoint, "vmon+unix://")
		var transport *http.Transport
		if original, ok := template.Transport.(*http.Transport); ok {
			transport = original.Clone()
		} else {
			transport = http.DefaultTransport.(*http.Transport).Clone()
		}
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", result.uds)
		}
		client.Transport = transport
		streamClient.Transport = transport
	}
	return result
}

func (driver *meshDriver) markFailed(endpoint string) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	for _, entry := range driver.roster {
		if entry.url == endpoint {
			entry.healthy = false
			entry.cooldownUntil = driver.now().Add(endpointCooldown)
			return
		}
	}
}

func (driver *meshDriver) markSuccess(endpoint string) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	for _, entry := range driver.roster {
		if entry.url == endpoint {
			entry.healthy = true
			entry.cooldownUntil = time.Time{}
			driver.preferred = endpoint
			return
		}
	}
}

func (driver *meshDriver) mergeDiscovered(advertised []string) {
	normalized := make([]string, 0, len(advertised))
	seen := make(map[string]struct{})
	for _, raw := range advertised {
		endpoint, err := normalizeDriverEndpoint(raw)
		if err != nil {
			continue
		}
		if _, exists := seen[endpoint]; exists {
			continue
		}
		seen[endpoint] = struct{}{}
		normalized = append(normalized, endpoint)
	}
	driver.mu.Lock()
	keep := make(map[string]struct{}, len(driver.seeds)+len(normalized))
	for endpoint := range driver.seeds {
		keep[endpoint] = struct{}{}
	}
	for _, endpoint := range normalized {
		keep[endpoint] = struct{}{}
	}
	previous := make(map[string]*rosterEntry, len(driver.roster))
	for _, entry := range driver.roster {
		previous[entry.url] = entry
	}
	roster := make([]*rosterEntry, 0, len(keep))
	for _, entry := range driver.roster {
		if _, seed := driver.seeds[entry.url]; seed {
			roster = append(roster, entry)
		}
	}
	for _, endpoint := range normalized {
		if _, seed := driver.seeds[endpoint]; seed {
			continue
		}
		entry := previous[endpoint]
		if entry == nil {
			entry = &rosterEntry{url: endpoint, source: "discovered", healthy: true}
		}
		roster = append(roster, entry)
	}
	var removed []*endpointTransport
	for endpoint, transport := range driver.transports {
		if _, exists := keep[endpoint]; !exists {
			removed = append(removed, transport)
			delete(driver.transports, endpoint)
		}
	}
	driver.roster = roster
	if _, exists := keep[driver.preferred]; !exists && len(roster) != 0 {
		driver.preferred = roster[0].url
	}
	driver.mu.Unlock()
	for _, transport := range removed {
		transport.client.CloseIdleConnections()
	}
}

func (driver *meshDriver) applyHeaders(header http.Header) {
	if driver.token != "" {
		header.Set("Authorization", "Bearer "+driver.token)
	}
	if driver.userAgent != "" {
		header.Set("User-Agent", driver.userAgent)
	}
}

func normalizeDriverEndpoint(value string) (string, error) {
	if strings.HasPrefix(value, "vmon+unix://") {
		parsed, err := url.Parse(value)
		if err != nil || parsed.Host != "" {
			return "", errors.New("vmon+unix endpoint must not contain a host")
		}
		path, err := url.PathUnescape(parsed.EscapedPath())
		if err != nil || !strings.HasPrefix(path, "/") {
			return "", errors.New("vmon+unix endpoint requires an absolute socket path")
		}
		return "vmon+unix://" + path, nil
	}
	return normalizeHTTPEndpoint(value, "")
}

func endpointURL(endpoint, path string, query url.Values, websocket bool) (string, error) {
	if !strings.HasPrefix(path, "/") {
		return "", errors.New("vmon: endpoint path must be absolute")
	}
	var parsed *url.URL
	var err error
	if strings.HasPrefix(endpoint, "vmon+unix://") {
		parsed, err = url.Parse("http://vmon")
	} else {
		parsed, err = url.Parse(endpoint)
	}
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(parsed.EscapedPath(), "/")
	fullPath := basePath + path
	decoded, err := url.PathUnescape(fullPath)
	if err != nil {
		return "", err
	}
	parsed.Path = decoded
	parsed.RawPath = fullPath
	parsed.RawQuery = query.Encode()
	if websocket {
		if parsed.Scheme == "http" {
			parsed.Scheme = "ws"
		} else {
			parsed.Scheme = "wss"
		}
	}
	return parsed.String(), nil
}
