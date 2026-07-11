package vmon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
)

const maxEventBytes = 1 << 20

// Health retrieves the health status of the vmon daemon.
func (client *Client) Health(ctx context.Context) (Health, error) {
	var out Health
	err := client.doJSON(ctx, http.MethodGet, "/healthz", nil, nil, &out)
	return out, err
}

// Info retrieves information about the vmon server.
func (client *Client) Info(ctx context.Context) (ServerInfo, error) {
	var out ServerInfo
	err := client.doJSON(ctx, http.MethodGet, "/v1/info", nil, nil, &out)
	return out, err
}

// Metrics retrieves Prometheus metrics from the daemon.
func (client *Client) Metrics(ctx context.Context) (string, error) {
	response, _, err := client.request(ctx, DriverRequest{Method: http.MethodGet, Path: "/metrics", Headers: http.Header{"Accept": {"text/plain"}}})
	if err != nil {
		return "", err
	}
	body, err := client.readResponse(response)
	return string(body), err
}

// OpenAPI retrieves the OpenAPI JSON spec from the server.
func (client *Client) OpenAPI(ctx context.Context) (json.RawMessage, error) {
	response, _, err := client.request(ctx, DriverRequest{Method: http.MethodGet, Path: "/v1/openapi.json"})
	if err != nil {
		return nil, err
	}
	body, err := client.readResponse(response)
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, &ProtocolError{Operation: "get OpenAPI", Message: "invalid JSON response"}
	}
	return json.RawMessage(body), nil
}

// EventStream incrementally decodes daemon lifecycle events.
type EventStream struct {
	body      io.ReadCloser
	scanner   *bufio.Scanner
	readMu    sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// Events opens a stream to receive lifecycle events from the daemon.
func (client *Client) Events(ctx context.Context) (*EventStream, error) {
	response, _, err := client.request(ctx, DriverRequest{Method: http.MethodGet, Path: "/v1/events", Stream: true, Headers: http.Header{"Accept": {"text/event-stream"}}})
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), maxEventBytes)
	return &EventStream{body: response.Body, scanner: scanner}, nil
}

// Next block-reads and decodes the next event from the stream.
func (stream *EventStream) Next(ctx context.Context) (Event, error) {
	if stream == nil || stream.body == nil {
		return nil, errors.New("vmon: event stream is not open")
	}
	stream.readMu.Lock()
	defer stream.readMu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	defer stop()
	var data []byte
	for stream.scanner.Scan() {
		line := strings.TrimSuffix(stream.scanner.Text(), "\r")
		if line == "" {
			if len(data) == 0 {
				continue
			}
			return decodeEvent(data)
		}
		if strings.HasPrefix(line, "data:") {
			part := strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			if len(data) > 0 {
				data = append(data, '\n')
			}
			if len(data)+len(part) > maxEventBytes {
				_ = stream.Close()
				return nil, &ProtocolError{Operation: "read events", Message: "event exceeds size limit"}
			}
			data = append(data, part...)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := stream.scanner.Err(); err != nil {
		return nil, fmt.Errorf("vmon: read event stream: %w", err)
	}
	if len(data) > 0 {
		return decodeEvent(data)
	}
	return nil, io.EOF
}

// Close terminates the event stream.
func (stream *EventStream) Close() error {
	if stream == nil {
		return nil
	}
	stream.closeOnce.Do(func() {
		if stream.body != nil {
			stream.closeErr = stream.body.Close()
		}
	})
	return stream.closeErr
}
func decodeEvent(data []byte) (Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, &ProtocolError{Operation: "read events", Message: "invalid event JSON", Err: err}
	}
	if event == nil {
		return nil, &ProtocolError{Operation: "read events", Message: "event is not an object"}
	}
	return event, nil
}

// SandboxService manages sandbox resources.
type SandboxService struct{ client *Client }

// Create provisions a new sandbox.
func (service *SandboxService) Create(ctx context.Context, request SandboxCreateRequest) (*Sandbox, error) {
	response, endpoint, err := service.client.request(ctx, DriverRequest{Method: http.MethodPost, Path: "/v1/sandboxes", JSON: request})
	if err != nil {
		return nil, err
	}
	var sandbox Sandbox
	if err = service.client.decodeJSONResponse(response, "create sandbox", &sandbox); err != nil {
		return nil, err
	}
	return service.client.bindSandbox(&sandbox, endpoint, "create sandbox")
}

// Get retrieves metadata of an existing sandbox by ID.
func (service *SandboxService) Get(ctx context.Context, id string) (*Sandbox, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	response, endpoint, err := service.client.request(ctx, DriverRequest{Method: http.MethodGet, Path: sandboxPath(id)})
	if err != nil {
		return nil, err
	}
	var sandbox Sandbox
	if err = service.client.decodeJSONResponse(response, "get sandbox", &sandbox); err != nil {
		return nil, err
	}
	return service.client.bindSandbox(&sandbox, endpoint, "get sandbox")
}

// Ref returns a local reference to a sandbox without calling the server.
func (service *SandboxService) Ref(id string) *Sandbox {
	sandbox := &Sandbox{ID: id, client: service.client}
	sandbox.initServices()
	return sandbox
}

// List lists sandboxes matching the optional filters.
func (service *SandboxService) List(ctx context.Context, options ...SandboxListOptions) ([]*Sandbox, error) {
	var filter SandboxListOptions
	if len(options) > 0 {
		filter = options[0]
	}
	endpoints := service.client.driver.Endpoints()
	healthy := make([]EndpointInfo, 0, len(endpoints))
	for _, entry := range endpoints {
		if entry.Healthy {
			healthy = append(healthy, entry)
		}
	}
	if len(healthy) == 0 {
		healthy = append(healthy, EndpointInfo{})
	}

	type listResult struct {
		index    int
		endpoint string
		rows     []*Sandbox
		err      error
	}
	results := make(chan listResult, len(healthy))
	query := makeURLValues(filter.Tags)
	for index, entry := range healthy {
		go func(index int, entry EndpointInfo) {
			response, endpoint, err := service.client.request(ctx, DriverRequest{
				Method:   http.MethodGet,
				Path:     "/v1/sandboxes",
				Query:    query,
				Endpoint: entry.URL,
			})
			if err != nil {
				results <- listResult{index: index, err: err}
				return
			}
			body, err := service.client.readResponse(response)
			if err == nil {
				var rows []*Sandbox
				rows, err = decodeSandboxRows(body)
				results <- listResult{index: index, endpoint: endpoint, rows: rows, err: err}
				return
			}
			results <- listResult{index: index, err: err}
		}(index, entry)
	}

	ordered := make([]listResult, len(healthy))
	for range healthy {
		result := <-results
		ordered[result.index] = result
	}
	values := make([]*Sandbox, 0)
	seen := make(map[string]bool)
	successes := 0
	var lastTransport error
	for _, result := range ordered {
		if result.err != nil {
			var transportErr *TransportError
			if errors.As(result.err, &transportErr) {
				lastTransport = result.err
				continue
			}
			return nil, result.err
		}
		successes++
		for _, sandbox := range result.rows {
			if sandbox == nil || seen[sandbox.ID] || filter.Node != "" && sandbox.Node != filter.Node {
				continue
			}
			bound, err := service.client.bindSandbox(sandbox, result.endpoint, "list sandboxes")
			if err != nil {
				return nil, err
			}
			seen[sandbox.ID] = true
			values = append(values, bound)
		}
	}
	if successes == 0 && lastTransport != nil {
		return nil, lastTransport
	}
	sort.SliceStable(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values, nil
}
func decodeSandboxRows(body []byte) ([]*Sandbox, error) {
	var wire struct {
		Sandboxes json.RawMessage `json:"sandboxes"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, &ProtocolError{Operation: "list sandboxes", Message: "invalid JSON response", Err: err}
	}
	if wire.Sandboxes == nil {
		return nil, &ProtocolError{Operation: "list sandboxes", Message: "response did not include sandboxes"}
	}
	var rows []*Sandbox
	if err := json.Unmarshal(wire.Sandboxes, &rows); err != nil {
		return nil, &ProtocolError{Operation: "list sandboxes", Message: "sandboxes is not an array", Err: err}
	}
	return rows, nil
}

// SnapshotService manages filesystem and memory snapshots.
type SnapshotService struct{ client *Client }

// List retrieves the names of all snapshots.
func (s *SnapshotService) List(ctx context.Context) ([]string, error) {
	var out struct {
		Snapshots []string `json:"snapshots"`
	}
	if err := s.client.doJSON(ctx, http.MethodGet, "/v1/snapshots", nil, nil, &out); err != nil {
		return nil, err
	}
	if out.Snapshots == nil {
		return nil, &ProtocolError{Operation: "list snapshots", Message: "response did not include snapshots"}
	}
	return out.Snapshots, nil
}

// Restore reverts a sandbox to a specific snapshot.
func (s *SnapshotService) Restore(ctx context.Context, name string, request RestoreRequest) (*Sandbox, error) {
	response, endpoint, err := s.client.request(ctx, DriverRequest{Method: http.MethodPost, Path: "/v1/snapshots/" + escapePathSegment(name) + "/restore", JSON: request})
	if err != nil {
		return nil, err
	}
	var out Sandbox
	if err = s.client.decodeJSONResponse(response, "restore snapshot", &out); err != nil {
		return nil, err
	}
	return s.client.bindSandbox(&out, endpoint, "restore snapshot")
}

// Fork creates one or more clones from a snapshot.
func (s *SnapshotService) Fork(ctx context.Context, name string, request ForkRequest) ([]*Sandbox, error) {
	response, endpoint, err := s.client.request(ctx, DriverRequest{Method: http.MethodPost, Path: "/v1/snapshots/" + escapePathSegment(name) + "/fork", JSON: request})
	if err != nil {
		return nil, err
	}
	var out struct {
		Clones []*Sandbox `json:"clones"`
	}
	if err = s.client.decodeJSONResponse(response, "fork snapshot", &out); err != nil {
		return nil, err
	}
	if out.Clones == nil {
		return nil, &ProtocolError{Operation: "fork snapshot", Message: "response did not include clones"}
	}
	rows := out.Clones
	for _, sandbox := range rows {
		if _, err = s.client.bindSandbox(sandbox, endpoint, "fork snapshot"); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// VolumeService manages persistent storage volumes.
type VolumeService struct{ client *Client }

// List retrieves all persistent volumes.
func (s *VolumeService) List(ctx context.Context) ([]Volume, error) {
	var out struct {
		Volumes []string `json:"volumes"`
	}
	if err := s.client.doJSON(ctx, http.MethodGet, "/v1/volumes", nil, nil, &out); err != nil {
		return nil, err
	}
	if out.Volumes == nil {
		return nil, &ProtocolError{Operation: "list volumes", Message: "response did not include volumes"}
	}
	values := make([]Volume, 0, len(out.Volumes))
	for _, name := range out.Volumes {
		value, err := NewVolume(name)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

// Create provisions a new persistent volume.
func (s *VolumeService) Create(ctx context.Context, name string) (Volume, error) {
	value, err := NewVolume(name)
	if err != nil {
		return Volume{}, err
	}
	response, _, err := s.client.request(ctx, DriverRequest{Method: http.MethodPut, Path: "/v1/volumes/" + escapePathSegment(name)})
	if err == nil {
		response.Body.Close()
	}
	return value, err
}

// Delete removes a persistent volume by name.
func (s *VolumeService) Delete(ctx context.Context, name string) error {
	if _, err := NewVolume(name); err != nil {
		return err
	}
	response, _, err := s.client.request(ctx, DriverRequest{Method: http.MethodDelete, Path: "/v1/volumes/" + escapePathSegment(name)})
	if err == nil {
		response.Body.Close()
	}
	return err
}

// PoolService manages sandbox resource pools.
type PoolService struct{ client *Client }

// List retrieves resource usage stats for all pools.
func (s *PoolService) List(ctx context.Context) (map[string]PoolStats, error) {
	var out map[string]PoolStats
	if err := s.client.doJSON(ctx, http.MethodGet, "/v1/pools", nil, nil, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, &ProtocolError{Operation: "list pools", Message: "response is not an object"}
	}
	return out, nil
}

// Set updates the resource allocation or parameters for a pool.
func (s *PoolService) Set(ctx context.Context, reference string, request PoolRequest) (PoolStats, error) {
	var out PoolStats
	err := s.client.doJSON(ctx, http.MethodPut, "/v1/pools/"+escapePathSegment(reference), nil, request, &out)
	return out, err
}

// Delete removes a pool allocation.
func (s *PoolService) Delete(ctx context.Context, reference string) error {
	response, _, err := s.client.request(ctx, DriverRequest{Method: http.MethodDelete, Path: "/v1/pools/" + escapePathSegment(reference)})
	if err == nil {
		response.Body.Close()
	}
	return err
}

// Clear removes all configured pools.
func (s *PoolService) Clear(ctx context.Context) error {
	pools, err := s.List(ctx)
	if err != nil {
		return err
	}
	for reference := range pools {
		if err := s.Delete(ctx, reference); err != nil {
			return err
		}
	}
	return nil
}

// MeshService monitors mesh cluster topology.
type MeshService struct{ client *Client }

// Status returns the current mesh membership and replication status.
func (s *MeshService) Status(ctx context.Context) (MeshStatus, error) {
	var out MeshStatus
	err := s.client.doJSON(ctx, http.MethodGet, "/v1/mesh/status", nil, nil, &out)
	return out, err
}

// Nodes lists all known nodes in the mesh topology.
func (s *MeshService) Nodes(ctx context.Context) ([]MeshNode, error) {
	status, err := s.Status(ctx)
	if err != nil {
		return nil, err
	}
	return append([]MeshNode{status.Self}, status.Peers...), nil
}

func makeURLValues(tags map[string]string) url.Values {
	values := make(url.Values)
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values.Add("tag", key+"="+tags[key])
	}
	return values
}
