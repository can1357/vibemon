package vmon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

const maxEventBytes = 1 << 20

// Health returns the daemon health response.
func (client *Client) Health(ctx context.Context) (Health, error) {
	var health Health
	if err := client.doJSON(ctx, http.MethodGet, "/healthz", nil, nil, &health); err != nil {
		return Health{}, err
	}
	return health, nil
}

// Info returns daemon build, platform, backend, and capability information.
func (client *Client) Info(ctx context.Context) (ServerInfo, error) {
	var info ServerInfo
	if err := client.doJSON(ctx, http.MethodGet, "/v1/info", nil, nil, &info); err != nil {
		return ServerInfo{}, err
	}
	return info, nil
}

// OpenAPISchema returns the daemon's current OpenAPI document as validated JSON.
func (client *Client) OpenAPISchema(ctx context.Context) (json.RawMessage, error) {
	request, err := client.newRequest(ctx, http.MethodGet, "/v1/openapi.json", nil, nil, "")
	if err != nil {
		return nil, err
	}
	response, err := client.do(request)
	if err != nil {
		return nil, err
	}
	body, err := client.readResponse(response)
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, &ProtocolError{Operation: "get OpenAPI schema", Message: "invalid JSON response"}
	}
	return json.RawMessage(body), nil
}

// Metrics returns the daemon's Prometheus exposition text.
func (client *Client) Metrics(ctx context.Context) (string, error) {
	request, err := client.newRequest(ctx, http.MethodGet, "/metrics", nil, nil, "")
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "text/plain")
	response, err := client.do(request)
	if err != nil {
		return "", err
	}
	body, err := client.readResponse(response)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// EventStream incrementally decodes daemon lifecycle events from an SSE response.
type EventStream struct {
	body      io.ReadCloser
	scanner   *bufio.Scanner
	readMu    sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// Events opens the daemon lifecycle event stream bound to ctx.
func (client *Client) Events(ctx context.Context) (*EventStream, error) {
	request, err := client.newRequest(ctx, http.MethodGet, "/v1/events", nil, nil, "")
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "text/event-stream")
	response, err := client.do(request)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), maxEventBytes)
	return &EventStream{body: response.Body, scanner: scanner}, nil
}

// Next waits for and decodes the next lifecycle event.
func (stream *EventStream) Next(ctx context.Context) (Event, error) {
	if stream == nil || stream.body == nil {
		return nil, errors.New("vmon: event stream is not open")
	}
	stream.readMu.Lock()
	defer stream.readMu.Unlock()
	stopCancel := context.AfterFunc(ctx, func() {
		_ = stream.Close()
	})
	defer stopCancel()

	var data []byte
	for stream.scanner.Scan() {
		line := strings.TrimSuffix(stream.scanner.Text(), "\r")
		if line == "" {
			if len(data) == 0 {
				continue
			}
			event, err := decodeEvent(data)
			if err != nil {
				_ = stream.Close()
			}
			return event, err
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		part := strings.TrimPrefix(line, "data:")
		part = strings.TrimPrefix(part, " ")
		if len(data) != 0 {
			data = append(data, '\n')
		}
		if len(data)+len(part) > maxEventBytes {
			_ = stream.Close()
			return nil, &ProtocolError{Operation: "read events", Message: "event exceeds size limit"}
		}
		data = append(data, part...)
	}
	if err := ctx.Err(); err != nil {
		_ = stream.Close()
		return nil, err
	}
	if err := stream.scanner.Err(); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("vmon: read event stream: %w", err)
	}
	if len(data) != 0 {
		event, err := decodeEvent(data)
		if err != nil {
			_ = stream.Close()
		}
		return event, err
	}
	_ = stream.Close()
	return nil, io.EOF
}

// Close closes the event response body and unblocks any pending Next call.
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

// ListSnapshots returns stable snapshot names.
func (client *Client) ListSnapshots(ctx context.Context) ([]string, error) {
	var response struct {
		Snapshots []string `json:"snapshots"`
	}
	if err := client.doJSON(ctx, http.MethodGet, "/v1/snapshots", nil, nil, &response); err != nil {
		return nil, err
	}
	return response.Snapshots, nil
}

// RestoreSnapshot restores one snapshot into a sandbox.
func (client *Client) RestoreSnapshot(
	ctx context.Context,
	name string,
	request RestoreRequest,
) (*Sandbox, error) {
	if err := requireIdentifier("snapshot name", name); err != nil {
		return nil, err
	}
	var sandbox Sandbox
	path := "/v1/snapshots/" + escapePathSegment(name) + "/restore"
	if err := client.doJSON(ctx, http.MethodPost, path, nil, request, &sandbox); err != nil {
		return nil, err
	}
	return client.bindSandbox(&sandbox, "restore snapshot")
}

// ForkSnapshot creates an ordered batch of sandboxes from one snapshot.
func (client *Client) ForkSnapshot(
	ctx context.Context,
	name string,
	request ForkRequest,
) (ForkResult, error) {
	if err := requireIdentifier("snapshot name", name); err != nil {
		return ForkResult{}, err
	}
	var result ForkResult
	path := "/v1/snapshots/" + escapePathSegment(name) + "/fork"
	if err := client.doJSON(ctx, http.MethodPost, path, nil, request, &result); err != nil {
		return ForkResult{}, err
	}
	for _, sandbox := range result.Clones {
		if sandbox == nil {
			return ForkResult{}, &ProtocolError{Operation: "fork snapshot", Message: "clone list contains null"}
		}
		if _, err := client.bindSandbox(sandbox, "fork snapshot"); err != nil {
			return ForkResult{}, err
		}
	}
	return result, nil
}

// ListVolumes returns server-owned persistent volume names.
func (client *Client) ListVolumes(ctx context.Context) ([]string, error) {
	var response struct {
		Volumes []string `json:"volumes"`
	}
	if err := client.doJSON(ctx, http.MethodGet, "/v1/volumes", nil, nil, &response); err != nil {
		return nil, err
	}
	return response.Volumes, nil
}

// CreateVolume ensures a named persistent volume exists and returns its validated value helper.
func (client *Client) CreateVolume(ctx context.Context, name string) (Volume, error) {
	volume, err := NewVolume(name)
	if err != nil {
		return Volume{}, err
	}
	var response okResponse
	path := "/v1/volumes/" + escapePathSegment(name)
	if err := client.doJSON(ctx, http.MethodPut, path, nil, nil, &response); err != nil {
		return Volume{}, err
	}
	if !response.OK {
		return Volume{}, &ProtocolError{Operation: "create volume", Message: "response did not confirm success"}
	}
	return volume, nil
}

// DeleteVolume deletes a named persistent volume.
func (client *Client) DeleteVolume(ctx context.Context, name string) error {
	if _, err := NewVolume(name); err != nil {
		return err
	}
	var response okResponse
	path := "/v1/volumes/" + escapePathSegment(name)
	if err := client.doJSON(ctx, http.MethodDelete, path, nil, nil, &response); err != nil {
		return err
	}
	if !response.OK {
		return &ProtocolError{Operation: "delete volume", Message: "response did not confirm success"}
	}
	return nil
}

// ListPools returns warm-pool statistics keyed by server pool reference.
func (client *Client) ListPools(ctx context.Context) (map[string]PoolStats, error) {
	var pools map[string]PoolStats
	if err := client.doJSON(ctx, http.MethodGet, "/v1/pools", nil, nil, &pools); err != nil {
		return nil, err
	}
	return pools, nil
}

// SetPool sets a server-owned warm-pool size and returns its current statistics.
func (client *Client) SetPool(
	ctx context.Context,
	reference string,
	request PoolRequest,
) (PoolStats, error) {
	if err := requireIdentifier("pool reference", reference); err != nil {
		return PoolStats{}, err
	}
	var stats PoolStats
	path := "/v1/pools/" + escapePathSegment(reference)
	if err := client.doJSON(ctx, http.MethodPut, path, nil, request, &stats); err != nil {
		return PoolStats{}, err
	}
	return stats, nil
}

// DeletePool shuts down and deletes a server-owned warm pool.
func (client *Client) DeletePool(ctx context.Context, reference string) error {
	if err := requireIdentifier("pool reference", reference); err != nil {
		return err
	}
	var response okResponse
	path := "/v1/pools/" + escapePathSegment(reference)
	if err := client.doJSON(ctx, http.MethodDelete, path, nil, nil, &response); err != nil {
		return err
	}
	if !response.OK {
		return &ProtocolError{Operation: "delete pool", Message: "response did not confirm success"}
	}
	return nil
}
