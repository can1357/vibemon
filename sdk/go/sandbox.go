package vmon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func sandboxPath(id string) string { return "/v1/sandboxes/" + escapePathSegment(id) }
func (client *Client) bindSandbox(sandbox *Sandbox, endpoint, operation string) (*Sandbox, error) {
	if sandbox == nil || sandbox.ID == "" {
		return nil, &ProtocolError{Operation: operation, Message: "sandbox response has no id"}
	}
	sandbox.client = client
	sandbox.endpoint = endpoint
	sandbox.initServices()
	return sandbox, nil
}
func (sandbox *Sandbox) initServices() {
	sandbox.Files = &Files{sandbox: sandbox}
	sandbox.Ports = &Ports{sandbox: sandbox}
}
func (sandbox *Sandbox) do(ctx context.Context, request DriverRequest) (*http.Response, error) {
	if sandbox == nil || sandbox.client == nil {
		return nil, errors.New("vmon: sandbox is not bound to a client")
	}
	request.Path = sandboxPath(sandbox.ID) + request.Path
	request.Endpoint = sandbox.endpoint
	execute := func() (*http.Response, error) {
		response, endpoint, err := sandbox.client.request(ctx, request)
		if endpoint != "" {
			sandbox.endpoint = endpoint
		}
		return response, err
	}
	response, err := execute()
	var apiErr *APIError
	if err == nil || !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound || len(sandbox.client.driver.Endpoints()) <= 1 {
		return response, err
	}
	endpoint, resolveErr := sandbox.client.driver.ResolveSandbox(ctx, sandbox.ID, sandbox.endpoint)
	if resolveErr != nil {
		return nil, resolveErr
	}
	sandbox.endpoint = endpoint
	request.Endpoint = endpoint
	return execute()
}
func (sandbox *Sandbox) json(ctx context.Context, method, path string, query url.Values, body, out any) error {
	response, err := sandbox.do(ctx, DriverRequest{Method: method, Path: path, Query: query, JSON: body})
	if err != nil {
		return err
	}
	if out == nil {
		_, err = sandbox.client.readResponse(response)
		return err
	}
	return sandbox.client.decodeJSONResponse(response, method+" "+path, out)
}

// Refresh updates the sandbox state by fetching metadata from the server.
func (sandbox *Sandbox) Refresh(ctx context.Context) (*Sandbox, error) {
	var out Sandbox
	if err := sandbox.json(ctx, http.MethodGet, "", nil, nil, &out); err != nil {
		return nil, err
	}
	endpoint, client := sandbox.endpoint, sandbox.client
	*sandbox = out
	sandbox.client = client
	sandbox.endpoint = endpoint
	sandbox.initServices()
	return sandbox, nil
}

// Run executes a command and captures its output.
func (sandbox *Sandbox) Run(ctx context.Context, request ExecRequest) (ExecResult, error) {
	if len(request.Command) == 0 {
		return ExecResult{}, errors.New("vmon: exec command must not be empty")
	}
	var wire struct {
		Exit   int64  `json:"exit"`
		Stdout string `json:"stdout_b64"`
		Stderr string `json:"stderr_b64"`
	}
	if err := sandbox.json(ctx, http.MethodPost, "/exec", nil, request, &wire); err != nil {
		return ExecResult{}, err
	}
	stdout, err := base64.StdEncoding.DecodeString(wire.Stdout)
	if err != nil {
		return ExecResult{}, &ProtocolError{Operation: "run", Message: "invalid stdout_b64", Err: err}
	}
	stderr, err := base64.StdEncoding.DecodeString(wire.Stderr)
	if err != nil {
		return ExecResult{}, &ProtocolError{Operation: "run", Message: "invalid stderr_b64", Err: err}
	}
	return ExecResult{ExitCode: wire.Exit, Stdout: stdout, Stderr: stderr}, nil
}

// Logs retrieves the current standard output and standard error logs of the sandbox.
func (sandbox *Sandbox) Logs(ctx context.Context) (string, error) {
	response, err := sandbox.do(ctx, DriverRequest{Method: http.MethodGet, Path: "/logs", Query: url.Values{"follow": {"false"}}, Headers: http.Header{"Accept": {"text/plain"}}})
	if err != nil {
		return "", err
	}
	body, err := sandbox.client.readResponse(response)
	return string(body), err
}

// LogStream incrementally decodes follow-log SSE chunks.
type LogStream struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
}

// Next returns the next decoded log chunk.
func (stream *LogStream) Next() ([]byte, error) {
	if stream == nil || stream.body == nil {
		return nil, errors.New("vmon: log stream is not open")
	}
	for stream.scanner.Scan() {
		line := strings.TrimSuffix(stream.scanner.Text(), "\r")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var event struct {
			Data string `json:"data"`
			B64  string `json:"b64"`
		}
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			return nil, &ProtocolError{Operation: "follow logs", Message: "invalid log event JSON", Err: err}
		}
		if event.B64 != "" {
			chunk, err := base64.StdEncoding.DecodeString(event.B64)
			if err != nil {
				return nil, &ProtocolError{Operation: "follow logs", Message: "invalid base64 log chunk", Err: err}
			}
			return chunk, nil
		}
		return []byte(event.Data), nil
	}
	if err := stream.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// Close closes the underlying response stream.
func (stream *LogStream) Close() error {
	if stream == nil || stream.body == nil {
		return nil
	}
	return stream.body.Close()
}

// FollowLogs opens a stream to receive real-time sandbox logs.
func (sandbox *Sandbox) FollowLogs(ctx context.Context) (*LogStream, error) {
	response, err := sandbox.do(ctx, DriverRequest{Method: http.MethodGet, Path: "/logs", Query: url.Values{"follow": {"true"}}, Stream: true, Headers: http.Header{"Accept": {"text/event-stream"}}})
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), maxEventBytes)
	return &LogStream{body: response.Body, scanner: scanner}, nil
}

// Metrics retrieves resource utilization metrics for the sandbox.
func (sandbox *Sandbox) Metrics(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	err := sandbox.json(ctx, http.MethodGet, "/metrics", nil, nil, &out)
	return out, err
}
func (sandbox *Sandbox) action(ctx context.Context, method, suffix string, body any) (*Sandbox, error) {
	var out Sandbox
	if err := sandbox.json(ctx, method, suffix, nil, body, &out); err != nil {
		return nil, err
	}
	endpoint := sandbox.endpoint
	client := sandbox.client
	if out.ID == "" {
		out.ID = sandbox.ID
	}
	if out.Name == "" {
		out.Name = sandbox.Name
	}
	*sandbox = out
	sandbox.client = client
	sandbox.endpoint = endpoint
	sandbox.initServices()
	return sandbox, nil
}

// Stop halts the execution of the sandbox.
func (sandbox *Sandbox) Stop(ctx context.Context) (*Sandbox, error) {
	return sandbox.action(ctx, http.MethodPost, "/stop", nil)
}

// Terminate immediately halts the sandbox and releases all its resources.
func (sandbox *Sandbox) Terminate(ctx context.Context) error {
	response, err := sandbox.do(ctx, DriverRequest{Method: http.MethodPost, Path: "/terminate"})
	if err == nil {
		response.Body.Close()
	}
	return err
}

// Remove deletes the sandbox and any persistent filesystems.
func (sandbox *Sandbox) Remove(ctx context.Context) error {
	response, err := sandbox.do(ctx, DriverRequest{Method: http.MethodDelete})
	if err == nil {
		response.Body.Close()
	}
	return err
}

// Pause suspends all active processes in the sandbox.
func (sandbox *Sandbox) Pause(ctx context.Context) (*Sandbox, error) {
	return sandbox.action(ctx, http.MethodPost, "/pause", nil)
}

// Resume reactivates previously paused processes in the sandbox.
func (sandbox *Sandbox) Resume(ctx context.Context) (*Sandbox, error) {
	return sandbox.action(ctx, http.MethodPost, "/resume", nil)
}

// Extend increases the lease duration of the sandbox by the specified seconds.
func (sandbox *Sandbox) Extend(ctx context.Context, seconds uint64) (*Sandbox, error) {
	return sandbox.action(ctx, http.MethodPost, "/extend", struct {
		Seconds uint64 `json:"secs"`
	}{seconds})
}

// Migrate relocates the sandbox to a different target node.
func (sandbox *Sandbox) Migrate(ctx context.Context, target string) (*Sandbox, error) {
	out, err := sandbox.action(ctx, http.MethodPost, "/migrate", struct {
		Target string `json:"target"`
	}{target})
	if err != nil {
		return nil, err
	}
	endpoint, err := sandbox.client.driver.ResolveSandbox(ctx, sandbox.ID, sandbox.endpoint)
	if err != nil {
		return nil, err
	}
	sandbox.endpoint = endpoint
	return out, nil
}

// Snapshot captures the current filesystem and memory state of the sandbox.
func (sandbox *Sandbox) Snapshot(ctx context.Context, request SnapshotRequest) (string, error) {
	var out struct {
		Snapshot string `json:"snapshot"`
	}
	if err := sandbox.json(ctx, http.MethodPost, "/snapshots", nil, request, &out); err != nil {
		return "", err
	}
	if out.Snapshot == "" {
		return "", &ProtocolError{Operation: "snapshot", Message: "response did not include snapshot"}
	}
	return out.Snapshot, nil
}

// SnapshotFilesystem captures the current filesystem image of the sandbox.
func (sandbox *Sandbox) SnapshotFilesystem(ctx context.Context, request FilesystemSnapshotRequest) (string, error) {
	var out struct {
		Image string `json:"image"`
	}
	if err := sandbox.json(ctx, http.MethodPost, "/snapshots/fs", nil, request, &out); err != nil {
		return "", err
	}
	if out.Image == "" {
		return "", &ProtocolError{Operation: "filesystem snapshot", Message: "response did not include image"}
	}
	return out.Image, nil
}

// Network retrieves the active network state and policy for the sandbox.
func (sandbox *Sandbox) Network(ctx context.Context) (NetworkState, error) {
	var out NetworkState
	err := sandbox.json(ctx, http.MethodGet, "/network", nil, nil, &out)
	return out, err
}

// SetNetwork updates the network policy of the sandbox.
func (sandbox *Sandbox) SetNetwork(ctx context.Context, policy NetworkPolicy) (NetworkState, error) {
	var out NetworkState
	err := sandbox.json(ctx, http.MethodPut, "/network", nil, policy, &out)
	return out, err
}

// Tunnels retrieves active proxy tunnel endpoints and updates the connect token.
func (sandbox *Sandbox) Tunnels(ctx context.Context) (TunnelSet, error) {
	var out TunnelSet
	err := sandbox.json(ctx, http.MethodGet, "/tunnels", nil, nil, &out)
	if err == nil && out.ConnectToken != "" {
		sandbox.connectToken = out.ConnectToken
	}
	return out, err
}

// WaitReadyOptions configures lifecycle polling and an optional readiness probe.
type WaitReadyOptions struct {
	Timeout  time.Duration
	Interval time.Duration
	Port     uint16
	Command  []string
}

// WaitReady blocks until the sandbox is running and its optional probe succeeds.
func (sandbox *Sandbox) WaitReady(ctx context.Context, options ...WaitReadyOptions) (*Sandbox, error) {
	settings := WaitReadyOptions{Timeout: 5 * time.Minute, Interval: 250 * time.Millisecond}
	if len(options) > 0 {
		settings = options[0]
		if settings.Timeout <= 0 {
			settings.Timeout = 5 * time.Minute
		}
		if settings.Interval <= 0 {
			settings.Interval = 250 * time.Millisecond
		}
	}
	if settings.Port != 0 && len(settings.Command) != 0 {
		return nil, errors.New("vmon: readiness port and command probes are mutually exclusive")
	}
	waitCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
	defer cancel()
	ticker := time.NewTicker(settings.Interval)
	defer ticker.Stop()
	for {
		current, err := sandbox.Refresh(waitCtx)
		if err != nil {
			return nil, err
		}
		switch current.Status {
		case "failed", "terminated", "exited":
			return nil, &APIError{StatusCode: http.StatusConflict, Code: "not_running", Message: "sandbox entered " + current.Status}
		case "running", "ready":
			ready := settings.Port == 0 && len(settings.Command) == 0
			if settings.Port != 0 {
				tunnels, probeErr := sandbox.Tunnels(waitCtx)
				if probeErr == nil {
					if target, exists := tunnels.Tunnels[settings.Port]; exists {
						address := net.JoinHostPort(target.Host, strconv.Itoa(int(target.Port)))
						connection, dialErr := (&net.Dialer{Timeout: settings.Interval}).DialContext(waitCtx, "tcp", address)
						if dialErr == nil {
							_ = connection.Close()
							ready = true
						}
					}
				}
			}
			if len(settings.Command) != 0 {
				result, probeErr := sandbox.Run(waitCtx, ExecRequest{Command: settings.Command})
				ready = probeErr == nil && result.ExitCode == 0
			}
			if ready {
				return current, nil
			}
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, &APIError{StatusCode: http.StatusRequestTimeout, Code: "timeout", Message: "sandbox readiness timed out"}
		case <-ticker.C:
		}
	}
}

// Files manages file-system operations on the guest sandbox.
type Files struct{ sandbox *Sandbox }

// Open retrieves a stream to read the file at the specified guest path.
func (files *Files) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	if path == "" {
		return nil, errors.New("vmon: guest path must not be empty")
	}
	response, err := files.sandbox.do(ctx, DriverRequest{Method: http.MethodGet, Path: "/files", Query: url.Values{"path": {path}}, Stream: true, Headers: http.Header{"Accept": {"application/octet-stream"}}})
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

// Read reads and returns the full contents of the file at the specified guest path.
func (files *Files) Read(ctx context.Context, path string) ([]byte, error) {
	body, err := files.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, files.sandbox.client.maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > files.sandbox.client.maxResponseBytes {
		return nil, &ResponseTooLargeError{Limit: files.sandbox.client.maxResponseBytes}
	}
	return data, nil
}

// Write writes data to the file at the specified guest path.
func (files *Files) Write(ctx context.Context, path string, data []byte) error {
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	response, err := files.sandbox.do(ctx, DriverRequest{Method: http.MethodPut, Path: "/files", Query: url.Values{"path": {path}}, Content: data, Headers: http.Header{"Content-Type": {"application/octet-stream"}}})
	if err == nil {
		response.Body.Close()
	}
	return err
}

// List retrieves metadata for files and directories inside the specified guest path.
func (files *Files) List(ctx context.Context, path string) ([]FileInfo, error) {
	if path == "" {
		path = "."
	}
	var out struct {
		Entries []FileInfo `json:"entries"`
	}
	if err := files.sandbox.json(ctx, http.MethodGet, "/files/list", url.Values{"path": {path}}, nil, &out); err != nil {
		return nil, err
	}
	if out.Entries == nil {
		return nil, &ProtocolError{Operation: "list files", Message: "response did not include entries"}
	}
	return out.Entries, nil
}

// Stat retrieves file information/metadata for the specified guest path.
func (files *Files) Stat(ctx context.Context, path string) (FileInfo, error) {
	if path == "" {
		return FileInfo{}, errors.New("vmon: guest path must not be empty")
	}
	var out FileInfo
	err := files.sandbox.json(ctx, http.MethodGet, "/files/stat", url.Values{"path": {path}}, nil, &out)
	return out, err
}

// DeleteOptions configures guest filesystem deletion.
type DeleteOptions struct {
	Recursive bool
}

// Delete removes the file or directory at the specified guest path.
func (files *Files) Delete(ctx context.Context, path string, options ...DeleteOptions) error {
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	query := url.Values{"path": {path}}
	if len(options) > 0 && options[0].Recursive {
		query.Set("recursive", "true")
	}
	response, err := files.sandbox.do(ctx, DriverRequest{Method: http.MethodDelete, Path: "/files", Query: query})
	if err == nil {
		response.Body.Close()
	}
	return err
}

// Mkdir creates a directory (and any necessary parent directories) inside the guest.
func (files *Files) Mkdir(ctx context.Context, path string) error {
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	result, err := files.sandbox.Run(ctx, ExecRequest{Command: []string{"mkdir", "-p", "--", path}})
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("vmon: mkdir failed: %s", strings.TrimSpace(string(result.Stderr)))
	}
	return nil
}

// Ports manages HTTP proxying and port forwarding to the sandbox.
type Ports struct{ sandbox *Sandbox }

func (ports *Ports) token(ctx context.Context) (string, error) {
	if ports.sandbox.connectToken == "" {
		if _, err := ports.sandbox.Tunnels(ctx); err != nil {
			return "", err
		}
	}
	if ports.sandbox.connectToken == "" {
		return "", &ProtocolError{Operation: "port proxy", Message: "server did not return a connect token"}
	}
	return ports.sandbox.connectToken, nil
}
func sanitizeProxyQuery(query url.Values) url.Values {
	query = cloneValues(query)
	query.Del("connect_token")
	query.Del("token")
	query.Del("access_token")
	return query
}

type replayReadCloser struct {
	io.Reader
	closer io.Closer
}

func (body *replayReadCloser) Close() error {
	return body.closer.Close()
}

func daemonNotFoundResponse(response *http.Response, limit int64) (bool, error) {
	original := response.Body
	prefix, err := io.ReadAll(io.LimitReader(original, limit+1))
	if err != nil {
		return false, err
	}
	response.Body = &replayReadCloser{Reader: io.MultiReader(bytes.NewReader(prefix), original), closer: original}
	if int64(len(prefix)) > limit {
		return false, nil
	}
	var envelope struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(prefix, &envelope); err != nil {
		return false, nil
	}
	return envelope.Code == "not_found", nil
}

// HTTP proxies an incoming HTTP request to the specified guest port.
func (ports *Ports) HTTP(ctx context.Context, port uint16, proxy ProxyRequest) (*http.Response, error) {
	if proxy.Method == "" {
		closeProxyBody(proxy.Body)
		return nil, errors.New("vmon: proxy method must not be empty")
	}
	content, err := io.ReadAll(proxy.Body)
	closeProxyBody(proxy.Body)
	if err != nil {
		return nil, err
	}
	token, err := ports.token(ctx)
	if err != nil {
		return nil, err
	}
	query := sanitizeProxyQuery(proxy.Query)
	query.Set("connect_token", token)
	request := DriverRequest{
		Method:   proxy.Method,
		Path:     sandboxPath(ports.sandbox.ID) + "/ports/" + strconv.Itoa(int(port)) + "/" + escapeRestPath(proxy.Path),
		Query:    query,
		Content:  content,
		Headers:  proxy.Header,
		Stream:   true,
		Endpoint: ports.sandbox.endpoint,
	}
	response, endpoint, err := ports.sandbox.client.driver.Do(ctx, request)
	if endpoint != "" {
		ports.sandbox.endpoint = endpoint
	}
	if err != nil || response.StatusCode != http.StatusNotFound || len(ports.sandbox.client.driver.Endpoints()) <= 1 {
		return response, err
	}
	daemonNotFound, inspectErr := daemonNotFoundResponse(response, ports.sandbox.client.maxResponseBytes)
	if inspectErr != nil {
		_ = response.Body.Close()
		return nil, inspectErr
	}
	if !daemonNotFound {
		return response, nil
	}
	_ = response.Body.Close()
	endpoint, err = ports.sandbox.client.driver.ResolveSandbox(ctx, ports.sandbox.ID, ports.sandbox.endpoint)
	if err != nil {
		return nil, err
	}
	ports.sandbox.endpoint = endpoint
	request.Endpoint = endpoint
	response, used, err := ports.sandbox.client.driver.Do(ctx, request)
	if used != "" {
		ports.sandbox.endpoint = used
	}
	return response, err
}
