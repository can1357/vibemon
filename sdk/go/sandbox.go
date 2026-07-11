package vmon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
)

type sandboxesResponse struct {
	Sandboxes []*Sandbox `json:"sandboxes"`
}

type okResponse struct {
	OK bool `json:"ok"`
}

func sandboxPath(id string) string {
	return "/v1/sandboxes/" + escapePathSegment(id)
}

func (client *Client) bindSandbox(sandbox *Sandbox, operation string) (*Sandbox, error) {
	if sandbox.ID == "" && sandbox.Name == "" {
		return nil, &ProtocolError{Operation: operation, Message: "sandbox response has no id or name"}
	}
	sandbox.client = client
	return sandbox, nil
}

// CreateSandbox creates a sandbox and returns its initial daemon view.
func (client *Client) CreateSandbox(ctx context.Context, request SandboxCreateRequest) (*Sandbox, error) {
	var sandbox Sandbox
	if err := client.doJSON(ctx, http.MethodPost, "/v1/sandboxes", nil, request, &sandbox); err != nil {
		return nil, err
	}
	return client.bindSandbox(&sandbox, "create sandbox")
}

// ListSandboxes returns sandbox views matching all requested tags.
func (client *Client) ListSandboxes(ctx context.Context, options SandboxListOptions) ([]*Sandbox, error) {
	query := make(url.Values)
	keys := make([]string, 0, len(options.Tags))
	for key := range options.Tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		query.Add("tag", key+"="+options.Tags[key])
	}
	var response sandboxesResponse
	if err := client.doJSON(ctx, http.MethodGet, "/v1/sandboxes", query, nil, &response); err != nil {
		return nil, err
	}
	for _, sandbox := range response.Sandboxes {
		if sandbox == nil {
			return nil, &ProtocolError{Operation: "list sandboxes", Message: "sandbox list contains null"}
		}
		if _, err := client.bindSandbox(sandbox, "list sandboxes"); err != nil {
			return nil, err
		}
	}
	return response.Sandboxes, nil
}

// GetSandbox returns the latest daemon view for a sandbox identifier.
func (client *Client) GetSandbox(ctx context.Context, id string) (*Sandbox, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	var sandbox Sandbox
	if err := client.doJSON(ctx, http.MethodGet, sandboxPath(id), nil, nil, &sandbox); err != nil {
		return nil, err
	}
	return client.bindSandbox(&sandbox, "get sandbox")
}

// PollSandbox performs one non-blocking lifecycle observation.
func (client *Client) PollSandbox(ctx context.Context, id string) (SandboxPoll, error) {
	sandbox, err := client.GetSandbox(ctx, id)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return SandboxPoll{Exists: false, Done: true}, nil
		}
		return SandboxPoll{}, err
	}
	done := false
	switch sandbox.Status {
	case "stopped", "terminated", "failed":
		done = true
	}
	return SandboxPoll{
		Sandbox:  sandbox,
		Exists:   true,
		Done:     done,
		ExitCode: sandbox.ReturnCode,
	}, nil
}

func (client *Client) sandboxAction(ctx context.Context, method, id, suffix string, body any) (*Sandbox, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	var sandbox Sandbox
	path := sandboxPath(id) + suffix
	if err := client.doJSON(ctx, method, path, nil, body, &sandbox); err != nil {
		return nil, err
	}
	return client.bindSandbox(&sandbox, method+" "+path)
}

// StopSandbox requests an orderly sandbox stop.
func (client *Client) StopSandbox(ctx context.Context, id string) (*Sandbox, error) {
	return client.sandboxAction(ctx, http.MethodPost, id, "/stop", nil)
}

// TerminateSandbox forcefully terminates a sandbox.
func (client *Client) TerminateSandbox(ctx context.Context, id string) (*Sandbox, error) {
	return client.sandboxAction(ctx, http.MethodPost, id, "/terminate", nil)
}

// RemoveSandbox removes a sandbox and returns the daemon's terminal view.
func (client *Client) RemoveSandbox(ctx context.Context, id string) (*Sandbox, error) {
	return client.sandboxAction(ctx, http.MethodDelete, id, "", nil)
}

// PauseSandbox pauses a running sandbox.
func (client *Client) PauseSandbox(ctx context.Context, id string) (*Sandbox, error) {
	return client.sandboxAction(ctx, http.MethodPost, id, "/pause", nil)
}

// ResumeSandbox resumes a paused sandbox.
func (client *Client) ResumeSandbox(ctx context.Context, id string) (*Sandbox, error) {
	return client.sandboxAction(ctx, http.MethodPost, id, "/resume", nil)
}

// ExtendSandbox extends a sandbox's idle deadline and returns its absolute deadline.
func (client *Client) ExtendSandbox(
	ctx context.Context,
	id string,
	seconds uint64,
) (ExtendResult, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return ExtendResult{}, err
	}
	var result ExtendResult
	body := struct {
		Seconds uint64 `json:"secs"`
	}{Seconds: seconds}
	if err := client.doJSON(
		ctx,
		http.MethodPost,
		sandboxPath(id)+"/extend",
		nil,
		body,
		&result,
	); err != nil {
		return ExtendResult{}, err
	}
	return result, nil
}

// MigrateSandbox moves a sandbox to a target mesh node and returns the opaque mesh result.
func (client *Client) MigrateSandbox(
	ctx context.Context,
	id string,
	target string,
) (MigrateResult, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	if err := requireIdentifier("target node id", target); err != nil {
		return nil, err
	}
	var result MigrateResult
	body := struct {
		Target string `json:"target"`
	}{Target: target}
	if err := client.doJSON(
		ctx,
		http.MethodPost,
		sandboxPath(id)+"/migrate",
		nil,
		body,
		&result,
	); err != nil {
		return nil, err
	}
	if result == nil {
		return nil, &ProtocolError{Operation: "migrate sandbox", Message: "response is not an object"}
	}
	return result, nil
}

// ExecCapture runs a command and returns its fully captured output.
func (client *Client) ExecCapture(ctx context.Context, id string, request ExecRequest) (ExecResult, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return ExecResult{}, err
	}
	if len(request.Command) == 0 || request.Command[0] == "" {
		return ExecResult{}, errors.New("vmon: exec command must not be empty")
	}
	var response struct {
		Exit       int64  `json:"exit"`
		StdoutBase string `json:"stdout_b64"`
		StderrBase string `json:"stderr_b64"`
	}
	path := sandboxPath(id) + "/exec"
	if err := client.doJSON(ctx, http.MethodPost, path, nil, request, &response); err != nil {
		return ExecResult{}, err
	}
	stdout, err := base64.StdEncoding.DecodeString(response.StdoutBase)
	if err != nil {
		return ExecResult{}, &ProtocolError{Operation: "captured exec", Message: "invalid stdout_b64", Err: err}
	}
	stderr, err := base64.StdEncoding.DecodeString(response.StderrBase)
	if err != nil {
		return ExecResult{}, &ProtocolError{Operation: "captured exec", Message: "invalid stderr_b64", Err: err}
	}
	return ExecResult{ExitCode: response.Exit, Stdout: stdout, Stderr: stderr}, nil
}

// SandboxMetrics returns the daemon's dynamic metrics object for one sandbox.
func (client *Client) SandboxMetrics(ctx context.Context, id string) (map[string]any, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	var metrics map[string]any
	if err := client.doJSON(ctx, http.MethodGet, sandboxPath(id)+"/metrics", nil, nil, &metrics); err != nil {
		return nil, err
	}
	return metrics, nil
}

// SandboxLogs returns the currently captured sandbox console log.
func (client *Client) SandboxLogs(ctx context.Context, id string) (string, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return "", err
	}
	request, err := client.newRequest(
		ctx,
		http.MethodGet,
		sandboxPath(id)+"/logs",
		url.Values{"follow": []string{"false"}},
		nil,
		"",
	)
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

// GetNetwork returns the effective sandbox network state.
func (client *Client) GetNetwork(ctx context.Context, id string) (NetworkState, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return NetworkState{}, err
	}
	var state NetworkState
	if err := client.doJSON(ctx, http.MethodGet, sandboxPath(id)+"/network", nil, nil, &state); err != nil {
		return NetworkState{}, err
	}
	return state, nil
}

// SetNetwork applies a partial sandbox network policy and returns the effective state.
func (client *Client) SetNetwork(ctx context.Context, id string, policy NetworkPolicy) (NetworkState, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return NetworkState{}, err
	}
	var state NetworkState
	if err := client.doJSON(ctx, http.MethodPut, sandboxPath(id)+"/network", nil, policy, &state); err != nil {
		return NetworkState{}, err
	}
	return state, nil
}

// Tunnels returns exposed sandbox ports and a fresh proxy connection token.
func (client *Client) Tunnels(ctx context.Context, id string) (TunnelSet, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return TunnelSet{}, err
	}
	var tunnels TunnelSet
	if err := client.doJSON(ctx, http.MethodGet, sandboxPath(id)+"/tunnels", nil, nil, &tunnels); err != nil {
		return TunnelSet{}, err
	}
	return tunnels, nil
}

// OpenFile opens a guest file response stream; the caller must close the returned body.
func (client *Client) OpenFile(ctx context.Context, id, path string) (io.ReadCloser, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("vmon: guest path must not be empty")
	}
	request, err := client.newRequest(
		ctx,
		http.MethodGet,
		sandboxPath(id)+"/files",
		url.Values{"path": []string{path}},
		nil,
		"",
	)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/octet-stream")
	response, err := client.do(request)
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

// ReadFile reads a guest file into memory subject to the configured response limit.
func (client *Client) ReadFile(ctx context.Context, id, path string) ([]byte, error) {
	body, err := client.OpenFile(ctx, id, path)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return readLimited(body, client.maxResponseBytes)
}

// WriteFile writes the supplied bytes to a guest file.
func (client *Client) WriteFile(ctx context.Context, id, path string, data []byte) error {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return err
	}
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	request, err := client.newRequest(
		ctx,
		http.MethodPut,
		sandboxPath(id)+"/files",
		url.Values{"path": []string{path}},
		bytes.NewReader(data),
		"application/octet-stream",
	)
	if err != nil {
		return err
	}
	response, err := client.do(request)
	if err != nil {
		return err
	}
	var result okResponse
	if err := client.decodeJSONResponse(response, "write file", &result); err != nil {
		return err
	}
	if !result.OK {
		return &ProtocolError{Operation: "write file", Message: "response did not confirm success"}
	}
	return nil
}

// DeleteFile removes a guest file or directory.
func (client *Client) DeleteFile(ctx context.Context, id, path string, recursive bool) error {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return err
	}
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	query := url.Values{"path": []string{path}}
	if recursive {
		query.Set("recursive", "true")
	}
	var result okResponse
	if err := client.doJSON(ctx, http.MethodDelete, sandboxPath(id)+"/files", query, nil, &result); err != nil {
		return err
	}
	if !result.OK {
		return &ProtocolError{Operation: "delete file", Message: "response did not confirm success"}
	}
	return nil
}

// ListFiles lists entries in a guest directory.
func (client *Client) ListFiles(ctx context.Context, id, path string) ([]FileInfo, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("vmon: guest path must not be empty")
	}
	var entries []FileInfo
	if err := client.doJSON(
		ctx,
		http.MethodGet,
		sandboxPath(id)+"/files/list",
		url.Values{"path": []string{path}},
		nil,
		&entries,
	); err != nil {
		return nil, err
	}
	return entries, nil
}

// StatFile returns metadata for a guest filesystem path.
func (client *Client) StatFile(ctx context.Context, id, path string) (FileInfo, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return FileInfo{}, err
	}
	if path == "" {
		return FileInfo{}, errors.New("vmon: guest path must not be empty")
	}
	var info FileInfo
	if err := client.doJSON(
		ctx,
		http.MethodGet,
		sandboxPath(id)+"/files/stat",
		url.Values{"path": []string{path}},
		nil,
		&info,
	); err != nil {
		return FileInfo{}, err
	}
	return info, nil
}

// SnapshotSandbox creates a full snapshot of a sandbox.
func (client *Client) SnapshotSandbox(
	ctx context.Context,
	id string,
	request SnapshotRequest,
) (SnapshotResult, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return SnapshotResult{}, err
	}
	var result SnapshotResult
	if err := client.doJSON(
		ctx,
		http.MethodPost,
		sandboxPath(id)+"/snapshots",
		nil,
		request,
		&result,
	); err != nil {
		return SnapshotResult{}, err
	}
	return result, nil
}

// SnapshotFilesystem creates a filesystem-only image from a sandbox.
func (client *Client) SnapshotFilesystem(
	ctx context.Context,
	id string,
	request FilesystemSnapshotRequest,
) (FilesystemSnapshotResult, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return FilesystemSnapshotResult{}, err
	}
	var result FilesystemSnapshotResult
	if err := client.doJSON(
		ctx,
		http.MethodPost,
		sandboxPath(id)+"/snapshots/fs",
		nil,
		request,
		&result,
	); err != nil {
		return FilesystemSnapshotResult{}, err
	}
	return result, nil
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

func (sandbox *Sandbox) boundClient() (*Client, error) {
	if sandbox == nil || sandbox.client == nil {
		return nil, errors.New("vmon: sandbox is not bound to a client")
	}
	return sandbox.client, nil
}

// Identifier returns the preferred name or stable id for sandbox operations.
func (sandbox *Sandbox) Identifier() string {
	if sandbox == nil {
		return ""
	}
	if sandbox.Name != "" {
		return sandbox.Name
	}
	return sandbox.ID
}

// Refresh updates the sandbox with its latest daemon view.
func (sandbox *Sandbox) Refresh(ctx context.Context) error {
	client, err := sandbox.boundClient()
	if err != nil {
		return err
	}
	updated, err := client.GetSandbox(ctx, sandbox.Identifier())
	if err != nil {
		return err
	}
	*sandbox = *updated
	return nil
}

// Poll performs one non-blocking lifecycle observation for this sandbox.
func (sandbox *Sandbox) Poll(ctx context.Context) (SandboxPoll, error) {
	client, err := sandbox.boundClient()
	if err != nil {
		return SandboxPoll{}, err
	}
	result, err := client.PollSandbox(ctx, sandbox.Identifier())
	if err != nil {
		return SandboxPoll{}, err
	}
	if result.Sandbox != nil {
		*sandbox = *result.Sandbox
		result.Sandbox = sandbox
	} else if result.ExitCode == nil {
		result.ExitCode = sandbox.ReturnCode
	}
	return result, nil
}

// ExecCapture runs a command in this sandbox and captures its output.
func (sandbox *Sandbox) ExecCapture(ctx context.Context, request ExecRequest) (ExecResult, error) {
	client, err := sandbox.boundClient()
	if err != nil {
		return ExecResult{}, err
	}
	return client.ExecCapture(ctx, sandbox.Identifier(), request)
}

// ReadFile reads a guest file from this sandbox.
func (sandbox *Sandbox) ReadFile(ctx context.Context, path string) ([]byte, error) {
	client, err := sandbox.boundClient()
	if err != nil {
		return nil, err
	}
	return client.ReadFile(ctx, sandbox.Identifier(), path)
}

// WriteFile writes bytes to a guest file in this sandbox.
func (sandbox *Sandbox) WriteFile(ctx context.Context, path string, data []byte) error {
	client, err := sandbox.boundClient()
	if err != nil {
		return err
	}
	return client.WriteFile(ctx, sandbox.Identifier(), path, data)
}

// Terminate terminates this sandbox and refreshes its local view.
func (sandbox *Sandbox) Terminate(ctx context.Context) error {
	client, err := sandbox.boundClient()
	if err != nil {
		return err
	}
	updated, err := client.TerminateSandbox(ctx, sandbox.Identifier())
	if err != nil {
		return err
	}
	*sandbox = *updated
	return nil
}

// Remove removes this sandbox and refreshes its local terminal view.
func (sandbox *Sandbox) Remove(ctx context.Context) error {
	client, err := sandbox.boundClient()
	if err != nil {
		return err
	}
	updated, err := client.RemoveSandbox(ctx, sandbox.Identifier())
	if err != nil {
		return err
	}
	*sandbox = *updated
	return nil
}

// String returns the sandbox's preferred identifier.
func (sandbox *Sandbox) String() string {
	return sandbox.Identifier()
}
