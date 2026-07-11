package vmon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultRemoteFunctionImage provides the Node.js runtime used when no image source is set.
	DefaultRemoteFunctionImage = "node:22-slim"
	remoteFunctionRunnerPath   = "/tmp/vmon-remote-function-runner.mjs"
	remoteFunctionPayloadPath  = "/tmp/vmon-remote-function-invocation"
	remoteRuntimeCheckTimeout  = 10.0
	remoteCleanupTimeout       = 30 * time.Second
	defaultRemoteConcurrency   = 4
)

var remoteExportNamePattern = regexp.MustCompile(`^(?:default|[$A-Z_a-z][$\w]*)$`)

const remoteFunctionRunner = `
import { readFile } from "node:fs/promises";

function jsonValue(value, path, active) {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(path + " contains a non-finite number");
    }
    return value;
  }
  if (typeof value !== "object") {
    throw new TypeError(path + " contains a non-JSON value");
  }
  if (active.has(value)) {
    throw new TypeError(path + " contains a cycle");
  }
  active.add(value);
  try {
    if (Array.isArray(value)) {
      const result = [];
      for (let index = 0; index < value.length; index += 1) {
        if (!(index in value)) {
          throw new TypeError(path + " contains a sparse array");
        }
        result.push(jsonValue(value[index], path + "[" + index + "]", active));
      }
      return result;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(path + " contains a non-plain object");
    }
    if (Object.getOwnPropertySymbols(value).length !== 0) {
      throw new TypeError(path + " contains symbol properties");
    }
    const result = {};
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Object.keys(value).sort();
    for (const key of keys) {
      const descriptor = descriptors[key];
      if (!("value" in descriptor)) {
        throw new TypeError(path + "." + key + " is an accessor property");
      }
      result[key] = jsonValue(descriptor.value, path + "." + key, active);
    }
    return result;
  } finally {
    active.delete(value);
  }
}

function errorDetails(error) {
  if (error !== null && typeof error === "object") {
    const type = typeof error.name === "string" && error.name.length > 0
      ? error.name
      : error.constructor && typeof error.constructor.name === "string"
        ? error.constructor.name
        : "RemoteError";
    return {
      type,
      message: typeof error.message === "string" ? error.message : String(error),
      stack: typeof error.stack === "string" ? error.stack : "",
    };
  }
  return { type: "RemoteError", message: String(error), stack: "" };
}

const originalWrite = process.stdout.write;
let capturedStdout = "";
process.stdout.write = function captureStdout(chunk, encoding, callback) {
  capturedStdout += typeof chunk === "string" ? chunk : Buffer.from(chunk).toString();
  const done = typeof encoding === "function" ? encoding : callback;
  if (typeof done === "function") {
    queueMicrotask(done);
  }
  return true;
};

let response;
try {
  const payload = JSON.parse(await readFile(process.argv[2], "utf8"));
  if (payload === null || typeof payload !== "object") {
    throw new TypeError("remote function invocation must be an object");
  }
  if (typeof payload.source !== "string" || typeof payload.exportName !== "string") {
    throw new TypeError("remote function source and exportName must be strings");
  }
  if (!Array.isArray(payload.args)) {
    throw new TypeError("remote function args must be an array");
  }
  const moduleSource = payload.source + "\n//# sourceURL=vmon-remote-function.mjs\n";
  const moduleURL = "data:text/javascript;base64," + Buffer.from(moduleSource).toString("base64");
  const namespace = await import(moduleURL);
  const handler = namespace[payload.exportName];
  if (typeof handler !== "function") {
    throw new TypeError("remote function module does not export callable " + payload.exportName);
  }
  const result = await handler(...payload.args);
  response = {
    ok: true,
    result: jsonValue(result, "remote function result", new Set()),
    stdout: capturedStdout,
  };
} catch (error) {
  response = {
    ok: false,
    error: errorDetails(error),
    stdout: capturedStdout,
  };
}
process.stdout.write = originalWrite;
originalWrite.call(process.stdout, JSON.stringify(response));
`

// RemoteFunctionSourceSpec identifies a callable export in a self-contained JavaScript module.
type RemoteFunctionSourceSpec struct {
	// Source is JavaScript ES module source uploaded with each invocation.
	Source string
	// ExportName names the exported function invoked by the guest runner.
	ExportName string
}

// RemoteFunctionOptions configures the sandboxes and calls owned by a remote function.
type RemoteFunctionOptions struct {
	// Sandbox is the create request used for cached and map-worker sandboxes.
	Sandbox SandboxCreateRequest
	// InvocationTimeout is the server-side timeout for each Node.js invocation.
	InvocationTimeout *float64
	// Stdout receives output captured while the remote handler runs; nil uses os.Stdout.
	Stdout io.Writer
}

// RemoteResultOrder selects how Map and StarMap order successful results.
type RemoteResultOrder uint8

const (
	// RemoteInputOrder returns results in the same order as the supplied inputs.
	RemoteInputOrder RemoteResultOrder = iota
	// RemoteCompletionOrder returns results in remote completion order.
	RemoteCompletionOrder
)

// RemoteMapOptions configures one bounded ephemeral worker pool.
type RemoteMapOptions struct {
	// Concurrency is the number of worker sandboxes and must be at least one.
	Concurrency int
	// Order defaults to RemoteInputOrder.
	Order RemoteResultOrder
}

// RemoteFunctionError reports a guest-side function or runner failure.
type RemoteFunctionError struct {
	// RemoteType is the guest error class or a runner failure category.
	RemoteType string
	// Message is the guest or runner error message.
	Message string
	// RemoteStack is the guest JavaScript stack, when available.
	RemoteStack string
	// Cause is a local transport or decoding failure, when available.
	Cause error
}

// Error implements error.
func (err *RemoteFunctionError) Error() string {
	if err == nil {
		return "<nil>"
	}
	kind := err.RemoteType
	if kind == "" {
		kind = "RemoteError"
	}
	if err.Message == "" {
		return "vmon remote function: " + kind
	}
	return fmt.Sprintf("vmon remote function: %s: %s", kind, err.Message)
}

// Unwrap exposes a local transport or decoding failure.
func (err *RemoteFunctionError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

type remoteTermination struct {
	done chan struct{}
	err  error
}

// RemoteFunction runs a JavaScript module export in cached or ephemeral vmon sandboxes.
type RemoteFunction[Result any] struct {
	client            *Client
	source            RemoteFunctionSourceSpec
	sandboxRequest    SandboxCreateRequest
	invocationTimeout *float64
	stdout            io.Writer
	stdoutMu          sync.Mutex
	sequence          atomic.Uint64

	mu          sync.Mutex
	sandboxID   string
	creating    chan struct{}
	terminating *remoteTermination
}

// NewRemoteFunction creates a source-backed JavaScript remote function.
//
// Go cannot recover source from a func value, so callers provide an explicit ES module and export.
func NewRemoteFunction[Result any](
	client *Client,
	source RemoteFunctionSourceSpec,
	options ...RemoteFunctionOptions,
) (*RemoteFunction[Result], error) {
	if client == nil {
		return nil, errors.New("vmon: remote function client must not be nil")
	}
	if len(options) > 1 {
		return nil, errors.New("vmon: remote function accepts at most one options value")
	}
	if strings.TrimSpace(source.Source) == "" {
		return nil, errors.New("vmon: remote function source must not be empty")
	}
	if !remoteExportNamePattern.MatchString(source.ExportName) {
		return nil, errors.New("vmon: remote function export name must be a JavaScript identifier or default")
	}

	var configured RemoteFunctionOptions
	if len(options) == 1 {
		configured = options[0]
	}
	request, err := cloneRemoteSandboxRequest(configured.Sandbox)
	if err != nil {
		return nil, err
	}
	if request.Image == "" && request.Template == "" && request.Dockerfile == "" {
		request.Image = DefaultRemoteFunctionImage
	}
	stdout := configured.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	invocationTimeout := cloneFloatPointer(configured.InvocationTimeout)
	return &RemoteFunction[Result]{
		client:            client,
		source:            source,
		sandboxRequest:    request,
		invocationTimeout: invocationTimeout,
		stdout:            stdout,
	}, nil
}

// Remote invokes the function in one lazily created, reusable sandbox.
func (function *RemoteFunction[Result]) Remote(ctx context.Context, arguments ...any) (Result, error) {
	var zero Result
	if function == nil {
		return zero, errors.New("vmon: remote function is nil")
	}
	encodedArguments, err := encodeRemoteArguments(arguments)
	if err != nil {
		return zero, err
	}
	sandboxID, err := function.ensureSandbox(ctx)
	if err != nil {
		return zero, err
	}
	return function.invoke(ctx, sandboxID, encodedArguments)
}

// Map invokes the function once per unary input across ephemeral worker sandboxes.
func (function *RemoteFunction[Result]) Map(
	ctx context.Context,
	inputs []any,
	options ...RemoteMapOptions,
) ([]Result, error) {
	arguments := make([][]any, len(inputs))
	for index, input := range inputs {
		arguments[index] = []any{input}
	}
	return function.mapArguments(ctx, arguments, options...)
}

// StarMap invokes the function once per argument tuple across ephemeral worker sandboxes.
func (function *RemoteFunction[Result]) StarMap(
	ctx context.Context,
	arguments [][]any,
	options ...RemoteMapOptions,
) ([]Result, error) {
	return function.mapArguments(ctx, arguments, options...)
}

// Terminate terminates the cached sandbox; repeated and concurrent calls are harmless.
func (function *RemoteFunction[Result]) Terminate(ctx context.Context) error {
	if function == nil {
		return nil
	}
	for {
		function.mu.Lock()
		if termination := function.terminating; termination != nil {
			function.mu.Unlock()
			select {
			case <-termination.done:
				return termination.err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if creating := function.creating; creating != nil {
			function.mu.Unlock()
			select {
			case <-creating:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if function.sandboxID == "" {
			function.mu.Unlock()
			return nil
		}
		sandboxID := function.sandboxID
		function.sandboxID = ""
		termination := &remoteTermination{done: make(chan struct{})}
		function.terminating = termination
		function.mu.Unlock()

		_, err := function.client.TerminateSandbox(ctx, sandboxID)
		if isRemoteNotFound(err) {
			err = nil
		}
		function.mu.Lock()
		termination.err = err
		close(termination.done)
		if function.terminating == termination {
			function.terminating = nil
		}
		function.mu.Unlock()
		return err
	}
}

func (function *RemoteFunction[Result]) mapArguments(
	ctx context.Context,
	arguments [][]any,
	options ...RemoteMapOptions,
) ([]Result, error) {
	if function == nil {
		return nil, errors.New("vmon: remote function is nil")
	}
	settings, err := checkedRemoteMapOptions(options)
	if err != nil {
		return nil, err
	}
	encoded := make([]json.RawMessage, len(arguments))
	for index, argumentSet := range arguments {
		encoded[index], err = encodeRemoteArguments(argumentSet)
		if err != nil {
			return nil, fmt.Errorf("vmon: remote function arguments at index %d: %w", index, err)
		}
	}
	if len(encoded) == 0 {
		return []Result{}, nil
	}

	workerCount := min(settings.Concurrency, len(encoded))
	sandboxIDs := make([]string, 0, workerCount)
	defer func() {
		function.cleanupSandboxes(sandboxIDs)
	}()
	for range workerCount {
		sandboxID, provisionErr := function.provisionSandbox(ctx)
		if provisionErr != nil {
			return nil, provisionErr
		}
		sandboxIDs = append(sandboxIDs, sandboxID)
	}

	ordered := make([]Result, len(encoded))
	completed := make([]Result, 0, len(encoded))
	var workers sync.WaitGroup
	var stateMu sync.Mutex
	nextIndex := 0
	stopped := false
	var firstError error

	workers.Add(len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		go func() {
			defer workers.Done()
			for {
				stateMu.Lock()
				if stopped || nextIndex >= len(encoded) {
					stateMu.Unlock()
					return
				}
				index := nextIndex
				nextIndex++
				stateMu.Unlock()

				result, invokeErr := function.invoke(ctx, sandboxID, encoded[index])
				stateMu.Lock()
				if invokeErr != nil {
					if firstError == nil {
						firstError = invokeErr
						stopped = true
					}
					stateMu.Unlock()
					return
				}
				ordered[index] = result
				completed = append(completed, result)
				stateMu.Unlock()
			}
		}()
	}
	workers.Wait()
	if firstError != nil {
		return nil, firstError
	}
	if settings.Order == RemoteCompletionOrder {
		return completed, nil
	}
	return ordered, nil
}

func (function *RemoteFunction[Result]) ensureSandbox(ctx context.Context) (string, error) {
	for {
		function.mu.Lock()
		if termination := function.terminating; termination != nil {
			function.mu.Unlock()
			select {
			case <-termination.done:
				if termination.err != nil {
					return "", termination.err
				}
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		if function.sandboxID != "" {
			sandboxID := function.sandboxID
			function.mu.Unlock()
			poll, err := function.client.PollSandbox(ctx, sandboxID)
			if err != nil {
				return "", err
			}
			if poll.Exists && !poll.Done {
				return sandboxID, nil
			}
			function.mu.Lock()
			if function.sandboxID == sandboxID {
				function.sandboxID = ""
			}
			function.mu.Unlock()
			function.cleanupSandboxes([]string{sandboxID})
			continue
		}
		if creating := function.creating; creating != nil {
			function.mu.Unlock()
			select {
			case <-creating:
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		creating := make(chan struct{})
		function.creating = creating
		function.mu.Unlock()

		sandboxID, err := function.provisionSandbox(ctx)
		function.mu.Lock()
		if err == nil {
			function.sandboxID = sandboxID
		}
		if function.creating == creating {
			function.creating = nil
			close(creating)
		}
		function.mu.Unlock()
		return sandboxID, err
	}
}

func (function *RemoteFunction[Result]) provisionSandbox(ctx context.Context) (string, error) {
	sandbox, err := function.client.CreateSandbox(ctx, function.sandboxRequest)
	if err != nil {
		return "", err
	}
	sandboxID := sandbox.Identifier()
	cleanup := true
	defer func() {
		if cleanup {
			function.cleanupSandboxes([]string{sandboxID})
		}
	}()

	timeout := remoteRuntimeCheckTimeout
	check, err := function.client.ExecCapture(ctx, sandboxID, ExecRequest{
		Command: []string{"node", "--version"},
		Timeout: &timeout,
	})
	if err != nil {
		return "", err
	}
	if check.ExitCode != 0 {
		message := strings.TrimSpace(string(check.Stderr))
		if message == "" {
			message = "remote function image does not provide Node.js"
		}
		return "", &RemoteFunctionError{RemoteType: "RuntimeUnavailable", Message: message}
	}
	if err := function.client.WriteFile(ctx, sandboxID, remoteFunctionRunnerPath, []byte(remoteFunctionRunner)); err != nil {
		return "", err
	}
	cleanup = false
	return sandboxID, nil
}

func (function *RemoteFunction[Result]) invoke(
	ctx context.Context,
	sandboxID string,
	encodedArguments json.RawMessage,
) (Result, error) {
	var zero Result
	sequence := function.sequence.Add(1)
	payloadPath := fmt.Sprintf("%s-%d.json", remoteFunctionPayloadPath, sequence)
	payload, err := json.Marshal(struct {
		Source     string          `json:"source"`
		ExportName string          `json:"exportName"`
		Arguments  json.RawMessage `json:"args"`
	}{
		Source:     function.source.Source,
		ExportName: function.source.ExportName,
		Arguments:  encodedArguments,
	})
	if err != nil {
		return zero, fmt.Errorf("vmon: encode remote function invocation: %w", err)
	}
	if err := function.client.WriteFile(ctx, sandboxID, payloadPath, payload); err != nil {
		return zero, err
	}
	defer function.cleanupPayload(sandboxID, payloadPath)

	capture, err := function.client.ExecCapture(ctx, sandboxID, ExecRequest{
		Command: []string{"node", remoteFunctionRunnerPath, payloadPath},
		Timeout: cloneFloatPointer(function.invocationTimeout),
	})
	if err != nil {
		return zero, err
	}
	if capture.ExitCode != 0 {
		message := strings.TrimSpace(string(capture.Stderr))
		if message == "" {
			message = fmt.Sprintf("remote function runner exited with %d", capture.ExitCode)
		}
		return zero, &RemoteFunctionError{RemoteType: "RunnerProcessError", Message: message}
	}

	var response struct {
		OK     *bool           `json:"ok"`
		Result json.RawMessage `json:"result"`
		Stdout string          `json:"stdout"`
		Error  *struct {
			Type    string `json:"type"`
			Message string `json:"message"`
			Stack   string `json:"stack"`
		} `json:"error"`
	}
	if err := decodeRemoteJSON(capture.Stdout, &response); err != nil {
		return zero, &RemoteFunctionError{
			RemoteType: "ProtocolError",
			Message:    "remote function returned an invalid response",
			Cause:      err,
		}
	}
	if response.OK == nil {
		return zero, &RemoteFunctionError{
			RemoteType: "ProtocolError",
			Message:    "remote function response did not include ok",
		}
	}
	if response.Stdout != "" {
		function.stdoutMu.Lock()
		_, err := io.WriteString(function.stdout, response.Stdout)
		function.stdoutMu.Unlock()
		if err != nil {
			return zero, fmt.Errorf("vmon: forward remote function stdout: %w", err)
		}
	}
	if !*response.OK {
		if response.Error == nil {
			return zero, &RemoteFunctionError{
				RemoteType: "ProtocolError",
				Message:    "remote function failure did not include error details",
			}
		}
		return zero, &RemoteFunctionError{
			RemoteType:  response.Error.Type,
			Message:     response.Error.Message,
			RemoteStack: response.Error.Stack,
		}
	}
	if response.Result == nil {
		return zero, &RemoteFunctionError{
			RemoteType: "ProtocolError",
			Message:    "remote function success did not include a result",
		}
	}
	if err := decodeRemoteJSON(response.Result, &zero); err != nil {
		return zero, &RemoteFunctionError{
			RemoteType: "ProtocolError",
			Message:    "remote function result did not match the requested Go type",
			Cause:      err,
		}
	}
	return zero, nil
}

func (function *RemoteFunction[Result]) cleanupPayload(sandboxID, path string) {
	ctx, cancel := context.WithTimeout(context.Background(), remoteCleanupTimeout)
	defer cancel()
	_ = function.client.DeleteFile(ctx, sandboxID, path, false)
}

func (function *RemoteFunction[Result]) cleanupSandboxes(sandboxIDs []string) {
	if len(sandboxIDs) == 0 {
		return
	}
	var cleanups sync.WaitGroup
	cleanups.Add(len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		go func() {
			defer cleanups.Done()
			ctx, cancel := context.WithTimeout(context.Background(), remoteCleanupTimeout)
			defer cancel()
			_, err := function.client.TerminateSandbox(ctx, sandboxID)
			if isRemoteNotFound(err) {
				return
			}
		}()
	}
	cleanups.Wait()
}

func checkedRemoteMapOptions(options []RemoteMapOptions) (RemoteMapOptions, error) {
	if len(options) == 0 {
		return RemoteMapOptions{Concurrency: defaultRemoteConcurrency, Order: RemoteInputOrder}, nil
	}
	if len(options) > 1 {
		return RemoteMapOptions{}, errors.New("vmon: remote map accepts at most one options value")
	}
	settings := options[0]
	if settings.Concurrency < 1 {
		return RemoteMapOptions{}, errors.New("vmon: remote map concurrency must be at least one")
	}
	if settings.Order != RemoteInputOrder && settings.Order != RemoteCompletionOrder {
		return RemoteMapOptions{}, errors.New("vmon: remote map result order is invalid")
	}
	return settings, nil
}

func encodeRemoteArguments(arguments []any) (json.RawMessage, error) {
	if arguments == nil {
		arguments = []any{}
	}
	encoded, err := json.Marshal(arguments)
	if err != nil {
		return nil, fmt.Errorf("vmon: remote function arguments must be JSON-serializable: %w", err)
	}
	var checked []json.RawMessage
	if err := json.Unmarshal(encoded, &checked); err != nil {
		return nil, fmt.Errorf("vmon: validate remote function arguments: %w", err)
	}
	return json.RawMessage(encoded), nil
}

func decodeRemoteJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func cloneRemoteSandboxRequest(request SandboxCreateRequest) (SandboxCreateRequest, error) {
	cloned := request
	cloned.Timeout = cloneFloatPointer(request.Timeout)
	if request.TimeoutSeconds != nil {
		value := *request.TimeoutSeconds
		cloned.TimeoutSeconds = &value
	}
	cloned.Env = maps.Clone(request.Env)
	cloned.Tags = maps.Clone(request.Tags)
	cloned.Volumes = maps.Clone(request.Volumes)
	cloned.Ports = slices.Clone(request.Ports)
	cloned.EgressAllow = slices.Clone(request.EgressAllow)
	cloned.EgressAllowDomains = slices.Clone(request.EgressAllowDomains)
	cloned.InboundCIDRAllowlist = slices.Clone(request.InboundCIDRAllowlist)
	cloned.Command = slices.Clone(request.Command)
	if request.Secrets != nil {
		cloned.Secrets = make([]Secret, len(request.Secrets))
		for index, secret := range request.Secrets {
			cloned.Secrets[index] = Secret{name: secret.name, values: maps.Clone(secret.values)}
		}
	}
	if request.ReadinessProbe != nil {
		encoded, err := json.Marshal(request.ReadinessProbe)
		if err != nil {
			return SandboxCreateRequest{}, fmt.Errorf("vmon: clone remote readiness probe: %w", err)
		}
		if err := json.Unmarshal(encoded, &cloned.ReadinessProbe); err != nil {
			return SandboxCreateRequest{}, fmt.Errorf("vmon: clone remote readiness probe: %w", err)
		}
	}
	return cloned, nil
}

func cloneFloatPointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func isRemoteNotFound(err error) bool {
	var apiError *APIError
	return errors.As(err, &apiError) &&
		(apiError.StatusCode == http.StatusNotFound || apiError.Code == "not_found")
}
