package vmon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultTakeoverImage is the plain glibc guest used when no image source is set.
	// CGO_ENABLED=0 static worker builds run on any Linux image, including this one.
	DefaultTakeoverImage = "debian:stable-slim"

	takeoverWorkerGuestPath     = "/tmp/vmon-fn-worker"
	takeoverHelloTimeout        = 30 * time.Second
	takeoverMaxInfraRetries     = 3
	defaultTakeoverConcurrency  = 8
	takeoverChmodTimeoutSeconds = 30.0
	takeoverStderrTailLimit     = 4096
)

// ErrCallCancelled is returned by Call.Get after Cancel stopped the call.
var ErrCallCancelled = errors.New("vmon: remote function call was cancelled")

// CallTimeoutError reports an expired client-side per-call deadline.
// The worker session is killed on expiry; timeouts are retried under the Retries policy.
type CallTimeoutError struct {
	// Timeout is the configured per-call deadline.
	Timeout time.Duration
}

// Error implements error.
func (err *CallTimeoutError) Error() string {
	return fmt.Sprintf("vmon: remote function call timed out after %s", err.Timeout)
}

// Unwrap makes the error match errors.Is(err, context.DeadlineExceeded).
func (err *CallTimeoutError) Unwrap() error { return context.DeadlineExceeded }

// Retries is a Modal-style user retry policy for remote calls.
//
// Zero fields take defaults: BackoffCoefficient 2.0, InitialDelay 1s, MaxDelay 60s.
// The delay before retry n (1-based) is min(InitialDelay*BackoffCoefficient**(n-1), MaxDelay).
// User errors and per-call timeouts are retried; cancellation never is. Infrastructure
// failures (worker died without a result) get an independent, always-on budget of
// three retries, each on a fresh sandbox.
type Retries struct {
	// MaxRetries is the number of retries after the initial attempt; must be >= 0.
	MaxRetries int
	// BackoffCoefficient multiplies the delay each retry; 0 defaults to 2.0.
	BackoffCoefficient float64
	// InitialDelay is the delay before the first retry; 0 defaults to 1s.
	InitialDelay time.Duration
	// MaxDelay caps the delay; 0 defaults to 60s.
	MaxDelay time.Duration
}

// RetryCount is the integer retry form: count retries with a fixed 1s delay.
func RetryCount(count int) *Retries {
	return &Retries{MaxRetries: count, BackoffCoefficient: 1.0}
}

func (retries Retries) normalized() (Retries, error) {
	if retries.BackoffCoefficient == 0 {
		retries.BackoffCoefficient = 2.0
	}
	if retries.InitialDelay == 0 {
		retries.InitialDelay = time.Second
	}
	if retries.MaxDelay == 0 {
		retries.MaxDelay = time.Minute
	}
	switch {
	case retries.MaxRetries < 0:
		return Retries{}, errors.New("vmon: retries: MaxRetries must be non-negative")
	case retries.BackoffCoefficient < 1 || retries.BackoffCoefficient > 10:
		return Retries{}, errors.New("vmon: retries: BackoffCoefficient must be between 1 and 10")
	case retries.InitialDelay < 0 || retries.InitialDelay > time.Minute:
		return Retries{}, errors.New("vmon: retries: InitialDelay must be between 0 and 60s")
	case retries.MaxDelay < time.Second || retries.MaxDelay > time.Minute:
		return Retries{}, errors.New("vmon: retries: MaxDelay must be between 1s and 60s")
	}
	return retries, nil
}

func (retries Retries) delayFor(retry int) time.Duration {
	delay := float64(retries.InitialDelay) * math.Pow(retries.BackoffCoefficient, float64(retry-1))
	if delay > float64(retries.MaxDelay) {
		return retries.MaxDelay
	}
	return time.Duration(delay)
}

// FunctionOptions configures the sandboxes and calls owned by a takeover function.
type FunctionOptions struct {
	// Sandbox is the create request used for the cached and map-worker sandboxes.
	Sandbox SandboxCreateRequest
	// WorkerBinary is a path to a GOOS=linux build of this program, uploaded once per
	// sandbox and re-executed with VMON_TAKEOVER=1. Empty defaults to os.Executable()
	// when the host itself is linux; on other hosts it must be set explicitly.
	WorkerBinary string
	// Retries is the user retry policy; nil disables user retries.
	// RetryCount(n) gives the fixed-1s-delay integer form.
	Retries *Retries
	// CallTimeout is the client-side deadline for each call; 0 means no deadline.
	// On expiry the worker session is killed and a *CallTimeoutError is returned.
	CallTimeout time.Duration
	// Pool requests a server-side warm pool of this many VMs for the sandbox image,
	// making fresh worker sandboxes (Map, infra retries) start hot.
	Pool uint32
	// Stdout receives live remote standard output; nil uses os.Stdout.
	Stdout io.Writer
	// Stderr receives live remote standard error; nil uses os.Stderr.
	Stderr io.Writer
}

// Function invokes one Register-ed Go function of this binary inside vmon sandboxes.
//
// Remote reuses one lazily created sandbox with a persistent worker session (warm
// calls dispatch without process spawn). Spawn opens a dedicated concurrent session
// on the same sandbox. Map fans out over ephemeral worker sandboxes.
type Function[Result any] struct {
	client      *Client
	name        string
	request     SandboxCreateRequest
	binaryPath  string
	retries     *Retries
	callTimeout time.Duration
	stdout      io.Writer
	stderr      io.Writer
	outMu       sync.Mutex
	ids         atomic.Uint64

	binaryOnce sync.Once
	binaryData []byte
	binaryErr  error

	callMu      sync.Mutex // one in-flight Remote call on the cached session
	provisionMu sync.Mutex // serializes cached-sandbox creation

	mu      sync.Mutex
	host    *takeoverHost
	session *takeoverWorker
	spawned map[*takeoverWorker]struct{}
}

// NewFunction creates a typed handle for the Register-ed function named name.
//
// The name must already be registered in this process: the worker binary is (a linux
// build of) the running program, so an unregistered name could never be dispatched.
func NewFunction[Result any](client *Client, name string, options ...FunctionOptions) (*Function[Result], error) {
	if client == nil {
		return nil, errors.New("vmon: takeover function client must not be nil")
	}
	if len(options) > 1 {
		return nil, errors.New("vmon: takeover function accepts at most one options value")
	}
	if lookupRegistered(name) == nil {
		return nil, fmt.Errorf(
			"vmon: takeover function %q is not registered; call vmon.Register(%q, fn) before vmon.Takeover() in main",
			name, name,
		)
	}
	var configured FunctionOptions
	if len(options) == 1 {
		configured = options[0]
	}
	binaryPath := configured.WorkerBinary
	if binaryPath == "" {
		if runtime.GOOS != "linux" {
			return nil, fmt.Errorf(
				"vmon: takeover functions upload os.Executable() as the guest worker, which only matches the "+
					"always-linux guest when the host is linux (host is %s); build this program with "+
					"GOOS=linux CGO_ENABLED=0 and set FunctionOptions.WorkerBinary to that build",
				runtime.GOOS,
			)
		}
		executable, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("vmon: resolve worker binary: %w", err)
		}
		binaryPath = executable
	}
	if configured.CallTimeout < 0 {
		return nil, errors.New("vmon: takeover function CallTimeout must be non-negative")
	}
	request, err := cloneRemoteSandboxRequest(configured.Sandbox)
	if err != nil {
		return nil, err
	}
	if request.Image == "" && request.Template == "" && request.Dockerfile == "" {
		request.Image = DefaultTakeoverImage
	}
	if configured.Pool != 0 {
		request.PoolSize = configured.Pool
	}
	var retries *Retries
	if configured.Retries != nil {
		normalized, err := configured.Retries.normalized()
		if err != nil {
			return nil, err
		}
		retries = &normalized
	}
	stdout := configured.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := configured.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	return &Function[Result]{
		client:      client,
		name:        name,
		request:     request,
		binaryPath:  binaryPath,
		retries:     retries,
		callTimeout: configured.CallTimeout,
		stdout:      stdout,
		stderr:      stderr,
		spawned:     map[*takeoverWorker]struct{}{},
	}, nil
}

// takeoverHost is one worker-carrying sandbox shared by reference-counted sessions.
type takeoverHost struct {
	client  *Client
	sandbox *Sandbox

	// execMu serializes Exec dials: *Sandbox mutates its endpoint affinity on dial.
	execMu sync.Mutex

	mu         sync.Mutex
	refs       int
	condemned  bool
	terminated bool
}

func (host *takeoverHost) retain() {
	host.mu.Lock()
	host.refs++
	host.mu.Unlock()
}

func (host *takeoverHost) release() {
	host.mu.Lock()
	host.refs--
	doomed := host.condemned && host.refs <= 0 && !host.terminated
	if doomed {
		host.terminated = true
	}
	host.mu.Unlock()
	if doomed {
		host.terminateAsync()
	}
}

// condemn marks the sandbox dead; it is terminated once the last session releases it.
func (host *takeoverHost) condemn() {
	host.mu.Lock()
	host.condemned = true
	doomed := host.refs <= 0 && !host.terminated
	if doomed {
		host.terminated = true
	}
	host.mu.Unlock()
	if doomed {
		host.terminateAsync()
	}
}

func (host *takeoverHost) terminateAsync() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), remoteCleanupTimeout)
		defer cancel()
		err := host.client.Sandboxes.Ref(host.sandbox.ID).Terminate(ctx)
		_ = err
	}()
}

func (host *takeoverHost) terminateNow(ctx context.Context) error {
	host.mu.Lock()
	host.condemned = true
	already := host.terminated
	host.terminated = true
	host.mu.Unlock()
	if already {
		return nil
	}
	err := host.client.Sandboxes.Ref(host.sandbox.ID).Terminate(ctx)
	if isRemoteNotFound(err) {
		err = nil
	}
	return err
}

// takeoverWorker is the client half of one persistent worker session.
type takeoverWorker struct {
	host       *takeoverHost
	process    *Process
	goVersion  string
	buffer     []byte
	stderrTail []byte
	closed     atomic.Bool
}

func (worker *takeoverWorker) close() {
	if worker == nil || worker.closed.Swap(true) {
		return
	}
	_ = worker.process.Close()
	worker.host.release()
}

type takeoverFrame struct {
	Event     string          `json:"event"`
	ID        uint64          `json:"id"`
	Go        string          `json:"go"`
	Stream    string          `json:"stream"`
	Data      string          `json:"data"`
	JSON      json.RawMessage `json:"json"`
	Pickle    *string         `json:"pickle"`
	File      *string         `json:"file"`
	EType     string          `json:"etype"`
	Message   string          `json:"message"`
	Traceback string          `json:"traceback"`
}

func (worker *takeoverWorker) nextFrame(ctx context.Context, stderrSink func(string)) (takeoverFrame, error) {
	for {
		if index := bytes.IndexByte(worker.buffer, '\n'); index >= 0 {
			line := bytes.Clone(worker.buffer[:index])
			worker.buffer = append(worker.buffer[:0], worker.buffer[index+1:]...)
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var frame takeoverFrame
			if err := json.Unmarshal(line, &frame); err != nil {
				worker.close()
				return takeoverFrame{}, &RemoteFunctionError{
					RemoteType: "ProtocolError",
					Message:    "takeover worker emitted a malformed frame",
					Cause:      err,
				}
			}
			return frame, nil
		}
		event, err := worker.process.Receive(ctx)
		if err != nil {
			worker.close()
			return takeoverFrame{}, err
		}
		if event.Exit != nil {
			worker.close()
			return takeoverFrame{}, worker.deathError(event.Exit)
		}
		switch event.Stream {
		case StreamStdout:
			worker.buffer = append(worker.buffer, event.Data...)
		case StreamStderr:
			worker.stderrTail = append(worker.stderrTail, event.Data...)
			if len(worker.stderrTail) > takeoverStderrTailLimit {
				worker.stderrTail = worker.stderrTail[len(worker.stderrTail)-takeoverStderrTailLimit:]
			}
			if stderrSink != nil {
				stderrSink(string(event.Data))
			}
		}
	}
}

func (worker *takeoverWorker) deathError(exit *ExecExit) error {
	detail := fmt.Sprintf("takeover worker exited with code %d", exit.Code)
	if exit.Signal != nil {
		detail = fmt.Sprintf("%s (signal %d)", detail, *exit.Signal)
	}
	if tail := strings.TrimSpace(string(worker.stderrTail)); tail != "" {
		detail += ": " + tail
	}
	return &RemoteFunctionError{RemoteType: "WorkerExit", Message: detail}
}

// takeoverInfraError marks failures where the session died without a user-level
// result; they are retried on a fresh sandbox independent of the user policy.
type takeoverInfraError struct{ err error }

func (err *takeoverInfraError) Error() string { return err.err.Error() }
func (err *takeoverInfraError) Unwrap() error { return err.err }

func takeoverInfra(err error) error { return &takeoverInfraError{err} }

type takeoverArgsValue struct {
	JSON json.RawMessage `json:"json"`
}

func (function *Function[Result]) workerBinaryBytes() ([]byte, error) {
	function.binaryOnce.Do(func() {
		function.binaryData, function.binaryErr = os.ReadFile(function.binaryPath)
		if function.binaryErr != nil {
			function.binaryErr = fmt.Errorf("vmon: read worker binary %s: %w", function.binaryPath, function.binaryErr)
		}
	})
	return function.binaryData, function.binaryErr
}

// provisionHost creates a sandbox, uploads the worker binary, and makes it executable.
func (function *Function[Result]) provisionHost(ctx context.Context) (*takeoverHost, error) {
	binary, err := function.workerBinaryBytes()
	if err != nil {
		return nil, err
	}
	sandbox, err := function.client.Sandboxes.Create(ctx, function.request)
	if err != nil {
		return nil, err
	}
	host := &takeoverHost{client: function.client, sandbox: sandbox}
	cleanup := true
	defer func() {
		if cleanup {
			host.condemn()
		}
	}()
	if err := sandbox.Files.Write(ctx, takeoverWorkerGuestPath, binary); err != nil {
		return nil, err
	}
	timeout := takeoverChmodTimeoutSeconds
	chmod, err := sandbox.Run(ctx, ExecRequest{
		Command: []string{"chmod", "+x", takeoverWorkerGuestPath},
		Timeout: &timeout,
	})
	if err != nil {
		return nil, err
	}
	if chmod.ExitCode != 0 {
		return nil, &RemoteFunctionError{
			RemoteType: "WorkerStartError",
			Message:    "chmod +x on the uploaded worker binary failed: " + strings.TrimSpace(string(chmod.Stderr)),
		}
	}
	cleanup = false
	return host, nil
}

// startSession execs the uploaded worker with VMON_TAKEOVER=1 and awaits its hello.
func startTakeoverSession(ctx context.Context, host *takeoverHost) (*takeoverWorker, error) {
	host.execMu.Lock()
	process, err := host.sandbox.Exec(ctx, ExecRequest{
		Command: []string{takeoverWorkerGuestPath},
		Env:     map[string]string{takeoverModeEnv: "1"},
		// No exec timeout: the session lives until shutdown or exec-stream close.
	})
	host.execMu.Unlock()
	if err != nil {
		return nil, err
	}
	host.retain()
	worker := &takeoverWorker{host: host, process: process}
	helloCtx, cancel := context.WithTimeout(ctx, takeoverHelloTimeout)
	defer cancel()
	frame, err := worker.nextFrame(helloCtx, nil)
	if err != nil {
		worker.close()
		return nil, fmt.Errorf(
			"vmon: takeover worker failed to start (is WorkerBinary a CGO_ENABLED=0 GOOS=linux build of this program?): %w",
			err,
		)
	}
	if frame.Event != "hello" || frame.Go == "" {
		worker.close()
		return nil, &RemoteFunctionError{
			RemoteType: "ProtocolError",
			Message:    "takeover worker sent an unexpected first frame",
		}
	}
	worker.goVersion = frame.Go
	return worker, nil
}

func (function *Function[Result]) ensureHost(ctx context.Context) (*takeoverHost, error) {
	function.provisionMu.Lock()
	defer function.provisionMu.Unlock()
	function.mu.Lock()
	host := function.host
	function.mu.Unlock()
	if host != nil {
		return host, nil
	}
	host, err := function.provisionHost(ctx)
	if err != nil {
		return nil, takeoverInfra(err)
	}
	function.mu.Lock()
	function.host = host
	function.mu.Unlock()
	return host, nil
}

func (function *Function[Result]) dropHost(host *takeoverHost) {
	function.mu.Lock()
	if function.host == host {
		function.host = nil
	}
	function.mu.Unlock()
	host.condemn()
}

func (function *Function[Result]) ensureSession(ctx context.Context) (*takeoverWorker, error) {
	function.mu.Lock()
	session := function.session
	function.mu.Unlock()
	if session != nil && !session.closed.Load() {
		return session, nil
	}
	host, err := function.ensureHost(ctx)
	if err != nil {
		return nil, err
	}
	worker, err := startTakeoverSession(ctx, host)
	if err != nil {
		function.dropHost(host)
		return nil, takeoverInfra(err)
	}
	function.mu.Lock()
	function.session = worker
	function.mu.Unlock()
	return worker, nil
}

func (function *Function[Result]) discardSession(worker *takeoverWorker, sandboxToo bool) {
	function.mu.Lock()
	if function.session == worker {
		function.session = nil
	}
	host := worker.host
	if sandboxToo && function.host == host {
		function.host = nil
	}
	function.mu.Unlock()
	worker.close()
	if sandboxToo {
		host.condemn()
	}
}

func (function *Function[Result]) writeOutput(stream, data string) {
	if data == "" {
		return
	}
	function.outMu.Lock()
	defer function.outMu.Unlock()
	writer := function.stdout
	if stream == "stderr" {
		writer = function.stderr
	}
	_, _ = io.WriteString(writer, data)
}

// callWorker performs one op round trip on an established session.
func (function *Function[Result]) callWorker(
	parent context.Context,
	worker *takeoverWorker,
	encodedArguments json.RawMessage,
) (Result, error) {
	var zero Result
	ctx := parent
	if function.callTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, function.callTimeout)
		defer cancel()
	}
	id := function.ids.Add(1)
	op, err := json.Marshal(struct {
		Op   string            `json:"op"`
		ID   uint64            `json:"id"`
		Name string            `json:"name"`
		Args takeoverArgsValue `json:"args"`
		Mode string            `json:"mode"`
	}{"call", id, function.name, takeoverArgsValue{encodedArguments}, "value"})
	if err != nil {
		return zero, fmt.Errorf("vmon: encode takeover call: %w", err)
	}
	if err := worker.process.WriteStdin(ctx, append(op, '\n')); err != nil {
		worker.close()
		return zero, function.sessionFailure(parent, ctx, err)
	}
	sink := func(data string) { function.writeOutput("stderr", data) }
	for {
		frame, err := worker.nextFrame(ctx, sink)
		if err != nil {
			return zero, function.sessionFailure(parent, ctx, err)
		}
		switch frame.Event {
		case "out":
			function.writeOutput(frame.Stream, frame.Data)
		case "result":
			if len(frame.JSON) == 0 || string(frame.JSON) == "null" {
				return zero, nil
			}
			if err := decodeRemoteJSON(frame.JSON, &zero); err != nil {
				return zero, &RemoteFunctionError{
					RemoteType: "ProtocolError",
					Message:    "remote function result did not match the requested Go type",
					Cause:      err,
				}
			}
			return zero, nil
		case "error":
			return zero, &RemoteFunctionError{
				RemoteType:  frame.EType,
				Message:     frame.Message,
				RemoteStack: frame.Traceback,
			}
		default:
			worker.close()
			return zero, takeoverInfra(&RemoteFunctionError{
				RemoteType: "ProtocolError",
				Message:    fmt.Sprintf("takeover worker sent unexpected %q event", frame.Event),
			})
		}
	}
}

// sessionFailure classifies a dead-session error: caller cancellation, per-call
// timeout, or infrastructure failure.
func (function *Function[Result]) sessionFailure(parent, callCtx context.Context, err error) error {
	if parentErr := parent.Err(); parentErr != nil {
		return parentErr
	}
	if callCtx != parent && errors.Is(callCtx.Err(), context.DeadlineExceeded) {
		return &CallTimeoutError{Timeout: function.callTimeout}
	}
	return takeoverInfra(err)
}

// execute runs one logical call with user retries and always-on infra retries.
//
// discard(worker, sandboxToo) removes a dead session; sandboxToo also retires its
// sandbox so the next acquire provisions a fresh one.
func (function *Function[Result]) execute(
	ctx context.Context,
	encodedArguments json.RawMessage,
	acquire func(context.Context) (*takeoverWorker, error),
	discard func(worker *takeoverWorker, sandboxToo bool),
) (Result, error) {
	var zero Result
	userRetries := 0
	infraRetries := 0
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		worker, err := acquire(ctx)
		if err == nil {
			var result Result
			result, err = function.callWorker(ctx, worker, encodedArguments)
			if err == nil {
				return result, nil
			}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if worker != nil {
				discard(worker, false)
			}
			return zero, ctxErr
		}
		var infraErr *takeoverInfraError
		var timeoutErr *CallTimeoutError
		var remoteErr *RemoteFunctionError
		switch {
		case errors.As(err, &infraErr):
			if worker != nil {
				discard(worker, true)
			}
			infraRetries++
			if infraRetries > takeoverMaxInfraRetries {
				return zero, infraErr.err
			}
			continue
		case errors.As(err, &timeoutErr):
			// The session was killed by the expired deadline; the sandbox stays warm.
			discard(worker, false)
		case errors.As(err, &remoteErr):
			// User-level failure: the session survived and stays warm.
		default:
			if worker != nil {
				discard(worker, false)
			}
			return zero, err
		}
		if function.retries == nil || userRetries >= function.retries.MaxRetries {
			return zero, err
		}
		userRetries++
		select {
		case <-time.After(function.retries.delayFor(userRetries)):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}

// Remote invokes the function in one lazily created, reusable sandbox and returns its result.
func (function *Function[Result]) Remote(ctx context.Context, arguments ...any) (Result, error) {
	var zero Result
	if function == nil {
		return zero, errors.New("vmon: takeover function is nil")
	}
	encoded, err := encodeRemoteArguments(arguments)
	if err != nil {
		return zero, err
	}
	function.callMu.Lock()
	defer function.callMu.Unlock()
	return function.execute(ctx, encoded, function.ensureSession, function.discardSession)
}

// Call is the handle of one Spawn-ed remote invocation.
type Call[Result any] struct {
	done      chan struct{}
	result    Result
	err       error
	cancel    context.CancelFunc
	cancelled atomic.Bool
}

// Get waits for the call and returns its result; ctx bounds only the wait.
func (call *Call[Result]) Get(ctx context.Context) (Result, error) {
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		var zero Result
		return zero, ctx.Err()
	}
}

// Done reports whether the call has finished.
func (call *Call[Result]) Done() bool {
	select {
	case <-call.done:
		return true
	default:
		return false
	}
}

// Cancel stops the call by closing its dedicated worker session (the server
// SIGTERMs the guest process). A cancelled Get returns ErrCallCancelled.
func (call *Call[Result]) Cancel() {
	if call == nil {
		return
	}
	call.cancelled.Store(true)
	call.cancel()
}

// Gather waits for every call in order and returns their results, or the first error.
func Gather[Result any](ctx context.Context, calls ...*Call[Result]) ([]Result, error) {
	results := make([]Result, len(calls))
	for index, call := range calls {
		result, err := call.Get(ctx)
		if err != nil {
			return nil, err
		}
		results[index] = result
	}
	return results, nil
}

func (function *Function[Result]) newSpawnSession(ctx context.Context) (*takeoverWorker, error) {
	host, err := function.ensureHost(ctx)
	if err != nil {
		return nil, err
	}
	worker, err := startTakeoverSession(ctx, host)
	if err != nil {
		function.dropHost(host)
		return nil, takeoverInfra(err)
	}
	function.mu.Lock()
	function.spawned[worker] = struct{}{}
	function.mu.Unlock()
	return worker, nil
}

func (function *Function[Result]) dropSpawnSession(worker *takeoverWorker, sandboxToo bool) {
	function.mu.Lock()
	delete(function.spawned, worker)
	host := worker.host
	if sandboxToo && function.host == host {
		function.host = nil
	}
	function.mu.Unlock()
	worker.close()
	if sandboxToo {
		host.condemn()
	}
}

// Spawn starts the call on a dedicated concurrent worker session of the cached
// sandbox and returns immediately. Cancel closes only that session.
func (function *Function[Result]) Spawn(ctx context.Context, arguments ...any) (*Call[Result], error) {
	if function == nil {
		return nil, errors.New("vmon: takeover function is nil")
	}
	encoded, err := encodeRemoteArguments(arguments)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithCancel(ctx)
	call := &Call[Result]{done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(call.done)
		defer cancel()
		var worker *takeoverWorker
		acquire := func(ctx context.Context) (*takeoverWorker, error) {
			if worker != nil && !worker.closed.Load() {
				return worker, nil
			}
			session, err := function.newSpawnSession(ctx)
			if err != nil {
				return nil, err
			}
			worker = session
			return session, nil
		}
		discard := func(session *takeoverWorker, sandboxToo bool) {
			function.dropSpawnSession(session, sandboxToo)
			if worker == session {
				worker = nil
			}
		}
		result, err := function.execute(callCtx, encoded, acquire, discard)
		if worker != nil {
			function.dropSpawnSession(worker, false)
		}
		if err != nil && call.cancelled.Load() && errors.Is(err, context.Canceled) {
			err = ErrCallCancelled
		}
		call.result, call.err = result, err
	}()
	return call, nil
}

// MapOptions configures one bounded ephemeral worker pool.
type MapOptions struct {
	// Concurrency is the maximum number of worker sandboxes; 0 defaults to 8.
	Concurrency int
}

// MapResult is one TryMap outcome: a value or that input's error.
type MapResult[Result any] struct {
	// Value is the call result when Err is nil.
	Value Result
	// Err is the failure for this input, when any.
	Err error
}

// Map invokes the function once per argument tuple across ephemeral worker
// sandboxes and returns results in input order, failing fast on the first error.
func (function *Function[Result]) Map(
	ctx context.Context,
	inputs [][]any,
	options ...MapOptions,
) ([]Result, error) {
	outcomes, err := function.mapCalls(ctx, inputs, options, true)
	if err != nil {
		return nil, err
	}
	results := make([]Result, len(outcomes))
	for index, outcome := range outcomes {
		results[index] = outcome.Value
	}
	return results, nil
}

// TryMap is Map with per-input error collection: every input runs to completion
// and failures are reported in their slot instead of aborting the batch.
func (function *Function[Result]) TryMap(
	ctx context.Context,
	inputs [][]any,
	options ...MapOptions,
) ([]MapResult[Result], error) {
	return function.mapCalls(ctx, inputs, options, false)
}

func (function *Function[Result]) mapCalls(
	ctx context.Context,
	inputs [][]any,
	options []MapOptions,
	failFast bool,
) ([]MapResult[Result], error) {
	if function == nil {
		return nil, errors.New("vmon: takeover function is nil")
	}
	if len(options) > 1 {
		return nil, errors.New("vmon: takeover map accepts at most one options value")
	}
	concurrency := defaultTakeoverConcurrency
	if len(options) == 1 && options[0].Concurrency != 0 {
		concurrency = options[0].Concurrency
	}
	if concurrency < 1 {
		return nil, errors.New("vmon: takeover map concurrency must be at least one")
	}
	encoded := make([]json.RawMessage, len(inputs))
	for index, arguments := range inputs {
		var err error
		encoded[index], err = encodeRemoteArguments(arguments)
		if err != nil {
			return nil, fmt.Errorf("vmon: takeover map arguments at index %d: %w", index, err)
		}
	}
	if len(encoded) == 0 {
		return []MapResult[Result]{}, nil
	}

	mapCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	indices := make(chan int)
	go func() {
		defer close(indices)
		for index := range encoded {
			select {
			case indices <- index:
			case <-mapCtx.Done():
				return
			}
		}
	}()

	outcomes := make([]MapResult[Result], len(encoded))
	var failOnce sync.Once
	var failErr error
	var workers sync.WaitGroup
	for range min(concurrency, len(encoded)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var worker *takeoverWorker
			defer func() {
				if worker != nil {
					host := worker.host
					worker.close()
					host.condemn()
				}
			}()
			acquire := func(ctx context.Context) (*takeoverWorker, error) {
				if worker != nil && !worker.closed.Load() {
					return worker, nil
				}
				host, err := function.provisionHost(ctx)
				if err != nil {
					return nil, takeoverInfra(err)
				}
				session, err := startTakeoverSession(ctx, host)
				if err != nil {
					host.condemn()
					return nil, takeoverInfra(err)
				}
				worker = session
				return session, nil
			}
			discard := func(session *takeoverWorker, sandboxToo bool) {
				host := session.host
				session.close()
				// Map sandboxes are exclusively owned: retire them with their session.
				host.condemn()
				if worker == session {
					worker = nil
				}
				_ = sandboxToo
			}
			for index := range indices {
				value, err := function.execute(mapCtx, encoded[index], acquire, discard)
				outcomes[index] = MapResult[Result]{Value: value, Err: err}
				if err != nil && failFast {
					failOnce.Do(func() {
						failErr = fmt.Errorf("vmon: takeover map input %d: %w", index, err)
					})
					cancel()
					return
				}
				if mapCtx.Err() != nil {
					return
				}
			}
		}()
	}
	workers.Wait()
	if failFast && failErr != nil {
		return nil, failErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return outcomes, nil
}

// Terminate closes the cached and spawned worker sessions and terminates the
// cached sandbox; repeated and concurrent calls are harmless.
func (function *Function[Result]) Terminate(ctx context.Context) error {
	if function == nil {
		return nil
	}
	function.mu.Lock()
	session := function.session
	function.session = nil
	host := function.host
	function.host = nil
	spawned := make([]*takeoverWorker, 0, len(function.spawned))
	for worker := range function.spawned {
		spawned = append(spawned, worker)
	}
	clear(function.spawned)
	function.mu.Unlock()

	session.close()
	for _, worker := range spawned {
		worker.close()
	}
	if host != nil {
		return host.terminateNow(ctx)
	}
	return nil
}
