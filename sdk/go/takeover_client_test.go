package vmon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/can1357/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// takeoverStub is a fake vmond: gRPC handlers answer the sandbox lifecycle and
// the Exec stream bridges to a real re-exec of this test binary in worker
// mode, exercising the full session protocol over real pipes.
type takeoverStub struct {
	pb.UnimplementedSandboxServiceServer
	t        *testing.T
	listener *bufconn.Listener

	mu         sync.Mutex
	creates    int
	sessions   int
	uploads    map[string][]byte
	terminated []string
}

func startTakeoverStub(t *testing.T) *takeoverStub {
	t.Helper()
	stub := &takeoverStub{t: t, uploads: map[string][]byte{}, listener: bufconn.Listen(1 << 20)}
	server := grpc.NewServer()
	pb.RegisterSandboxServiceServer(server, stub)
	go func() { _ = server.Serve(stub.listener) }()
	t.Cleanup(server.Stop)
	return stub
}

func (stub *takeoverStub) counts() (creates, sessions int, terminated []string) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.creates, stub.sessions, append([]string(nil), stub.terminated...)
}

func (stub *takeoverStub) Create(_ context.Context, _ *pb.CreateSandboxRequest) (*pb.JsonView, error) {
	stub.mu.Lock()
	stub.creates++
	id := fmt.Sprintf("to-%d", stub.creates)
	stub.mu.Unlock()
	return &pb.JsonView{Json: fmt.Sprintf(`{"id":%q,"status":"running"}`, id)}, nil
}

func (stub *takeoverStub) FileWrite(_ context.Context, request *pb.FileWriteRequest) (*pb.Ok, error) {
	stub.mu.Lock()
	stub.uploads[request.GetPath()] = bytes.Clone(request.GetData())
	stub.mu.Unlock()
	return &pb.Ok{}, nil
}

func (stub *takeoverStub) ExecCapture(_ context.Context, request *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error) {
	cmd := request.GetExec().GetCmd()
	if len(cmd) == 0 || cmd[0] != "chmod" {
		stub.t.Errorf("unexpected captured exec %v", cmd)
	}
	return &pb.ExecCaptureResponse{Code: 0}, nil
}

func (stub *takeoverStub) Terminate(_ context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
	stub.mu.Lock()
	stub.terminated = append(stub.terminated, ref.GetId())
	stub.mu.Unlock()
	return &pb.JsonView{Json: "{}"}, nil
}

// Exec runs this test binary in takeover worker mode and relays the exec
// stream frames to its pipes, mirroring the real guest agent.
func (stub *takeoverStub) Exec(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil || len(start.GetCmd()) != 1 || start.GetCmd()[0] != takeoverWorkerGuestPath ||
		start.GetEnv()[takeoverModeEnv] != "1" || start.Timeout != nil || start.GetSandboxId() == "" {
		stub.t.Errorf("unexpected exec start %v", start)
		return status.Error(codes.InvalidArgument, "unexpected exec start frame")
	}
	stub.mu.Lock()
	stub.sessions++
	stub.mu.Unlock()

	command := exec.Command(os.Args[0])
	command.Env = append(os.Environ(), takeoverModeEnv+"=1")
	stdin, err := command.StdinPipe()
	if err != nil {
		stub.t.Error(err)
		return status.Error(codes.Internal, "stdin pipe")
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		stub.t.Error(err)
		return status.Error(codes.Internal, "stdout pipe")
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		stub.t.Error(err)
		return status.Error(codes.Internal, "stderr pipe")
	}
	if err := command.Start(); err != nil {
		stub.t.Error(err)
		return status.Error(codes.Internal, "start worker")
	}

	var writeMu sync.Mutex
	writeFrame := func(output *pb.ExecOutput) {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = stream.Send(output)
	}

	// Client → worker stdin; a dropped stream kills the worker (server parity).
	go func() {
		defer stdin.Close()
		for {
			input, err := stream.Recv()
			if err != nil {
				_ = command.Process.Kill()
				return
			}
			if input.GetEof() != nil {
				return
			}
			if chunk := input.GetStdin(); len(chunk) != 0 {
				if _, err := stdin.Write(chunk); err != nil {
					return
				}
			}
		}
	}()

	var pumps sync.WaitGroup
	pump := func(name pb.Stream, reader io.Reader) {
		defer pumps.Done()
		buffer := make([]byte, 8192)
		for {
			count, err := reader.Read(buffer)
			if count > 0 {
				writeFrame(&pb.ExecOutput{Output: &pb.ExecOutput_Chunk{
					Chunk: &pb.Output{Stream: name, Data: bytes.Clone(buffer[:count])},
				}})
			}
			if err != nil {
				return
			}
		}
	}
	pumps.Add(2)
	go pump(pb.Stream_STREAM_STDOUT, stdout)
	go pump(pb.Stream_STREAM_STDERR, stderr)
	pumps.Wait()
	code := 0
	if err := command.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			code = -1
		}
	}
	writeFrame(&pb.ExecOutput{Output: &pb.ExecOutput_Exit{Exit: &pb.Exit{Code: int64(code)}}})
	return nil
}

func takeoverTestOptions(t *testing.T, extra FunctionOptions) FunctionOptions {
	t.Helper()
	// The stub always re-executes this test binary, so the uploaded worker
	// binary can be a small placeholder file.
	placeholder := filepath.Join(t.TempDir(), "worker-binary")
	if err := os.WriteFile(placeholder, []byte("placeholder worker binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	extra.WorkerBinary = placeholder
	if extra.Stdout == nil {
		extra.Stdout = io.Discard
	}
	if extra.Stderr == nil {
		extra.Stderr = io.Discard
	}
	return extra
}

func takeoverTestClient(t *testing.T, stub *takeoverStub) *Client {
	t.Helper()
	client, err := Connect("http://127.0.0.1:1", WithDiscovery(false), withGRPCDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return stub.listener.DialContext(ctx)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestTakeoverFunctionRemoteWarmReuse(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[int](client, "add", takeoverTestOptions(t, FunctionOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if result, err := function.Remote(ctx, 2, 3); err != nil || result != 5 {
		t.Fatalf("remote add = %d, %v", result, err)
	}
	if result, err := function.Remote(ctx, 40, 2); err != nil || result != 42 {
		t.Fatalf("remote add = %d, %v", result, err)
	}
	creates, sessions, _ := stub.counts()
	if creates != 1 || sessions != 1 {
		t.Fatalf("creates=%d sessions=%d, want warm reuse of one sandbox and one session", creates, sessions)
	}
	stub.mu.Lock()
	upload := stub.uploads[takeoverWorkerGuestPath]
	stub.mu.Unlock()
	if string(upload) != "placeholder worker binary" {
		t.Fatalf("uploaded worker binary = %q", upload)
	}
	if err := function.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, terminated := stub.counts(); len(terminated) != 1 || terminated[0] != "to-1" {
		t.Fatalf("terminated = %v", terminated)
	}
}

func TestTakeoverFunctionForwardsLiveOutput(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	var stdout, stderr bytes.Buffer
	function, err := NewFunction[int](client, "noisy", takeoverTestOptions(t, FunctionOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer function.Terminate(context.Background())
	if result, err := function.Remote(context.Background()); err != nil || result != 7 {
		t.Fatalf("remote noisy = %d, %v", result, err)
	}
	if !strings.Contains(stdout.String(), "hello from worker") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "warn from worker") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestTakeoverFunctionRemoteErrorKeepsSessionWarm(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[any](client, "fail", takeoverTestOptions(t, FunctionOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	defer function.Terminate(context.Background())
	_, err = function.Remote(context.Background(), 7)
	var remoteErr *RemoteFunctionError
	if !errors.As(err, &remoteErr) {
		t.Fatalf("error type = %T, %v", err, err)
	}
	if remoteErr.RemoteType != "*vmon.takeoverTestError" || remoteErr.Message != "test error 7" {
		t.Fatalf("remote error = %#v", remoteErr)
	}
	if _, err := function.Remote(context.Background(), 1); err == nil {
		t.Fatal("expected second failure")
	}
	if creates, sessions, _ := stub.counts(); creates != 1 || sessions != 1 {
		t.Fatalf("creates=%d sessions=%d, user errors must keep the session warm", creates, sessions)
	}
}

func TestTakeoverFunctionUserRetriesReuseSession(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[int64](client, "flaky", takeoverTestOptions(t, FunctionOptions{
		Retries: &Retries{MaxRetries: 2, BackoffCoefficient: 1, InitialDelay: time.Millisecond, MaxDelay: time.Second},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer function.Terminate(context.Background())
	// The worker-side counter persists in the session, so the third attempt succeeds.
	result, err := function.Remote(context.Background(), 3)
	if err != nil || result != 3 {
		t.Fatalf("flaky = %d, %v", result, err)
	}
	if creates, sessions, _ := stub.counts(); creates != 1 || sessions != 1 {
		t.Fatalf("creates=%d sessions=%d, user retries must reuse the warm session", creates, sessions)
	}
}

func TestTakeoverFunctionCallTimeoutKillsSessionKeepsSandbox(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[int](client, "sleepy", takeoverTestOptions(t, FunctionOptions{
		CallTimeout: 200 * time.Millisecond,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer function.Terminate(context.Background())
	_, err = function.Remote(context.Background(), 5000)
	var timeoutErr *CallTimeoutError
	if !errors.As(err, &timeoutErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %T %v", err, err)
	}
	// The next call restarts a session on the same warm sandbox.
	if result, err := function.Remote(context.Background(), 1); err != nil || result != 1 {
		t.Fatalf("post-timeout call = %d, %v", result, err)
	}
	if creates, sessions, _ := stub.counts(); creates != 1 || sessions != 2 {
		t.Fatalf("creates=%d sessions=%d, timeout must kill only the session", creates, sessions)
	}
}

func TestTakeoverFunctionInfraRetriesUseFreshSandboxes(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[int](client, "die", takeoverTestOptions(t, FunctionOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	defer function.Terminate(context.Background())
	_, err = function.Remote(context.Background(), 3)
	var remoteErr *RemoteFunctionError
	if !errors.As(err, &remoteErr) || remoteErr.RemoteType != "WorkerExit" ||
		!strings.Contains(remoteErr.Message, "exited with code 3") {
		t.Fatalf("error = %T %v", err, err)
	}
	creates, _, terminated := stub.counts()
	if creates != 1+takeoverMaxInfraRetries {
		t.Fatalf("creates = %d, want initial attempt + %d fresh-sandbox retries", creates, takeoverMaxInfraRetries)
	}
	// Every dead sandbox was condemned and terminated (async): allow a moment.
	deadline := time.Now().Add(5 * time.Second)
	for len(terminated) < creates && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		_, _, terminated = stub.counts()
	}
	if len(terminated) != creates {
		t.Fatalf("terminated %d of %d sandboxes", len(terminated), creates)
	}
}

func TestTakeoverFunctionSpawnAndGather(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[int](client, "add", takeoverTestOptions(t, FunctionOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	defer function.Terminate(context.Background())
	ctx := context.Background()
	first, err := function.Spawn(ctx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := function.Spawn(ctx, 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	results, err := Gather(ctx, first, second)
	if err != nil {
		t.Fatal(err)
	}
	if results[0] != 3 || results[1] != 7 {
		t.Fatalf("results = %v", results)
	}
	if !first.Done() || !second.Done() {
		t.Fatal("calls not done after gather")
	}
	// Both spawns share one sandbox but run on dedicated concurrent sessions.
	if creates, sessions, _ := stub.counts(); creates != 1 || sessions != 2 {
		t.Fatalf("creates=%d sessions=%d", creates, sessions)
	}
}

func TestTakeoverFunctionSpawnCancel(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[int](client, "sleepy", takeoverTestOptions(t, FunctionOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	defer function.Terminate(context.Background())
	call, err := function.Spawn(context.Background(), 30000)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	call.Cancel()
	_, err = call.Get(context.Background())
	if !errors.Is(err, ErrCallCancelled) {
		t.Fatalf("cancelled get = %v", err)
	}
}

func TestTakeoverFunctionMapOrderedAndTryMap(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	function, err := NewFunction[int](client, "add", takeoverTestOptions(t, FunctionOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	inputs := [][]any{{0, 0}, {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}
	results, err := function.Map(context.Background(), inputs, MapOptions{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range results {
		if result != index*11 {
			t.Fatalf("results = %v", results)
		}
	}
	creates, _, terminated := stub.counts()
	if creates != 2 {
		t.Fatalf("creates = %d, want one ephemeral sandbox per map worker", creates)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(terminated) < creates && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		_, _, terminated = stub.counts()
	}
	if len(terminated) != creates {
		t.Fatalf("map workers not terminated: %d of %d", len(terminated), creates)
	}

	// TryMap collects per-input errors instead of failing fast.
	failing, err := NewFunction[any](takeoverTestClient(t, startTakeoverStub(t)), "fail", takeoverTestOptions(t, FunctionOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	outcomes, err := failing.TryMap(context.Background(), [][]any{{1}, {2}}, MapOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	for index, outcome := range outcomes {
		var remoteErr *RemoteFunctionError
		if !errors.As(outcome.Err, &remoteErr) || remoteErr.Message != fmt.Sprintf("test error %d", index+1) {
			t.Fatalf("outcome %d = %+v", index, outcome)
		}
	}
	// Map fails fast on the same inputs.
	if _, err := failing.Map(context.Background(), [][]any{{1}, {2}}, MapOptions{Concurrency: 1}); err == nil {
		t.Fatal("expected fail-fast map error")
	}
}

func TestNewFunctionValidation(t *testing.T) {
	stub := startTakeoverStub(t)
	client := takeoverTestClient(t, stub)
	if _, err := NewFunction[int](client, "not-registered", takeoverTestOptions(t, FunctionOptions{})); err == nil ||
		!strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unregistered error = %v", err)
	}
	if _, err := NewFunction[int](nil, "add"); err == nil {
		t.Fatal("expected nil client error")
	}
	if _, err := NewFunction[int](client, "add", takeoverTestOptions(t, FunctionOptions{
		Retries: &Retries{MaxRetries: -1},
	})); err == nil {
		t.Fatal("expected retries validation error")
	}
	if runtime.GOOS != "linux" {
		_, err := NewFunction[int](client, "add")
		if err == nil || !strings.Contains(err.Error(), "WorkerBinary") {
			t.Fatalf("darwin default worker binary error = %v", err)
		}
	}
}

func TestRetriesDelaySchedule(t *testing.T) {
	fixed := RetryCount(3)
	normalized, err := fixed.normalized()
	if err != nil {
		t.Fatal(err)
	}
	for retry := 1; retry <= 3; retry++ {
		if delay := normalized.delayFor(retry); delay != time.Second {
			t.Fatalf("int-form delay %d = %s", retry, delay)
		}
	}
	backoff, err := (&Retries{MaxRetries: 5, InitialDelay: time.Second, MaxDelay: 5 * time.Second}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	expected := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for retry, want := range expected {
		if delay := backoff.delayFor(retry + 1); delay != want {
			t.Fatalf("delay %d = %s, want %s", retry+1, delay, want)
		}
	}
}
