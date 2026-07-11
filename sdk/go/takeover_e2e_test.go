package vmon

import (
	"bytes"
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestTakeoverFunctionE2E exercises native-Go remote functions against a real
// vmon server. The worker binary is this test binary itself: on a linux host it
// uploads automatically; on other hosts set VMON_GO_TAKEOVER_BINARY to a
// GOOS=linux CGO_ENABLED=0 build of this package's test binary
// (go test -c -o ...) or the test skips.
func TestTakeoverFunctionE2E(t *testing.T) {
	if os.Getenv("VMON_GO_REMOTE_SMOKE") != "1" {
		t.Skip("set VMON_GO_REMOTE_SMOKE=1, VMON_SERVER_URL, and VMON_API_TOKEN")
	}
	workerBinary := os.Getenv("VMON_GO_TAKEOVER_BINARY")
	if workerBinary == "" && runtime.GOOS != "linux" {
		t.Skipf("takeover e2e on %s needs VMON_GO_TAKEOVER_BINARY pointing at a GOOS=linux build of this test binary", runtime.GOOS)
	}
	serverURL := os.Getenv("VMON_SERVER_URL")
	token := os.Getenv("VMON_API_TOKEN")
	if serverURL == "" || token == "" {
		t.Fatal("VMON_SERVER_URL and VMON_API_TOKEN are required")
	}
	client, err := Connect(serverURL, WithToken(token))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if _, err := client.Health(ctx); err != nil {
		t.Fatalf("vmon server health: %v", err)
	}

	image := os.Getenv("VMON_GO_TAKEOVER_IMAGE")
	if image == "" {
		image = DefaultTakeoverImage
	}
	var stdout bytes.Buffer
	options := FunctionOptions{
		Sandbox:      SandboxCreateRequest{Image: image, BlockNetwork: true},
		WorkerBinary: workerBinary,
		Stdout:       &stdout,
		Stderr:       &stdout,
	}
	function, err := NewFunction[int](client, "add", options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := function.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate takeover function: %v", err)
		}
	}()

	coldStart := time.Now()
	if result, err := function.Remote(ctx, 20, 22); err != nil || result != 42 {
		t.Fatalf("remote add = %d, %v", result, err)
	}
	coldDuration := time.Since(coldStart)
	warmStart := time.Now()
	if result, err := function.Remote(ctx, 1, 2); err != nil || result != 3 {
		t.Fatalf("warm remote add = %d, %v", result, err)
	}
	warmDuration := time.Since(warmStart)
	t.Logf("cold call %s, warm call %s", coldDuration, warmDuration)
	if warmDuration >= coldDuration {
		t.Errorf("warm call (%s) was not faster than cold call (%s)", warmDuration, coldDuration)
	}

	// Live output through out events.
	noisy, err := NewFunction[int](client, "noisy", options)
	if err != nil {
		t.Fatal(err)
	}
	defer noisy.Terminate(context.Background())
	if result, err := noisy.Remote(ctx); err != nil || result != 7 {
		t.Fatalf("remote noisy = %d, %v", result, err)
	}
	if !strings.Contains(stdout.String(), "hello from worker") {
		t.Fatalf("forwarded output = %q", stdout.String())
	}

	// Error fidelity: concrete Go error type name and message.
	failing, err := NewFunction[any](client, "fail", options)
	if err != nil {
		t.Fatal(err)
	}
	defer failing.Terminate(context.Background())
	_, err = failing.Remote(ctx, 5)
	var remoteErr *RemoteFunctionError
	if !errors.As(err, &remoteErr) || remoteErr.RemoteType != "*vmon.takeoverTestError" ||
		remoteErr.Message != "test error 5" {
		t.Fatalf("remote failure = %T %v", err, err)
	}

	// Spawn two concurrent sessions on the warm sandbox and gather.
	first, err := function.Spawn(ctx, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := function.Spawn(ctx, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	results, err := Gather(ctx, first, second)
	if err != nil || results[0] != 2 || results[1] != 4 {
		t.Fatalf("gather = %v, %v", results, err)
	}
}
