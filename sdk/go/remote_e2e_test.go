package vmon

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRemoteFunctionE2E(t *testing.T) {
	if os.Getenv("VMON_GO_REMOTE_SMOKE") != "1" {
		t.Skip("set VMON_GO_REMOTE_SMOKE=1, VMON_SERVER_URL, and VMON_API_TOKEN")
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

	image := os.Getenv("VMON_GO_REMOTE_IMAGE")
	if image == "" {
		image = DefaultRemoteFunctionImage
	}
	var stdout bytes.Buffer
	function, err := NewRemoteFunction[int](
		client,
		RemoteFunctionSourceSpec{
			Source: `
export function double(value) {
  console.log("doubling " + value);
  if (value < 0) {
    throw new RangeError("value must be non-negative");
  }
  return value * 2;
}
`,
			ExportName: "double",
		},
		RemoteFunctionOptions{
			Sandbox: SandboxCreateRequest{Image: image, BlockNetwork: true},
			Stdout:  &stdout,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := function.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate remote function: %v", err)
		}
	}()

	result, err := function.Remote(ctx, 21)
	if err != nil || result != 42 {
		t.Fatalf("remote double = %d, %v", result, err)
	}
	_, err = function.Remote(ctx, -1)
	var remoteError *RemoteFunctionError
	if !errors.As(err, &remoteError) {
		t.Fatalf("remote failure type = %T, %v", err, err)
	}
	if remoteError.RemoteType != "RangeError" || !strings.Contains(remoteError.Message, "non-negative") {
		t.Fatalf("remote failure = %#v", remoteError)
	}
	if !strings.Contains(remoteError.RemoteStack, "double") {
		t.Fatalf("remote stack did not name handler: %q", remoteError.RemoteStack)
	}
	if stdout.String() != "doubling 21\ndoubling -1\n" {
		t.Fatalf("forwarded stdout = %q", stdout.String())
	}
}
