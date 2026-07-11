// Command teleport demonstrates live sandbox migration between two vmon mesh nodes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	vmon "github.com/can1357/vibemon/sdk/go"
)

func main() {
	sourceDSN := flag.String("source", "http://127.0.0.1:8081", "DSN of the source node")
	targetDSN := flag.String("target", "http://127.0.0.1:8082", "DSN of the target node")
	image := flag.String("image", "alpine:latest", "guest image")
	flag.Parse()
	token := os.Getenv("VMON_API_TOKEN")
	if token == "" {
		log.Fatal("VMON_API_TOKEN is required")
	}
	source, err := vmon.Connect(*sourceDSN, vmon.WithToken(token), vmon.WithDiscovery(true))
	if err != nil {
		log.Fatalf("source client: %v", err)
	}
	defer source.Close()
	target, err := vmon.Connect(*targetDSN, vmon.WithToken(token), vmon.WithDiscovery(true))
	if err != nil {
		log.Fatalf("target client: %v", err)
	}
	defer target.Close()
	ctx := context.Background()
	mesh, err := target.Mesh.Status(ctx)
	if err != nil {
		log.Fatalf("target mesh status: %v", err)
	}
	if mesh.Self.NodeID == "" {
		log.Fatal("target did not report a mesh node id")
	}
	fmt.Printf("target node: %s (%s)\n", mesh.Self.NodeID, mesh.Self.Advertise)
	timeout := uint64(900)
	sandbox, err := source.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{Image: *image, Name: fmt.Sprintf("teleport-%d", time.Now().UnixMilli()), Command: []string{"sleep", "600"}, TimeoutSeconds: &timeout})
	if err != nil {
		log.Fatalf("create sandbox: %v", err)
	}
	defer sandbox.Remove(ctx)
	nonce := fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	sh(ctx, sandbox, "seed guest state", fmt.Sprintf("printf %%s '%s' >/tmp/teleport", nonce))
	if _, err = sandbox.Migrate(ctx, mesh.Self.NodeID); err != nil {
		log.Fatalf("teleport: %v", err)
	}
	got := sh(ctx, sandbox, "verify migrated state", "cat /tmp/teleport")
	if got != nonce {
		log.Fatalf("state mismatch: got %q want %q", got, nonce)
	}
	fmt.Printf("teleport complete: sandbox %s is %s and state is intact\n", sandbox.ID, sandbox.Status)
}

func sh(ctx context.Context, sandbox *vmon.Sandbox, what, script string) string {
	result, err := sandbox.Run(ctx, vmon.ExecRequest{Command: []string{"/bin/sh", "-c", script}})
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
	if result.ExitCode != 0 {
		log.Fatalf("%s: exit %d stderr=%q", what, result.ExitCode, result.Stderr)
	}
	return strings.TrimSpace(string(result.Stdout))
}
