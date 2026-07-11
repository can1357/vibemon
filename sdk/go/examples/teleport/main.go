// Command teleport demonstrates live sandbox migration between two vmon
// nodes: it seeds guest state (a tmpfs marker, a disk marker, and a live
// counter process), teleports the sandbox with one API call, prints the
// phase latencies reported by the server, and then shells into the sandbox
// on the target node to prove every piece of state survived.
//
// Usage:
//
//	VMON_API_TOKEN=... go run ./examples/teleport \
//	    -source http://127.0.0.1:8081 -target http://127.0.0.1:8082
//
// Both nodes must already be joined into one mesh.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	vmon "github.com/can1357/vibemon/sdk/go"
)

func main() {
	sourceURL := flag.String("source", "http://127.0.0.1:8081", "API URL of the node that owns the sandbox")
	targetURL := flag.String("target", "http://127.0.0.1:8082", "API URL of the node to teleport to")
	image := flag.String("image", "alpine:latest", "guest image")
	flag.Parse()

	token := os.Getenv("VMON_API_TOKEN")
	if token == "" {
		log.Fatal("VMON_API_TOKEN is required")
	}

	source, err := vmon.NewClient(*sourceURL, vmon.WithToken(token))
	if err != nil {
		log.Fatalf("source client: %v", err)
	}
	target, err := vmon.NewClient(*targetURL, vmon.WithToken(token))
	if err != nil {
		log.Fatalf("target client: %v", err)
	}
	ctx := context.Background()

	mesh, err := target.MeshStatus(ctx)
	if err != nil {
		log.Fatalf("target mesh status: %v", err)
	}
	if !mesh.Enabled {
		log.Fatalf("node at %s is not part of a mesh", *targetURL)
	}
	fmt.Printf("target node: %s (%s)\n", mesh.NodeID, mesh.Advertise)

	name := fmt.Sprintf("teleport-%d", time.Now().UnixMilli())
	timeout := uint64(900)
	sandbox, err := source.CreateSandbox(ctx, vmon.SandboxCreateRequest{
		Image:          *image,
		Name:           name,
		Command:        []string{"sleep", "600"},
		BlockNetwork:   true,
		TimeoutSeconds: &timeout,
		HA:             "off",
	})
	if err != nil {
		log.Fatalf("create sandbox: %v", err)
	}
	fmt.Printf("created sandbox %s on source\n", sandbox.Identifier())
	defer func() {
		_, _ = target.RemoveSandbox(ctx, name)
		_, _ = source.RemoveSandbox(ctx, name)
	}()

	// Seed state that only survives if the teleport really carries it:
	//   - a marker on a tmpfs mount (guest RAM),
	//   - a marker on the writable rootfs (disk block delta),
	//   - a counter loop whose value lives in a shell process's memory.
	nonce := fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	seed := fmt.Sprintf(
		"mkdir -p /dev/shm && mount -t tmpfs tmpfs /dev/shm"+
			" && printf %%s '%s' > /dev/shm/teleport"+
			" && printf %%s '%s' > /root/teleport && sync"+
			" && (i=0; while :; do i=$((i+1)); printf %%s $i > /tmp/count; sleep 0.05; done) &",
		nonce, nonce,
	)
	sh(ctx, source, name, "seed guest state", seed)
	before := sh(ctx, source, name, "counter before teleport", "sleep 1; cat /tmp/count")
	fmt.Printf("counter before teleport: %s\n", before)

	fmt.Println("\nteleporting (single API call)...")
	result, err := source.MigrateSandbox(ctx, name, mesh.NodeID)
	if err != nil {
		log.Fatalf("teleport: %v", err)
	}
	timing := result.Migration
	fmt.Printf(
		"  pre-copy   %6d ms  (guest running: bulk RAM+disk streamed to target)\n"+
			"  downtime   %6d ms  (guest suspended: final RAM/disk delta + resume)\n"+
			"  total      %6d ms\n",
		timing.PrecopyMS, timing.DowntimeMS, timing.TotalMS,
	)
	fmt.Printf("sandbox now %s on the target\n", result.Sandbox.Status)

	fmt.Println("\nverifying state through the target node:")
	ram := sh(ctx, target, name, "read tmpfs marker", "cat /dev/shm/teleport")
	check("tmpfs (RAM) marker", ram, nonce)
	disk := sh(ctx, target, name, "read disk marker", "cat /root/teleport")
	check("rootfs (disk) marker", disk, nonce)

	countA := mustCount(ctx, target, name, "read counter")
	countB := mustCount(ctx, target, name, "re-read counter")
	fmt.Printf("  counter process alive: %d -> %d\n", countA, countB)
	if countB <= countA {
		log.Fatal("counter process did not survive the teleport")
	}

	listing, err := source.ListSandboxes(ctx, vmon.SandboxListOptions{})
	if err != nil {
		log.Fatalf("listing source sandboxes: %v", err)
	}
	for _, row := range listing {
		if row.Identifier() == name {
			log.Fatalf("source node still lists %s after teleport", name)
		}
	}
	fmt.Println("  source node no longer owns the sandbox")
	fmt.Println("\nteleport complete: process, RAM, and disk state all intact")
}

// sh runs one /bin/sh command inside the sandbox and returns trimmed
// stdout, failing the demo on any transport or nonzero-exit error.
func sh(ctx context.Context, client *vmon.Client, id, what, script string) string {
	result, err := client.ExecCapture(ctx, id, vmon.ExecRequest{
		Command: []string{"/bin/sh", "-c", script},
	})
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
	if result.ExitCode != 0 {
		log.Fatalf("%s: exit %d stderr=%q", what, result.ExitCode, result.Stderr)
	}
	return strings.TrimSpace(string(result.Stdout))
}

// mustCount reads the in-guest counter file as an integer, waiting a beat so
// consecutive reads observe progress.
func mustCount(ctx context.Context, client *vmon.Client, id, what string) int {
	value, err := strconv.Atoi(sh(ctx, client, id, what, "sleep 1; cat /tmp/count"))
	if err != nil {
		log.Fatalf("%s: non-numeric counter: %v", what, err)
	}
	return value
}

func check(what, got, want string) {
	if got != want {
		log.Fatalf("%s mismatch: got %q want %q", what, got, want)
	}
	fmt.Printf("  %-22s intact (%s)\n", what, got)
}
