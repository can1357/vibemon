// Package vmon provides a driver-backed client for the vmon gRPC API.
//
// Connect accepts local UDS, HTTP(S), multi-host mesh, and named-context DSNs.
// It discovers advertised peers lazily and fails over only on transport-level
// connection failures. NewClient binds the same object model to an injected Driver.
// Sandbox operations live on values returned by Client.Sandboxes.
//
// # Remote functions
//
// Two remote-function paths run code inside sandboxes. NewRemoteFunction executes a
// self-contained JavaScript module export on a Node.js image. Register plus Takeover
// re-execute this very Go binary inside the guest ("takeover"): register functions in
// main, call vmon.Takeover() right after, and invoke them through NewFunction handles
// with Remote, Spawn, and Map over a persistent in-guest worker session. Worker
// sandboxes default to a plain glibc image (DefaultTakeoverImage); a CGO_ENABLED=0
// static build runs on any Linux image. Non-linux hosts must point
// FunctionOptions.WorkerBinary at a GOOS=linux build of the same program, and
// FunctionOptions.Pool keeps a server-side warm pool for fast sandbox acquisition.
//
//	func main() {
//		vmon.Register("add", func(a, b int) int { return a + b })
//		vmon.Takeover() // no-op unless re-executed as a worker
//
//		client, _ := vmon.Connect("vmon+context://prod")
//		add, _ := vmon.NewFunction[int](client, "add")
//		sum, _ := add.Remote(ctx, 2, 3)
//		_ = sum
//	}
//
// # Example
//
//	client, err := vmon.Connect("vmon+context://prod")
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{Image: "alpine"})
//	if err != nil {
//		return err
//	}
//	defer sandbox.Terminate(ctx)
package vmon
