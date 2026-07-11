// Package vmon provides a driver-backed client for the vmon gRPC API.
//
// Connect accepts local UDS, HTTP(S), multi-host mesh, and named-context DSNs.
// It discovers advertised peers lazily and fails over only on transport-level
// connection failures. NewClient binds the same object model to an injected Driver.
// Sandbox operations live on values returned by Client.Sandboxes.
//
// # Deployed functions
//
// LookupFunction resolves a server-registered function and pins its immutable
// revision. Remote creates a durable call and waits for its result; Spawn returns
// a FunctionCall whose stable ID can be reconstructed in another process with
// FunctionCallFromID. SpawnMap creates a durable batch and Results applies bounded
// consumer backpressure. Execution and retries are owned entirely by the server.
//
// Inputs and results use portable JSON or CBOR ValueEnvelope values. Calls provide
// at-least-once execution: an attempt may run more than once, while indexed results
// and events are committed durably and can be resumed from their sequence cursors.
//
//	add, _ := vmon.LookupFunction[int](ctx, client, "production", "add")
//	sum, _ := add.Remote(ctx, []int{2, 3})
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
