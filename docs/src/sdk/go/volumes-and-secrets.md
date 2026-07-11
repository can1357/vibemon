# Volumes and Secrets

Persistent volumes and secrets are create-request values in the Go SDK. They are sent to the running `vmon serve` API; neither feature creates local storage or a local secret service. Use `client.Volumes` to manage named persistent volumes independently of sandbox lifecycle, and include mounts or secret bundles in `vmon.SandboxCreateRequest` when creating a sandbox.

For client setup, context cancellation, and shared error behavior, see [Connect](connect.md), [Connection Strings and Contexts](../connection-strings.md), and [Error Codes](../../reference/errors.md).

## Persistent volumes

A `vmon.Volume` is a validated, server-owned persistent volume name. Construct it with `vmon.NewVolume`, or have the server provision it with `client.Volumes.Create`. Valid names match `^[a-z0-9_][a-z0-9_.-]{0,63}$`.

```go
volume, err := client.Volumes.Create(ctx, "build-cache")
if err != nil {
    return err
}
fmt.Println(volume.Name())
```

`VolumeService` has three operations:

| Method | Effect |
| --- | --- |
| `client.Volumes.List(ctx)` | Returns all registered `[]vmon.Volume`. |
| `client.Volumes.Create(ctx, name)` | Validates `name`, provisions it on the server, and returns its `vmon.Volume` value. |
| `client.Volumes.Delete(ctx, name)` | Validates and deletes the named server volume. |

A persistent volume lifecycle is separate from sandbox removal. Removing or terminating a sandbox is not a substitute for `client.Volumes.Delete`; delete a named volume explicitly when its independent data lifecycle has ended.

```go
volumes, err := client.Volumes.List(ctx)
if err != nil {
    return err
}
for _, volume := range volumes {
    fmt.Println(volume.Name())
}

if err := client.Volumes.Delete(ctx, "build-cache"); err != nil {
    return err
}
```

### Mount a volume at create time

Call `volume.Mount(readOnly)` to build the `vmon.VolumeMount` value, then map each guest mount path to that mount in `SandboxCreateRequest.Volumes`. `Mount(false)` is writable; `Mount(true)` is read-only. The SDK serializes a writable mount as the volume name and a read-only mount with an explicit `read_only` value.

```go
cache, err := vmon.NewVolume("build-cache")
if err != nil {
    return err
}

sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image: "alpine:3.21",
    Volumes: map[string]vmon.VolumeMount{
        "/var/cache/build": cache.Mount(false),
        "/mnt/cache-ro":    cache.Mount(true),
    },
})
if err != nil {
    return err
}
fmt.Println(sandbox.ID)
```

`VolumeMount.Volume()` returns the backing `vmon.Volume`, and `ReadOnly()` reports the mount flag. Do not handcraft the JSON shape: `NewVolume` and `Mount` preserve the validation and request representation used by the client.

## Secret bundles

`vmon.Secret` is an in-memory named bundle of environment values. It is request-time data: place it in `SandboxCreateRequest.Secrets` during creation. The Go SDK does not expose a secret resource service, retrieve secret values from a sandbox, or treat a secret as persisted client-side configuration.

Create a bundle with `NewSecret(name, values)`. The constructor validates the bundle name and every environment key (they cannot be empty or contain `=` or NUL), rejects NUL in a value, and copies the input map. That copy means later mutation of the map passed to `NewSecret` does not mutate the bundle.

```go
credentials, err := vmon.NewSecret("registry", map[string]string{
    "REGISTRY_TOKEN": "token-from-a-secure-source",
    "REGISTRY_USER":  "ci-bot",
})
if err != nil {
    return err
}

sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image:   "alpine:3.21",
    Secrets: []vmon.Secret{credentials},
})
if err != nil {
    return err
}
fmt.Println(sandbox.ID)
```

`SecretFromEnvironment(name, variables...)` captures only the named variables that exist in the current process environment, then constructs the same validated bundle. It does not require every requested variable to be set.

```go
credentials, err := vmon.SecretFromEnvironment(
    "registry",
    "REGISTRY_TOKEN",
    "REGISTRY_USER",
)
if err != nil {
    return err
}

_, err = client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image:   "alpine:3.21",
    Secrets: []vmon.Secret{credentials},
})
return err
```

The only value-inspection API is intentionally limited: `Name()` returns the bundle name and `Names()` returns sorted variable names. `String()` and Go-syntax formatting redact values, reporting only the name and variable names. Avoid logging the source map or manually serializing a `Secret`; use the value directly in the create request.

## Non-secret environment and request scope

Use `SandboxCreateRequest.Env` for ordinary environment variables and `Secrets` for values that must not be represented as ordinary configuration. Both are create-request fields; `Sandbox` metadata does not provide a secret-value readback API. Per-process `ExecRequest.Env` is a separate non-secret environment map for `Run` or `Exec`.

```go
secret, err := vmon.NewSecret("application", map[string]string{
    "DATABASE_PASSWORD": "token-from-a-secure-source",
})
if err != nil {
    return err
}

_, err = client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image: "alpine:3.21",
    Env: map[string]string{
        "LOG_LEVEL": "info",
    },
    Secrets: []vmon.Secret{secret},
})
return err
```

The server receives the secret bundle only in the create request representation. Keep source values in your application's secret-management flow, pass a bounded request context to `Create`, and handle its error before retaining the sandbox handle.