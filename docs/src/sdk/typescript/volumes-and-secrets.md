# Volumes and Secrets

Volumes and secrets describe data supplied to remote sandboxes managed by
`vmon serve`. A `Volume` is a daemon-side persistent storage name; a `Secret`
is a validated client-side value that becomes sandbox creation data or an exec
environment. Neither creates local storage or a local secret manager. See
[Storage and Volumes](../../platform/storage.md) for platform semantics.

## Persistent volumes

Use `client.volumes` to list, create, or delete named persistent volumes.
`create()` and `list()` return `Volume` value objects.

```ts
import { connect } from "@vmon/sdk";

const client = connect();
const volume = await client.volumes.create("build-cache");
console.log(volume.name);

const volumes = await client.volumes.list();
console.log(volumes.map((item) => item.name));
```

`new Volume(name)` rejects an empty name. `volume.mount(readOnly = false)`
returns the typed mount descriptor `{ name, read_only }` accepted inside a
sandbox creation request's `volumes` map. The map key is the guest mount path.

```ts
const sandbox = await client.sandboxes.create({
  image: "alpine",
  volumes: {
    "/var/cache/build": volume.mount(),
    "/mnt/reference": volume.mount(true),
  },
});
```

A volume mount descriptor can also be written inline as
`{ name: "build-cache", read_only: true }`, and the model also permits a
string volume name. The SDK preserves the requested descriptor; mount timing,
sharing, persistence, and daemon-side validation are server behavior. Delete a
volume explicitly when it is no longer needed:

```ts
await client.volumes.delete("build-cache");
```

Do not delete a volume that other sandboxes still require. The SDK does not
track usage or garbage-collect volumes.

## Secrets

`Secret` validates and holds environment-variable values. Build one with the
constructor or `Secret.fromDict(values, name?)`; `Secret.fromEnv(names, name?)`
selects variables from the Node/Bun process environment when `process` is
available. It does not read a browser environment.

```ts
import { Secret } from "@vmon/sdk";

const registryCredentials = Secret.fromDict(
  { REGISTRY_USER: "build-bot", REGISTRY_PASSWORD: "correct-horse" },
  "registry",
);

console.log(registryCredentials.name);
console.log(registryCredentials.names());
```

Names must be non-empty and contain neither `=` nor a NUL byte. Secret values
must not contain a NUL byte. `names()` returns sorted variable names, while
`asEnv()` returns a copy of the values. Treat both the source dictionary and
returned copies as sensitive data.

`SecretInput` accepts all of the following shapes:

```ts
import { Secret, type SecretWire } from "@vmon/sdk";

const valueObject = Secret.fromDict({ API_TOKEN: "token" }, "api");
const wire: SecretWire = { name: "api", values: { API_TOKEN: "token" } };
const dictionary: Record<string, string> = { API_TOKEN: "token" };

const secrets = [valueObject, wire, dictionary];
```

The `Secret` instance is usually the clearest form. A bare dictionary is
normalized as a secret named `"secret"`. A `SecretWire` has exactly the wire
shape `{ name, values }`. For creation, the SDK normalizes every input by
constructing and validating a `Secret`, then serializes it to `{ name, values
}`. It sends `null` unchanged when a create request sets `secrets: null`, and
omits the field when `secrets` is `undefined`.

```ts
const sandbox = await client.sandboxes.create({
  image: "alpine",
  secrets: [registryCredentials],
});
```

For `sandbox.run()` and `sandbox.exec()`, `RunOptions.secrets` is different:
the SDK merges every secret's values into the remote command's `env`. Later
inputs overwrite earlier keys, and the merged secret values overwrite any
same-named keys supplied in `options.env`.

```ts
const result = await sandbox.run(["sh", "-lc", "test -n \"$API_TOKEN\""], {
  env: { LOG_LEVEL: "info" },
  secrets: [Secret.fromDict({ API_TOKEN: "token" }, "api")],
});

if (result.exit !== 0) {
  throw new Error("token was not available to the command");
}
```

Secret inputs are not encrypted, redacted, or retained by a special local
secret store in this SDK. Avoid logging them, process environments, serialized
creation requests, command output, or thrown error context that can reveal
them. Secure the client-to-daemon connection and deployment access controls as
described in [Shared Concepts](../shared-concepts.md#security-boundary) and
[Connection Strings and Contexts](../connection-strings.md).
