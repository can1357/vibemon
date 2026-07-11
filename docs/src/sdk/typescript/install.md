# Install the TypeScript SDK

Use the SDK in an ESM Bun application. `@vmon/sdk` is a private package in
this repository, so it cannot be installed from a registry. From the
application directory, add a local checkout (adjust the relative path):

```sh
bun add @vmon/sdk@file:../vibevmm/sdk/ts
bun add --dev typescript
```

The local package's ESM root is imported by package name:

```ts
import { connect, type Client } from "@vmon/sdk";

const client: Client = connect("https://vmon.example");
await client.close();
```

Do not import implementation files such as `@vmon/sdk/src/client.ts`; the
package exposes `@vmon/sdk`, `@vmon/sdk/functions`, and
`@vmon/sdk/function-values`.

## Type-check an application

Use the TypeScript compiler without emitting JavaScript:

```sh
bunx tsc --noEmit
```

Your TypeScript configuration must resolve ESM packages. A minimal strict
configuration for a Bun application is:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "Preserve",
    "moduleResolution": "bundler",
    "strict": true,
    "noEmit": true
  }
}
```

If you are working in this repository's SDK package rather than consuming it,
its checked-in command is:

```sh
cd sdk/ts
bun run typecheck
```

## Runtime prerequisites

Installation provides a client library only. Run or deploy `vmon serve`
separately and give the Bun process or browser a network-reachable daemon URL.
The TypeScript client cannot start a daemon and cannot connect through a local
Unix socket. See [Connect](connect.md) for runtime-specific defaults and
[Server Operation](../../platform/server.md) for the daemon.
