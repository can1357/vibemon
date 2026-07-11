# Connection Strings and Contexts

A DSN selects the daemon endpoint roster and connection policy. Parsing a DSN
does not contact the daemon; discovery begins only after a successful request.
Use a DSN explicitly when deployment configuration should not depend on a
developer's local environment.

## DSN forms

| Form | Endpoints and transport | SDK support |
| --- | --- | --- |
| `vmon://host-a,host-b:9000/prefix` | One or more plaintext HTTP/2 gRPC endpoints; omitted ports default to `8000` | Python, Go, TypeScript |
| `vmons://host-a,host-b/prefix` | One or more TLS gRPC endpoints | Python, Go, TypeScript |
| `http://host:8000/prefix` | One explicit plaintext endpoint | Python, Go, TypeScript |
| `https://host/prefix` | One explicit TLS endpoint | Python, Go, TypeScript |
| `vmon+unix:///absolute/path/vmond.sock` | Native gRPC and infrastructure HTTP over a local Unix-domain socket | Python and Go only |
| `vmon+context://name` | Endpoint roster and, optionally, a local credential from a named context | Python, Go, TypeScript in a filesystem-capable runtime |

`vmon` and `vmons` accept a comma-separated host list. Bracket IPv6 literals
may be used in that list. `http` and `https` deliberately accept exactly one
endpoint. All forms reject userinfo and fragments. A path prefix is retained
for network endpoints; do not use it to imply a different API protocol.

`vmon` is plaintext and `vmons` is TLS. Likewise, `http` and `https` select
the corresponding network security mode. The TypeScript transport translates
the network endpoint to the `/grpc` WebSocket bridge (`ws` or `wss`) for RPCs;
it does not gain native HTTP/2 or Unix-socket support. See
[SDK Overview](overview.md) for the daemon boundary and the limited HTTP
surfaces.

## Query parameters

Only these query parameters are accepted. They apply to every DSN form.

| Parameter | Meaning | Default |
| --- | --- | --- |
| `token` | Bearer token supplied with requests | No DSN token |
| `discover=on\|off` | Enable or disable mesh endpoint discovery | `on` |
| `timeout=seconds` | Positive per-operation connection/request timeout | `60` seconds |

Each parameter may occur once. Unknown parameters, duplicate parameters,
non-positive or non-finite timeouts, and a discovery value other than `on` or
`off` are invalid. Put credentials in the context credential store or the
environment where possible instead of widely shared DSNs.

## Defaults and token resolution

When the connection factory receives no DSN, resolution is language-aware:

| Client | Empty-DSN resolution |
| --- | --- |
| Python and Go | `VMON_DSN`, then `VMON_CONTEXT` as a named context, then `vmon+unix://$VMON_HOME/vmond.sock` |
| TypeScript in a browser | `VMON_DSN`, then `VMON_CONTEXT`, then the page's HTTP(S) origin |
| TypeScript outside a browser | `VMON_DSN`, then `VMON_CONTEXT`, then `http://127.0.0.1:8000` |

`VMON_HOME` selects the SDK state directory; without it, native clients use
`~/.vmon`. TypeScript's filesystem-backed context support likewise uses
`VMON_HOME` or the user's home directory. A browser has no local filesystem,
so a `vmon+context` DSN cannot resolve there even though browser TypeScript
can use ordinary network DSNs.

For an explicit DSN, a `token` query parameter wins. Otherwise clients use
`VMON_API_TOKEN`; a named context can provide a stored token when no higher
priority token is present. TypeScript connection options may explicitly
override the parsed token, timeout, and discovery setting. Consult the
language connection page for constructor-specific option names.

## Named contexts

A context is a local deployment profile, not a server-side object. The usual
store is `$VMON_HOME/contexts.json`, with a context record containing its
name, one or more HTTP(S) endpoints, and optional metadata such as region and
update time. Its optional credential is stored separately at
`$VMON_HOME/credentials/<name>.token`.

```json
{
  "current": "prod",
  "contexts": {
    "prod": {
      "name": "prod",
      "endpoints": ["https://vmon-a.example", "https://vmon-b.example"],
      "region": "us-east"
    }
  }
}
```

`vmon+context://prod` loads `prod`; it does not use a server-selected
"current" context. A missing context, an empty endpoint list, or a context
endpoint that is not HTTP(S) is an error. Contexts are particularly useful for
switching between local, staging, and production rosters without embedding a
token in source code.

## Choosing a form

| Need | Use |
| --- | --- |
| A local native client talking to the daemon's socket | `vmon+unix:///absolute/path/vmond.sock` in Python or Go |
| A browser application | An `http`, `https`, `vmon`, or `vmons` network DSN; normally the page origin by default |
| A remote daemon with one fixed address | `https://host` or `http://host:port` |
| TLS mesh seeds or explicit failover seeds | `vmons://host-a,host-b` |
| A reusable local deployment profile | `vmon+context://name` |

For what happens after an endpoint is selected—lazy discovery, endpoint
affinity, and restricted failover—see [Shared Concepts](shared-concepts.md).
