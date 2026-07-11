import { expect, test } from "bun:test";
import { Code, ConnectError, createRouterTransport } from "@connectrpc/connect";
import { Client, MeshDriver, ProtocolError } from "../src";
import {
  PoolService,
  SandboxService,
  SnapshotService,
  SystemService,
  VolumeService,
} from "../src/gen/vmon/v1/api_pb";

interface RecordedRpc {
  method: string;
  input: object;
}

interface FakeState {
  view: unknown;
  rows: unknown[];
  snapshots: string[];
  volumes: string[];
  logs: string[];
  events: unknown[];
}

/** In-memory vmon gRPC surface with canned per-call responses. */
function fakeVmon() {
  const rpcs: RecordedRpc[] = [];
  const state: FakeState = {
    view: {},
    rows: [],
    snapshots: [],
    volumes: [],
    logs: [],
    events: [],
  };
  const view = (method: string, input: object) => {
    rpcs.push({ method, input });
    return { json: JSON.stringify(state.view) };
  };
  const record = (method: string, input: object) => {
    rpcs.push({ method, input });
    return {};
  };
  const router = createRouterTransport(({ service }) => {
    service(SandboxService, {
      create: (req) => view("Create", req),
      get: (req) => view("Get", req),
      list: (req) => {
        rpcs.push({ method: "List", input: req });
        return { sandboxesJson: state.rows.map((row) => JSON.stringify(row)) };
      },
      stop: (req) => view("Stop", req),
      remove: (req) => view("Remove", req),
      terminate: (req) => view("Terminate", req),
      pause: (req) => view("Pause", req),
      resume: (req) => view("Resume", req),
      extend: (req) => view("Extend", req),
      metrics: (req) => view("Metrics", req),
      execCapture: (req) => {
        rpcs.push({ method: "ExecCapture", input: req });
        return { code: 0n, stdout: new Uint8Array(), stderr: new Uint8Array() };
      },
      async *logs(req) {
        rpcs.push({ method: "Logs", input: req });
        for (const line of state.logs) yield { data: new TextEncoder().encode(line) };
      },
      fileRead: (req) => {
        rpcs.push({ method: "FileRead", input: req });
        return { data: new TextEncoder().encode("data") };
      },
      fileWrite: (req) => record("FileWrite", req),
      fileDelete: (req) => record("FileDelete", req),
      fileList: (req) => view("FileList", req),
      fileStat: (req) => view("FileStat", req),
      networkGet: (req) => view("NetworkGet", req),
      networkSet: (req) => view("NetworkSet", req),
      tunnels: (req) => view("Tunnels", req),
      migrate: (req) => view("Migrate", req),
      snapshot: (req) => view("Snapshot", req),
      snapshotFs: (req) => view("SnapshotFs", req),
    });
    service(SnapshotService, {
      list: () => ({ snapshots: state.snapshots }),
      restore: (req) => view("Restore", req),
      fork: (req) => view("Fork", req),
    });
    service(VolumeService, {
      list: () => ({ volumes: state.volumes }),
      create: (req) => record("VolumeCreate", req),
      delete: (req) => record("VolumeDelete", req),
    });
    service(PoolService, {
      list: () => view("PoolList", {}),
      set: (req) => view("PoolSet", req),
      delete: (req) => record("PoolDelete", req),
    });
    service(SystemService, {
      info: () => view("Info", {}),
      meshStatus: () => view("MeshStatus", {}),
      async *events() {
        for (const event of state.events) yield { json: JSON.stringify(event) };
      },
    });
  });
  return { rpcs, state, transport: () => router };
}

function inputField(call: RecordedRpc | undefined, name: string): unknown {
  return call === undefined ? undefined : Reflect.get(call.input, name);
}

test("resource hierarchy maps RPCs and views", async () => {
  const { rpcs, state, transport } = fakeVmon();
  const httpCalls: URL[] = [];
  const httpState = { status: 200 };
  const client = new Client(
    new MeshDriver("http://node-a", {
      discover: false,
      transport,
      fetch: (input, init) => {
        httpCalls.push(new URL(new Request(input, init).url));
        return Promise.resolve(Response.json({ ok: true }, { status: httpState.status }));
      },
    }),
  );

  state.view = { id: "s1", node: "n1" };
  const sandbox = await client.sandboxes.create({ image: "alpine" });
  expect(sandbox.id).toBe("s1");
  const created = rpcs.at(-1);
  expect(created?.method).toBe("Create");
  expect(JSON.parse(String(inputField(created, "specJson")))).toEqual({ image: "alpine" });

  await sandbox.run(["echo", "ok"]);
  expect(rpcs.at(-1)).toMatchObject({ method: "ExecCapture", input: { id: "s1" } });

  await sandbox.files.writeText("/tmp/a b", "hello");
  expect(rpcs.at(-1)).toMatchObject({ method: "FileWrite", input: { id: "s1", path: "/tmp/a b" } });
  state.view = { entries: [{ path: "/tmp/a b" }] };
  expect(await sandbox.files.list("/tmp")).toEqual([{ path: "/tmp/a b" }]);
  expect(rpcs.at(-1)).toMatchObject({ method: "FileList", input: { path: "/tmp" } });
  state.view = { path: "/tmp/a b", size: 5 };
  await sandbox.files.stat("/tmp/a b");
  expect(rpcs.at(-1)?.method).toBe("FileStat");
  await sandbox.files.delete("/tmp/tree", true);
  expect(rpcs.at(-1)).toMatchObject({
    method: "FileDelete",
    input: { path: "/tmp/tree", recursive: true },
  });

  state.view = { block_network: true };
  await sandbox.setNetwork({ block_network: true });
  expect(rpcs.at(-1)).toMatchObject({ method: "NetworkSet", input: { blockNetwork: true } });

  state.view = { connect_token: "tunnel-token", tunnels: {} };
  await sandbox.tunnels();
  httpState.status = 418;
  const proxied = await sandbox.ports.http(8080, "GET", "/hello", {
    params: { token: "evil", access_token: "evil2", connect_token: "evil3", q: "ok" },
  });
  expect(proxied.status).toBe(418);
  httpState.status = 200;
  const proxyUrl = httpCalls.at(-1);
  expect(proxyUrl?.pathname).toBe("/v1/sandboxes/s1/ports/8080/hello");
  expect(proxyUrl?.searchParams.toString()).toBe("q=ok&connect_token=tunnel-token");
  await expect(sandbox.ports.http(-1, "GET")).rejects.toBeInstanceOf(RangeError);
  await expect(sandbox.ports.websocket(65_536)).rejects.toBeInstanceOf(RangeError);

  state.snapshots = ["snap"];
  expect(await client.snapshots.list()).toEqual(["snap"]);
  state.view = { id: "restored" };
  await client.snapshots.restore("snap", { name: "copy" });
  const restored = rpcs.at(-1);
  expect(restored?.method).toBe("Restore");
  expect(inputField(restored, "name")).toBe("snap");
  expect(JSON.parse(String(inputField(restored, "bodyJson")))).toEqual({ name: "copy" });
  state.view = { clones: [{ id: "clone-1" }] };
  expect((await client.snapshots.fork("snap", { count: 1 })).map((clone) => clone.id)).toEqual([
    "clone-1",
  ]);
  state.view = { snapshot: "vm-snapshot" };
  expect(await sandbox.snapshot()).toBe("vm-snapshot");
  state.view = { image: "fs-image" };
  expect(await sandbox.snapshotFilesystem()).toBe("fs-image");
  state.view = { expires_at: 123 };
  expect((await sandbox.extend(30)).id).toBe("s1");
  expect(rpcs.at(-1)).toMatchObject({ method: "Extend", input: { id: "s1", secs: 30n } });
  state.view = { target: "node-b" };
  expect((await sandbox.migrate("node-b")).id).toBe("s1");
  expect(rpcs.at(-1)).toMatchObject({ method: "Get", input: { id: "s1" } });

  state.view = { size: 2 };
  await client.pools.set("image", 2, { image: "alpine" });
  const poolSet = rpcs.at(-1);
  expect(poolSet?.method).toBe("PoolSet");
  expect(inputField(poolSet, "reference")).toBe("image");
  expect(JSON.parse(String(inputField(poolSet, "bodyJson")))).toEqual({
    image: "alpine",
    size: 2,
  });
  state.view = { image: { size: 2 }, other: { size: 1 } };
  expect((await client.pools.list()).map((pool) => pool.ref)).toEqual(["image", "other"]);
  const beforeClear = rpcs.length;
  await client.pools.clear();
  expect(
    rpcs
      .slice(beforeClear)
      .filter((call) => call.method === "PoolDelete")
      .map((call) => inputField(call, "reference")),
  ).toEqual(["image", "other"]);

  state.view = { self: { node_id: "a" }, peers: [{ node_id: "b" }], replicas_held: 0 };
  expect((await client.mesh.nodes()).map((node) => node.node_id)).toEqual(["a", "b"]);
});

test("pool list rejects malformed server data", async () => {
  const { state, transport } = fakeVmon();
  const client = new Client(new MeshDriver("http://node-a", { discover: false, transport }));
  for (const response of [[], { bad: null }, { bad: { size: "NaN" } }]) {
    state.view = response;
    await expect(client.pools.list()).rejects.toBeInstanceOf(ProtocolError);
  }
});

test("typed response models reject malformed payloads", async () => {
  const { state, transport } = fakeVmon();
  let healthBody = "{}";
  const client = new Client(
    new MeshDriver("http://node-a", {
      discover: false,
      transport,
      fetch: () =>
        Promise.resolve(
          new Response(healthBody, {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        ),
    }),
  );

  for (const response of ["{}", "[]", "null", '{"ok":true,"bad":NaN}']) {
    healthBody = response;
    await expect(client.health()).rejects.toBeInstanceOf(ProtocolError);
  }

  state.view = {};
  await expect(client.info()).rejects.toBeInstanceOf(ProtocolError);
  await expect(client.sandboxes.create({ image: "alpine" })).rejects.toBeInstanceOf(ProtocolError);
  state.rows = [null];
  await expect(client.sandboxes.list()).rejects.toBeInstanceOf(ProtocolError);
  await expect(client.mesh.status()).rejects.toBeInstanceOf(ProtocolError);

  const sandbox = client.sandboxes.ref("box");
  state.view = [];
  await expect(sandbox.metrics()).rejects.toBeInstanceOf(ProtocolError);
  state.view = { block_network: "false" };
  await expect(sandbox.network()).rejects.toBeInstanceOf(ProtocolError);
  state.view = { tunnels: { "8080": { host: "127.0.0.1", port: "bad" } } };
  await expect(sandbox.tunnels()).rejects.toBeInstanceOf(ProtocolError);

  state.events = [[]];
  const events = await client.events();
  await expect(events[Symbol.asyncIterator]().next()).rejects.toBeInstanceOf(ProtocolError);
});

test("RPC failures map to APIError via the vmon-code trailer with gRPC-code fallback", async () => {
  const router = createRouterTransport(({ service }) => {
    service(SandboxService, {
      get(req) {
        if (req.id === "trailer")
          throw new ConnectError("locked", Code.Aborted, { "vmon-code": "busy" });
        throw new ConnectError("gone", Code.NotFound);
      },
    });
  });
  const client = new Client(
    new MeshDriver("http://node-a", { discover: false, transport: () => router }),
  );
  await expect(client.sandboxes.get("trailer")).rejects.toMatchObject({
    name: "APIError",
    code: "busy",
    status: 409,
    message: "locked",
  });
  await expect(client.sandboxes.get("other")).rejects.toMatchObject({
    name: "APIError",
    code: "not_found",
    status: 404,
  });
});

test("log and event streams decode server-streaming RPCs", async () => {
  const { state, transport } = fakeVmon();
  const client = new Client(new MeshDriver("http://node-a", { discover: false, transport }));
  state.logs = ["hello ", "world"];
  const sandbox = client.sandboxes.ref("s1");
  expect(await sandbox.logs()).toBe("hello world");
  const followed: string[] = [];
  for await (const chunk of await sandbox.followLogs()) followed.push(chunk);
  expect(followed).toEqual(["hello ", "world"]);

  state.events = [{ type: "created" }, { type: "removed" }];
  const seen: unknown[] = [];
  for await (const event of await client.events()) seen.push(event);
  expect(seen).toEqual([{ type: "created" }, { type: "removed" }]);
});
