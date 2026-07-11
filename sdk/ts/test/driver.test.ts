import { afterEach, expect, test } from "bun:test";
import { Code, ConnectError, createRouterTransport, type Transport } from "@connectrpc/connect";
import { APIError, Client, MeshDriver, Sandbox, TransportError } from "../src";
import { SandboxService, SystemService } from "../src/gen/vmon/v1/api_pb";

const A = "http://node-a";
const B = "http://node-b";
const A_8000 = "http://node-a:8000";
const B_8000 = "http://node-b:8000";

const servers: Bun.Server<unknown>[] = [];
afterEach(() => {
  for (const server of servers.splice(0)) server.stop(true);
});
function serve(fetch: (request: Request) => Response | Promise<Response>): Bun.Server<unknown> {
  const server = Bun.serve({ port: 0, fetch });
  servers.push(server);
  return server;
}
function json(value: unknown, status = 200): Response {
  return Response.json(value, { status });
}

function meshStatusRouter(doc: () => unknown): Transport {
  return createRouterTransport(({ service }) => {
    service(SystemService, { meshStatus: () => ({ json: JSON.stringify(doc()) }) });
  });
}

test("discovers a peer, fails over, and sticks to the successful endpoint", async () => {
  const state = { aDown: false };
  let bCalls = 0;
  const routers: Record<string, Transport> = {
    [A]: meshStatusRouter(() => ({
      self: { node_id: "a", advertise: A },
      peers: [{ node_id: "b", advertise: B }],
    })),
    [B]: meshStatusRouter(() => ({ self: { node_id: "b", advertise: B }, peers: [] })),
  };
  const driver = new MeshDriver(A, {
    transport: (baseUrl) => routers[baseUrl],
    fetch: (input, init) => {
      const url = new URL(new Request(input, init).url);
      if (url.host === "node-a") {
        if (state.aDown) return Promise.reject(new TypeError("connection refused"));
        return Promise.resolve(json({ node: "a" }));
      }
      bCalls += 1;
      return Promise.resolve(json({ node: "b" }));
    },
  });
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ node: "a" });
  expect(driver.endpoints().map((entry) => entry.url)).toEqual([A, B]);
  state.aDown = true;
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ node: "b" });
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ node: "b" });
  expect(bCalls).toBeGreaterThanOrEqual(2);
});

test("successful retry clears seed cooldown before discovery expands the roster", async () => {
  let seedCalls = 0;
  const router = meshStatusRouter(() => ({
    self: { node_id: "seed", advertise: A },
    peers: [{ node_id: "peer", advertise: B }],
  }));
  const driver = new MeshDriver(A, {
    now: () => 0,
    transport: () => router,
    fetch: (input, init) => {
      const url = new URL(new Request(input, init).url);
      if (url.host === "node-a" && seedCalls++ > 0) return Promise.resolve(json({ ok: true }));
      return Promise.reject(new TypeError("connection refused"));
    },
  });

  await expect(driver.request("GET", "/healthz")).rejects.toBeInstanceOf(TransportError);
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ ok: true });
  expect(driver.endpoints()).toContainEqual({ url: A, healthy: true, source: "seed" });
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ ok: true });
});

test("refresh drops discovered peers absent from the latest mesh status", async () => {
  let includePeer = true;
  const router = meshStatusRouter(() => ({
    self: { node_id: "seed", advertise: A },
    peers: includePeer ? [{ node_id: "peer", advertise: B }] : [],
  }));
  const driver = new MeshDriver(A, {
    transport: () => router,
    fetch: () => Promise.resolve(json({ ok: true })),
  });
  await driver.request("GET", "/healthz");
  expect(driver.endpoints().map((entry) => entry.url)).toEqual([A, B]);
  includePeer = false;
  await driver.refresh(true);
  expect(driver.endpoints().map((entry) => entry.url)).toEqual([A]);
});

test("does not retry HTTP errors and reports all transport failures", async () => {
  let secondCalls = 0;
  const first = serve(() => json({ code: "busy", message: "busy" }, 503));
  const second = serve(() => {
    secondCalls += 1;
    return json({ ok: true });
  });
  const driver = new MeshDriver(
    `vmon://127.0.0.1:${first.port},127.0.0.1:${second.port}?discover=off`,
  );
  expect((await driver.request("GET", "/healthz")).status).toBe(503);
  expect(secondCalls).toBe(0);
  first.stop(true);
  second.stop(true);
  await expect(driver.request("GET", "/healthz")).rejects.toBeInstanceOf(TransportError);
});

test("fan-out list propagates API errors instead of hiding them", async () => {
  const failing = createRouterTransport(({ service }) => {
    service(SandboxService, {
      list() {
        throw new ConnectError("node busy", Code.Aborted, { "vmon-code": "busy" });
      },
    });
  });
  const healthy = createRouterTransport(({ service }) => {
    service(SandboxService, {
      list: () => ({ sandboxesJson: [JSON.stringify({ id: "healthy" })] }),
    });
  });
  const client = new Client(
    new MeshDriver("vmon://node-a,node-b?discover=off", {
      transport: (baseUrl) => (baseUrl === A_8000 ? failing : healthy),
    }),
  );
  await expect(client.sandboxes.list()).rejects.toMatchObject({ status: 409, code: "busy" });
});

test("sandbox transparently relocates once after pinned not_found", async () => {
  const id = "moved";
  const gone = createRouterTransport(({ service }) => {
    service(SandboxService, {
      get() {
        throw new ConnectError("gone", Code.NotFound, { "vmon-code": "not_found" });
      },
    });
  });
  const serving = createRouterTransport(({ service }) => {
    service(SandboxService, {
      get(req) {
        if (req.id === id) return { json: JSON.stringify({ id, node: "b", state: "running" }) };
        throw new ConnectError("missing", Code.NotFound, { "vmon-code": "not_found" });
      },
    });
  });
  const driver = new MeshDriver("vmon://node-a,node-b?discover=off", {
    transport: (baseUrl) => (baseUrl === A_8000 ? gone : serving),
  });
  const sandbox = new Sandbox(new Client(driver), { id }, A_8000);
  expect((await sandbox.refresh()).node).toBe("b");
  expect(sandbox.endpoint).toBe(B_8000);

  const absent = new Sandbox(new Client(driver), { id: "absent" }, A_8000);
  await expect(absent.refresh()).rejects.toBeInstanceOf(APIError);
});

