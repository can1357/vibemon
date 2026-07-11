import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { APIError, Client, MeshDriver, Sandbox, TransportError } from "../src";

const servers: Bun.Server<unknown>[] = [];
afterEach(() => {
  for (const server of servers.splice(0)) server.stop(true);
});
function serve(fetch: (request: Request) => Response | Promise<Response>): Bun.Server<unknown> {
  const server = Bun.serve({ port: 0, fetch });
  servers.push(server);
  return server;
}
function base(server: Bun.Server<unknown>): string {
  return `http://127.0.0.1:${server.port}`;
}
function json(value: unknown, status = 200): Response {
  return Response.json(value, { status });
}

test("discovers a peer, fails over, and sticks to the successful endpoint", async () => {
  let bCalls = 0;
  const b = serve((request) => {
    bCalls += 1;
    return request.url.endsWith("/v1/mesh/status")
      ? json({ self: { node_id: "b", advertise: base(b) }, peers: [] })
      : json({ node: "b" });
  });
  let advertised = "";
  const a = serve((request) =>
    request.url.endsWith("/v1/mesh/status")
      ? json({
          self: { node_id: "a", advertise: base(a) },
          peers: [{ node_id: "b", advertise: advertised }],
        })
      : json({ node: "a" }),
  );
  advertised = base(b);
  const driver = new MeshDriver(base(a));
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ node: "a" });
  expect(driver.endpoints().map((entry) => entry.url)).toEqual([base(a), base(b)]);
  a.stop(true);
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ node: "b" });
  expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ node: "b" });
  expect(bCalls).toBeGreaterThanOrEqual(2);
});

test("refresh drops discovered peers absent from the latest mesh status", async () => {
  let includePeer = true;
  const peer = serve(() => json({ ok: true }));
  const seed = serve((request) =>
    request.url.endsWith("/v1/mesh/status")
      ? json({
          self: { node_id: "seed", advertise: base(seed) },
          peers: includePeer ? [{ node_id: "peer", advertise: base(peer) }] : [],
        })
      : json({ ok: true }),
  );
  const driver = new MeshDriver(base(seed));
  await driver.request("GET", "/healthz");
  expect(driver.endpoints().map((entry) => entry.url)).toEqual([base(seed), base(peer)]);
  includePeer = false;
  await driver.refresh(true);
  expect(driver.endpoints().map((entry) => entry.url)).toEqual([base(seed)]);
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
  expect((await driver.request("POST", "/v1/sandboxes")).status).toBe(503);
  expect(secondCalls).toBe(0);
  first.stop(true);
  second.stop(true);
  await expect(driver.request("GET", "/healthz")).rejects.toBeInstanceOf(TransportError);
});

test("fan-out list propagates API errors instead of hiding them", async () => {
  const failing = serve(() => json({ code: "busy", message: "node busy" }, 503));
  const healthy = serve(() => json({ sandboxes: [{ id: "healthy" }] }));
  const client = new Client(
    new MeshDriver(`vmon://127.0.0.1:${failing.port},127.0.0.1:${healthy.port}?discover=off`),
  );
  await expect(client.sandboxes.list()).rejects.toMatchObject({ status: 503, code: "busy" });
});

test("sandbox transparently relocates once after pinned not_found", async () => {
  const id = "moved";
  const a = serve(() => json({ code: "not_found", message: "gone" }, 404));
  const b = serve((request) =>
    request.url.endsWith(`/v1/sandboxes/${id}`)
      ? json({ id, node: "b", state: "running" })
      : json({ code: "not_found", message: "missing" }, 404),
  );
  const driver = new MeshDriver(`vmon://127.0.0.1:${a.port},127.0.0.1:${b.port}?discover=off`);
  const sandbox = new Sandbox(new Client(driver), { id }, base(a));
  expect((await sandbox.refresh()).node).toBe("b");
  expect(sandbox.endpoint).toBe(base(b));

  const absent = new Sandbox(new Client(driver), { id: "absent" }, base(a));
  await expect(absent.refresh()).rejects.toBeInstanceOf(APIError);
});

test("uses Bun's unix fetch transport for vmon+unix DSNs", async () => {
  const directory = mkdtempSync(join(tmpdir(), "vmon-ts-uds-"));
  const socket = join(directory, "vmond.sock");
  const server = Bun.serve({
    unix: socket,
    fetch: () => json({ transport: "unix" }),
  });
  servers.push(server);
  try {
    const driver = new MeshDriver(`vmon+unix://${socket}?discover=off`);
    expect(await (await driver.request("GET", "/healthz")).json()).toEqual({ transport: "unix" });
  } finally {
    server.stop(true);
    const index = servers.indexOf(server);
    if (index >= 0) servers.splice(index, 1);
    rmSync(directory, { recursive: true, force: true });
  }
});
