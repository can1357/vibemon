import { expect, test } from "bun:test";
import type { Driver, DriverRequestOptions, DriverResponse, EndpointInfo } from "../src";
import { Client, ProtocolError } from "../src";

interface Call {
  method: string;
  path: string;
  options: DriverRequestOptions;
}
class RecordingDriver implements Driver {
  readonly token = "auth";
  readonly calls: Call[] = [];
  readonly websocketCalls: Array<{
    path: string;
    query?: URLSearchParams | Record<string, string>;
    endpoint?: string;
  }> = [];
  response: unknown = { ok: true };
  status = 200;
  request(
    method: string,
    path: string,
    options: DriverRequestOptions = {},
  ): Promise<DriverResponse> {
    this.calls.push({ method, path, options });
    const response = Response.json(this.response, { status: this.status });
    Object.defineProperty(response, "endpoint", { value: "http://node-a" });
    return Promise.resolve(response as DriverResponse);
  }
  websocket(
    path: string,
    options: { query?: URLSearchParams | Record<string, string>; endpoint?: string } = {},
  ): Promise<[WebSocket, string]> {
    this.websocketCalls.push({ path, ...options });
    return Promise.resolve([{} as WebSocket, "http://node-a"]);
  }
  resolveSandbox(): Promise<string> {
    return Promise.resolve("http://node-b");
  }
  endpoints(): EndpointInfo[] {
    return [{ url: "http://node-a", healthy: true, source: "seed" }];
  }
  refresh(): Promise<void> {
    return Promise.resolve();
  }
  close(): void {}
}

test("resource hierarchy maps endpoints and bodies", async () => {
  const driver = new RecordingDriver();
  const client = new Client(driver);
  driver.response = { id: "s1", node: "n1" };
  const sandbox = await client.sandboxes.create({ image: "alpine" });
  expect(driver.calls.at(-1)).toMatchObject({
    method: "POST",
    path: "/v1/sandboxes",
    options: { json: { image: "alpine" } },
  });

  driver.response = { exit: 0, stdout_b64: "", stderr_b64: "" };
  await sandbox.run(["echo", "ok"]);
  expect(driver.calls.at(-1)).toMatchObject({ method: "POST", path: "/v1/sandboxes/s1/exec" });
  await sandbox.files.writeText("/tmp/a b", "hello");
  expect(driver.calls.at(-1)).toMatchObject({ method: "PUT", path: "/v1/sandboxes/s1/files" });
  driver.response = { entries: [{ path: "/tmp/a b" }] };
  expect(await sandbox.files.list("/tmp")).toEqual([{ path: "/tmp/a b" }]);
  expect(driver.calls.at(-1)?.path).toBe("/v1/sandboxes/s1/files/list");
  driver.response = { path: "/tmp/a b", size: 5 };
  await sandbox.files.stat("/tmp/a b");
  expect(driver.calls.at(-1)?.path).toBe("/v1/sandboxes/s1/files/stat");
  await sandbox.files.delete("/tmp/tree", true);
  expect(driver.calls.at(-1)).toMatchObject({
    method: "DELETE",
    options: { params: { path: "/tmp/tree", recursive: true } },
  });

  driver.response = { block_network: true };
  await sandbox.setNetwork({ block_network: true });
  expect(driver.calls.at(-1)).toMatchObject({ method: "PUT", path: "/v1/sandboxes/s1/network" });
  driver.response = { connect_token: "tunnel-token", tunnels: {} };
  await sandbox.tunnels();
  driver.status = 418;
  const proxied = await sandbox.ports.http(8080, "GET", "/hello", {
    params: { token: "evil", access_token: "evil2", connect_token: "evil3", q: "ok" },
  });
  expect(proxied.status).toBe(418);
  driver.status = 200;
  const proxyParams = driver.calls.at(-1)?.options.params;
  expect(proxyParams).toBeInstanceOf(URLSearchParams);
  if (!(proxyParams instanceof URLSearchParams)) throw new Error("expected URLSearchParams");
  expect(proxyParams.toString()).toBe("q=ok&connect_token=tunnel-token");
  await expect(sandbox.ports.http(-1, "GET")).rejects.toBeInstanceOf(RangeError);
  await expect(sandbox.ports.websocket(65_536)).rejects.toBeInstanceOf(RangeError);

  driver.response = { snapshots: ["snap"] };
  expect(await client.snapshots.list()).toEqual(["snap"]);
  driver.response = { id: "restored" };
  await client.snapshots.restore("snap", { name: "copy" });
  expect(driver.calls.at(-1)?.path).toBe("/v1/snapshots/snap/restore");
  driver.response = { clones: [{ id: "clone-1" }] };
  expect((await client.snapshots.fork("snap", { count: 1 })).map((clone) => clone.id)).toEqual([
    "clone-1",
  ]);
  driver.response = { snapshot: "vm-snapshot" };
  expect(await sandbox.snapshot()).toBe("vm-snapshot");
  driver.response = { image: "fs-image" };
  expect(await sandbox.snapshotFilesystem()).toBe("fs-image");
  driver.response = { expires_at: 123 };
  expect((await sandbox.extend(30)).id).toBe("s1");
  driver.response = { target: "node-b" };
  expect((await sandbox.migrate("node-b")).id).toBe("s1");
  expect(sandbox.endpoint).toBe("http://node-b");
  driver.response = { size: 2 };
  await client.pools.set("image", 2, { image: "alpine" });
  expect(driver.calls.at(-1)).toMatchObject({
    method: "PUT",
    path: "/v1/pools/image",
    options: { json: { image: "alpine", size: 2 } },
  });
  driver.response = { image: { size: 2 }, other: { size: 1 } };
  expect((await client.pools.list()).map((pool) => pool.ref)).toEqual(["image", "other"]);
  const beforeClear = driver.calls.length;
  await client.pools.clear();
  expect(
    driver.calls
      .slice(beforeClear)
      .filter((call) => call.method === "DELETE")
      .map((call) => call.path),
  ).toEqual(["/v1/pools/image", "/v1/pools/other"]);
  driver.response = { self: { node_id: "a" }, peers: [{ node_id: "b" }], replicas_held: 0 };
  expect((await client.mesh.nodes()).map((node) => node.node_id)).toEqual(["a", "b"]);
});

test("pool list rejects malformed server data", async () => {
  const driver = new RecordingDriver();
  const client = new Client(driver);
  for (const response of [[], { bad: null }, { bad: { size: "NaN" } }]) {
    driver.response = response;
    await expect(client.pools.list()).rejects.toBeInstanceOf(ProtocolError);
  }
});
