import { afterEach, expect, test } from "bun:test";
import type { Driver, DriverRequestOptions, DriverResponse, EndpointInfo } from "../src";
import { APIError, Client, EventStream, LogStream, MeshDriver, Sandbox } from "../src";

const servers: Bun.Server<unknown>[] = [];
afterEach(() => {
  for (const server of servers.splice(0)) server.stop(true);
});

function sessionServer(frames: unknown[]): Bun.Server<unknown> {
  const server = Bun.serve({
    port: 0,
    fetch(request, server) {
      return server.upgrade(request)
        ? undefined
        : new Response("upgrade required", { status: 426 });
    },
    websocket: {
      open() {},
      message(socket, message) {
        const frame = JSON.parse(String(message));
        frames.push(frame);
        if (frame.stdin_b64) {
          socket.send(JSON.stringify({ stream: "stdout", b64: frame.stdin_b64 }));
          socket.send(JSON.stringify({ exit: 0, signal: null }));
        }
      },
    },
  });
  servers.push(server);
  return server;
}

test("Process sends request, resize, stdin and decodes stdout and exit", async () => {
  const frames: unknown[] = [];
  const server = sessionServer(frames);
  const client = new Client(new MeshDriver(`http://127.0.0.1:${server.port}`, { discover: false }));
  const stdout: string[] = [];
  const process = await client.sandboxes.ref("s1").exec(["cat"], {
    tty: true,
    onStdout: (data) => stdout.push(new TextDecoder().decode(data)),
  });
  process.resize(24, 80);
  process.stdin.write("hi");
  const events = [];
  for await (const event of process)
    events.push({ stream: event.stream, text: new TextDecoder().decode(event.data) });
  expect(await process.wait()).toEqual({ code: 0, signal: null });
  expect(frames).toEqual([
    { cmd: ["cat"], tty: true },
    { resize: [24, 80] },
    { stdin_b64: btoa("hi") },
  ]);
  expect(events).toEqual([{ stream: "stdout", text: "hi" }]);
  expect(stdout).toEqual(["hi"]);
});

test("Process turns an error envelope into APIError", async () => {
  const server = Bun.serve({
    port: 0,
    fetch(request, server) {
      return server.upgrade(request) ? undefined : new Response(null, { status: 426 });
    },
    websocket: {
      open(socket) {
        queueMicrotask(() =>
          socket.send(JSON.stringify({ error: { code: "not_running", message: "stopped" } })),
        );
      },
      message() {},
    },
  });
  servers.push(server);
  const client = new Client(new MeshDriver(`http://127.0.0.1:${server.port}`, { discover: false }));
  const process = await client.sandboxes.ref("s1").exec(["true"]);
  await expect(process.wait()).rejects.toBeInstanceOf(APIError);
});

test("attach returns a read-only console stream that closes its socket", async () => {
  const frames: unknown[] = [];
  const server = sessionServer(frames);
  const client = new Client(new MeshDriver(`http://127.0.0.1:${server.port}`, { discover: false }));
  const console = await client.sandboxes.ref("s1").attach();
  console.close();
  expect(frames).toEqual([]);
});

test("sandbox WebSocket helper relocates a migrated sandbox after not_found", async () => {
  const socket = {} as WebSocket;
  let attempts = 0;
  const driver: Driver = {
    request(
      _method: string,
      _path: string,
      _options?: DriverRequestOptions,
    ): Promise<DriverResponse> {
      throw new Error("unexpected HTTP request");
    },
    websocket(): Promise<[WebSocket, string]> {
      attempts += 1;
      if (attempts === 1) throw new APIError({ status: 404, code: "not_found", message: "moved" });
      return Promise.resolve([socket, "http://node-b"]);
    },
    resolveSandbox(): Promise<string> {
      return Promise.resolve("http://node-b");
    },
    endpoints(): EndpointInfo[] {
      return [
        { url: "http://node-a", healthy: true, source: "seed" },
        { url: "http://node-b", healthy: true, source: "seed" },
      ];
    },
    refresh(): Promise<void> {
      return Promise.resolve();
    },
    close() {},
  };
  const sandbox = new Sandbox(new Client(driver), { id: "moved" }, "http://node-a");
  expect(await sandbox.websocket("/attach")).toEqual([socket, "http://node-b"]);
  expect(sandbox.endpoint).toBe("http://node-b");
});

test("SSE stream close methods cancel their response bodies", async () => {
  let eventCancelled = false;
  let logCancelled = false;
  const events = new EventStream(
    new Response(
      new ReadableStream({
        cancel() {
          eventCancelled = true;
        },
      }),
    ),
  );
  const logs = new LogStream(
    new Response(
      new ReadableStream({
        cancel() {
          logCancelled = true;
        },
      }),
    ),
  );
  await Promise.all([events.close(), logs.close()]);
  expect(eventCancelled).toBe(true);
  expect(logCancelled).toBe(true);
});
