import { afterEach, expect, test } from "bun:test";
import { createServer, type Server, type Socket } from "node:net";
import { create, fromBinary, type MessageInitShape, toBinary } from "@bufbuild/protobuf";
import { ProtocolError } from "../src";
import {
  type HostGatewayInput,
  HostGatewayInputSchema,
  HostGatewayOutputSchema,
} from "../src/gen/vmon/v1/api_pb";
import { openHostGateway } from "../src/host-gateway";
import { bridgeServer, clientFor } from "./sessions.test";

const targets: Server[] = [];
const targetSockets = new Set<Socket>();
afterEach(() => {
  for (const socket of targetSockets) socket.destroy();
  targetSockets.clear();
  for (const target of targets.splice(0)) target.close();
});

function gatewayOutput(
  output: MessageInitShape<typeof HostGatewayOutputSchema>["output"],
): Uint8Array {
  return toBinary(HostGatewayOutputSchema, create(HostGatewayOutputSchema, { output }));
}

async function listenTarget(
  onConnection: (socket: Socket) => void,
  host = "127.0.0.1",
): Promise<{ server: Server; port: number }> {
  const server = createServer((socket) => {
    targetSockets.add(socket);
    socket.once("close", () => targetSockets.delete(socket));
    onConnection(socket);
  });
  targets.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, host, resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("missing TCP address");
  return { server, port: address.port };
}

test("HostGateway validates ready and relays bytes in both directions", async () => {
  const targetData = Promise.withResolvers<string>();
  const targetClosed = Promise.withResolvers<void>();
  const returnedData = Promise.withResolvers<string>();
  const { port } = await listenTarget((socket) => {
    socket.on("data", (data) => {
      targetData.resolve(data.toString());
      socket.write("from-target");
    });
    socket.on("close", () => targetClosed.resolve());
  });
  const inputs: HostGatewayInput["input"][] = [];
  const server = bridgeServer({
    "/vmon.v1.SandboxService/HostGateway": {
      onMessage(conn, payload) {
        const input = fromBinary(HostGatewayInputSchema, payload).input;
        inputs.push(input);
        if (input.case === "attach") {
          conn.sendMessage(
            gatewayOutput({ case: "ready", value: { url: "http://192.0.2.1:18080" } }),
          );
          conn.sendMessage(gatewayOutput({ case: "open", value: { conn: 7n } }));
          conn.sendMessage(
            gatewayOutput({
              case: "data",
              value: { conn: 7n, data: new TextEncoder().encode("from-guest") },
            }),
          );
        } else if (input.case === "data") {
          returnedData.resolve(new TextDecoder().decode(input.value.data));
          conn.sendMessage(gatewayOutput({ case: "close", value: { conn: input.value.conn } }));
        }
      },
    },
  });

  const gateway = await clientFor(server).sandboxes.ref("sandbox-1").hostGateway(`:${port}`);
  expect(gateway.url).toBe("http://192.0.2.1:18080");
  expect(await targetData.promise).toBe("from-guest");
  expect(await returnedData.promise).toBe("from-target");
  await targetClosed.promise;
  expect(inputs[0]).toMatchObject({ case: "attach", value: { sandboxId: "sandbox-1" } });
  gateway.close();
  await gateway.closed;
});

test("HostGateway close cancels the RPC and destroys active target sockets", async () => {
  const connected = Promise.withResolvers<void>();
  const targetClosed = Promise.withResolvers<void>();
  const { port } = await listenTarget((socket) => {
    connected.resolve();
    socket.on("close", () => targetClosed.resolve());
  });
  const server = bridgeServer({
    "/vmon.v1.SandboxService/HostGateway": {
      onMessage(conn, payload) {
        if (fromBinary(HostGatewayInputSchema, payload).input.case !== "attach") return;
        conn.sendMessage(gatewayOutput({ case: "ready", value: { url: "http://guest:18080" } }));
        conn.sendMessage(gatewayOutput({ case: "open", value: { conn: 1n } }));
      },
    },
  });

  const gateway = await clientFor(server)
    .sandboxes.ref("sandbox-1")
    .hostGateway(`https://127.0.0.1:${port}`);
  await connected.promise;
  gateway.close();
  await gateway.closed;
  await targetClosed.promise;
});

test("HostGateway rejects a stream whose first output is not ready", async () => {
  let cancelled = false;
  await expect(
    openHostGateway("sandbox-1", "127.0.0.1:1", async () => ({
      stream: (async function* () {
        yield create(HostGatewayOutputSchema, {
          output: { case: "open", value: { conn: 1n } },
        });
      })(),
      cancel: () => {
        cancelled = true;
      },
    })),
  ).rejects.toBeInstanceOf(ProtocolError);
  expect(cancelled).toBeTrue();
});

test("HostGateway rejects frames for unknown connections and cancels the RPC", async () => {
  let cancelled = false;
  const gateway = await openHostGateway("sandbox-1", "127.0.0.1:1", async () => ({
    stream: (async function* () {
      yield create(HostGatewayOutputSchema, {
        output: { case: "ready", value: { url: "http://guest:18080" } },
      });
      yield create(HostGatewayOutputSchema, {
        output: { case: "data", value: { conn: 99n, data: new Uint8Array([1]) } },
      });
    })(),
    cancel: () => {
      cancelled = true;
    },
  }));

  await expect(gateway.closed).rejects.toBeInstanceOf(ProtocolError);
  expect(cancelled).toBeTrue();
});

test("HostGateway reports a close frame when dialing the target fails", async () => {
  const reserved = await listenTarget(() => {});
  const port = reserved.port;
  await new Promise<void>((resolve, reject) =>
    reserved.server.close((error) => (error ? reject(error) : resolve())),
  );
  targets.splice(targets.indexOf(reserved.server), 1);
  const dialClosed = Promise.withResolvers<bigint>();
  const server = bridgeServer({
    "/vmon.v1.SandboxService/HostGateway": {
      onMessage(conn, payload) {
        const input = fromBinary(HostGatewayInputSchema, payload).input;
        if (input.case === "attach") {
          conn.sendMessage(gatewayOutput({ case: "ready", value: { url: "http://guest:18080" } }));
          conn.sendMessage(gatewayOutput({ case: "open", value: { conn: 42n } }));
        } else if (input.case === "close") {
          conn.sendMessage(gatewayOutput({ case: "close", value: { conn: input.value.conn } }));
          if (input.value.conn === 42n)
            conn.sendMessage(gatewayOutput({ case: "open", value: { conn: 43n } }));
          else dialClosed.resolve(input.value.conn);
        }
      },
    },
  });

  const gateway = await clientFor(server)
    .sandboxes.ref("sandbox-1")
    .hostGateway(`127.0.0.1:${port}`);
  expect(await dialClosed.promise).toBe(43n);
  gateway.close();
  await gateway.closed;
});

test("HostGateway rejects invalid targets before opening an RPC", async () => {
  const sandbox = clientFor(bridgeServer({})).sandboxes.ref("sandbox-1");
  for (const target of [
    "ftp://localhost:80",
    "http://localhost/path",
    "localhost",
    ":0",
    "[::1",
    "localhost:65536",
  ]) {
    await expect(sandbox.hostGateway(target)).rejects.toBeInstanceOf(TypeError);
  }
});
