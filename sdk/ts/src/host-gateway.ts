import { createConnection, type Socket } from "node:net";
import type { MessageInitShape } from "@bufbuild/protobuf";
import { AsyncQueue, deferred } from "./async-queue";
import { ProtocolError } from "./errors";
import type { HostGatewayInputSchema, HostGatewayOutput } from "./gen/vmon/v1/api_pb";
import type { StreamHandle } from "./process";

type HostGatewayInputInit = MessageInitShape<typeof HostGatewayInputSchema>;
type GatewayTarget = { host: string; port: number };

/** A live relay from a sandbox-private listener to a runner-owned TCP target. */
export interface HostGateway {
  /** Guest-visible URL of the sandbox-private gateway listener. */
  readonly url: string;
  /** Resolves when closed, or rejects if the relay fails. */
  readonly closed: Promise<void>;
  /** Cancel the gateway RPC and close every runner-owned target connection. */
  close(): void;
}

class HostGatewayRelay implements HostGateway {
  readonly #inputs = new AsyncQueue<HostGatewayInputInit>();
  readonly #ready = deferred<void>();
  readonly #done = deferred<void>();
  readonly #target: GatewayTarget;
  readonly #sockets = new Map<bigint, Socket>();
  readonly #closing = new Set<bigint>();
  #url: string | null = null;
  #cancel: (() => void) | null = null;
  #closed = false;

  constructor(
    sandboxId: string,
    target: GatewayTarget,
    open: (inputs: AsyncIterable<HostGatewayInputInit>) => Promise<StreamHandle<HostGatewayOutput>>,
  ) {
    this.#target = target;
    this.#inputs.push({ input: { case: "attach", value: { sandboxId } } });
    void this.#run(open);
  }

  get url(): string {
    if (this.#url === null) throw new ProtocolError("host gateway is not ready");
    return this.#url;
  }

  get closed(): Promise<void> {
    return this.#done.promise;
  }

  async waitReady(): Promise<void> {
    await this.#ready.promise;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#inputs.end();
    this.#cancel?.();
    this.#destroySockets();
  }

  async #run(
    open: (inputs: AsyncIterable<HostGatewayInputInit>) => Promise<StreamHandle<HostGatewayOutput>>,
  ): Promise<void> {
    try {
      const handle = await open(this.#inputs);
      this.#cancel = () => handle.cancel();
      if (this.#closed) handle.cancel();
      for await (const output of handle.stream) {
        if (this.#closed) break;
        this.#deliver(output);
      }
      if (!this.#closed) {
        throw new ProtocolError(
          this.#url === null
            ? "host gateway stream ended before a ready frame"
            : "host gateway stream ended unexpectedly",
        );
      }
      this.#done.resolve();
    } catch (error) {
      const failure =
        error instanceof Error ? error : new ProtocolError("host gateway stream failed");
      const userClosed = this.#closed;
      const wasReady = this.#url !== null;
      if (!wasReady) {
        this.#ready.reject(failure);
        this.#done.resolve();
      } else if (userClosed) {
        this.#done.resolve();
      } else {
        this.#done.reject(failure);
      }
      this.#closed = true;
      this.#inputs.end();
      if (!userClosed) this.#cancel?.();
      this.#destroySockets();
    } finally {
      this.#closed = true;
      this.#inputs.end();
      this.#destroySockets();
    }
  }

  #deliver(output: HostGatewayOutput): void {
    if (this.#url === null) {
      if (output.output.case !== "ready" || output.output.value.url.length === 0)
        throw new ProtocolError("host gateway stream did not begin with a ready frame");
      this.#url = output.output.value.url;
      this.#ready.resolve();
      return;
    }

    switch (output.output.case) {
      case "open":
        this.#openConnection(output.output.value.conn);
        return;
      case "data": {
        const { conn, data } = output.output.value;
        const socket = this.#sockets.get(conn);
        if (!socket) throw new ProtocolError(`host gateway data for unknown connection ${conn}`);
        try {
          socket.write(data, (error) => {
            if (error) this.#closeConnection(conn, socket, true);
          });
        } catch {
          this.#closeConnection(conn, socket, true);
        }
        return;
      }
      case "close": {
        const { conn } = output.output.value;
        const socket = this.#sockets.get(conn);
        if (socket) {
          this.#closeConnection(conn, socket, false);
          return;
        }
        if (this.#closing.delete(conn)) return;
        throw new ProtocolError(`host gateway close for unknown connection ${conn}`);
      }
      case "ready":
        throw new ProtocolError("host gateway received more than one ready frame");
      default:
        throw new ProtocolError("unknown host gateway output frame");
    }
  }

  #openConnection(conn: bigint): void {
    if (this.#sockets.has(conn) || this.#closing.has(conn))
      throw new ProtocolError(`host gateway opened duplicate connection ${conn}`);
    const socket = createConnection(this.#target);
    this.#sockets.set(conn, socket);
    socket.on("data", (data) => {
      if (this.#sockets.get(conn) !== socket) return;
      this.#send({ case: "data", value: { conn, data } });
    });
    socket.on("error", () => this.#closeConnection(conn, socket, true));
    socket.on("close", () => this.#closeConnection(conn, socket, true));
  }

  #closeConnection(conn: bigint, socket: Socket, notifyServer: boolean): void {
    if (this.#sockets.get(conn) !== socket) return;
    this.#sockets.delete(conn);
    socket.destroy();
    if (notifyServer) {
      this.#closing.add(conn);
      this.#send({ case: "close", value: { conn } });
    }
  }

  #send(input: HostGatewayInputInit["input"]): void {
    if (this.#closed) return;
    this.#inputs.push({ input });
  }

  #destroySockets(): void {
    const sockets = [...this.#sockets.values()];
    this.#sockets.clear();
    this.#closing.clear();
    for (const socket of sockets) socket.destroy();
  }
}

/** Open a host gateway and wait for its mandatory ready frame. */
export async function openHostGateway(
  sandboxId: string,
  target: string,
  open: (inputs: AsyncIterable<HostGatewayInputInit>) => Promise<StreamHandle<HostGatewayOutput>>,
): Promise<HostGateway> {
  const gateway = new HostGatewayRelay(sandboxId, parseTarget(target), open);
  await gateway.waitReady();
  return gateway;
}

function parseTarget(value: string): GatewayTarget {
  let authority: string;
  let defaultPort: number | undefined;
  if (value.startsWith("http://")) {
    authority = value.slice("http://".length);
    defaultPort = 80;
  } else if (value.startsWith("https://")) {
    authority = value.slice("https://".length);
    defaultPort = 443;
  } else if (value.includes("://")) {
    throw new TypeError("gateway target URL must use http or https");
  } else {
    authority = value;
  }
  if (/[/?#]/.test(authority))
    throw new TypeError("gateway target URL must not contain a path, query, or fragment");

  let host: string;
  let portText: string | undefined;
  if (authority.startsWith("[")) {
    const end = authority.indexOf("]");
    if (end < 0) throw new TypeError("gateway target has an invalid IPv6 address");
    host = authority.slice(1, end);
    const suffix = authority.slice(end + 1);
    if (suffix.length === 0) {
      if (defaultPort === undefined) throw new TypeError("gateway target requires a port");
    } else if (!suffix.startsWith(":")) {
      throw new TypeError("gateway target has an invalid port");
    } else {
      portText = suffix.slice(1);
    }
  } else {
    const separator = authority.lastIndexOf(":");
    if (separator >= 0) {
      host = authority.slice(0, separator) || "127.0.0.1";
      portText = authority.slice(separator + 1);
    } else {
      host = authority;
      if (defaultPort === undefined) throw new TypeError("gateway target requires a port");
    }
  }

  let port = defaultPort;
  if (portText !== undefined) {
    if (!/^\d+$/.test(portText)) throw new TypeError("gateway target has an invalid port");
    port = Number(portText);
    if (!Number.isSafeInteger(port) || port > 65_535)
      throw new TypeError("gateway target has an invalid port");
  }
  if (host.length === 0 || port === undefined || port === 0)
    throw new TypeError("gateway target requires a non-empty host and nonzero port");
  return { host, port };
}
