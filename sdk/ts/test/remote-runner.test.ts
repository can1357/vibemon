import { afterAll, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Subprocess } from "bun";

import { SESSION_RUNNER } from "../src";

const nodeBin = Bun.which("node");
if (nodeBin === null) {
  throw new Error("local Node.js is required to validate the session runner protocol");
}

const temporaryDirectories: string[] = [];

afterAll(async () => {
  await Promise.allSettled(
    temporaryDirectories.map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

/** Drives one local runner subprocess over the NDJSON session protocol. */
class RunnerHarness {
  readonly #child: Subprocess<"pipe", "pipe", "pipe">;
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;
  readonly #decoder = new TextDecoder();
  readonly #frames: Record<string, unknown>[] = [];
  #buffer = "";

  private constructor(runnerPath: string) {
    this.#child = Bun.spawn([nodeBin as string, runnerPath], {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
    });
    this.#reader = this.#child.stdout.getReader();
  }

  static async start(): Promise<RunnerHarness> {
    const directory = await mkdtemp(join(tmpdir(), "vmon-ts-runner-"));
    temporaryDirectories.push(directory);
    const runnerPath = join(directory, "runner.js");
    await Bun.write(runnerPath, SESSION_RUNNER);
    return new RunnerHarness(runnerPath);
  }

  send(op: Record<string, unknown>): void {
    this.#child.stdin.write(`${JSON.stringify(op)}\n`);
    this.#child.stdin.flush();
  }

  async next(): Promise<Record<string, unknown>> {
    while (this.#frames.length === 0) {
      const { value, done } = await this.#reader.read();
      if (done) throw new Error("runner stdout ended before a frame arrived");
      this.#buffer += this.#decoder.decode(value, { stream: true });
      let newline = this.#buffer.indexOf("\n");
      while (newline >= 0) {
        const line = this.#buffer.slice(0, newline).trim();
        this.#buffer = this.#buffer.slice(newline + 1);
        if (line.length > 0) {
          const frame: unknown = JSON.parse(line);
          if (typeof frame !== "object" || frame === null) {
            throw new Error(`runner emitted a non-object frame: ${line}`);
          }
          this.#frames.push(frame as Record<string, unknown>);
        }
        newline = this.#buffer.indexOf("\n");
      }
    }
    const frame = this.#frames.shift();
    if (frame === undefined) throw new Error("frame queue drained unexpectedly");
    return frame;
  }

  exited(): Promise<number> {
    return this.#child.exited;
  }

  kill(): void {
    this.#child.kill();
  }
}

test("runner greets, caches sources by hash, and shuts down cleanly", async () => {
  const session = await RunnerHarness.start();
  try {
    const hello = await session.next();
    expect(hello.event).toBe("hello");
    expect(Array.isArray(hello.node)).toBe(true);
    expect((hello.node as number[]).length).toBeGreaterThanOrEqual(2);
    expect((hello.node as number[])[0]).toBeGreaterThanOrEqual(18);

    session.send({
      op: "call",
      id: 1,
      hash: "sum-v1",
      exportName: "handle",
      source: "export function handle(a, b) { console.log('sum', a + b); return { sum: a + b }; }",
      args: { json: [2, 3] },
      mode: "value",
    });
    expect(await session.next()).toEqual({
      event: "out",
      id: 1,
      stream: "stdout",
      data: "sum 5\n",
    });
    expect(await session.next()).toEqual({ event: "result", id: 1, json: { sum: 5 } });

    // Second call carries the hash only: the guest namespace cache must serve it.
    session.send({
      op: "call",
      id: 2,
      hash: "sum-v1",
      exportName: "handle",
      args: { json: [10, 4] },
      mode: "value",
    });
    expect(await session.next()).toEqual({
      event: "out",
      id: 2,
      stream: "stdout",
      data: "sum 14\n",
    });
    expect(await session.next()).toEqual({ event: "result", id: 2, json: { sum: 14 } });

    session.send({
      op: "call",
      id: 3,
      hash: "never-sent",
      exportName: "handle",
      args: { json: [] },
      mode: "value",
    });
    const unknown = await session.next();
    expect(unknown.event).toBe("error");
    expect(unknown.message).toContain("unknown source hash");

    session.send({ op: "shutdown", id: 4 });
    expect(await session.next()).toEqual({ event: "result", id: 4, json: null });
    expect(await session.exited()).toBe(0);
  } finally {
    session.kill();
  }
});

test("runner streams generator yields and interleaved output in order", async () => {
  const session = await RunnerHarness.start();
  try {
    expect((await session.next()).event).toBe("hello");
    session.send({
      op: "call",
      id: 1,
      hash: "gen-v1",
      exportName: "squares",
      source:
        "export function* squares(n) { for (let i = 0; i < n; i += 1) { console.log('at', i); yield i * i; } }",
      args: { json: [3] },
      mode: "iter",
    });
    const events: Record<string, unknown>[] = [];
    while (true) {
      const frame = await session.next();
      events.push(frame);
      if (frame.event === "result" || frame.event === "error") break;
    }
    expect(
      events.map((frame) => [frame.event, frame.event === "out" ? frame.data : frame.json]),
    ).toEqual([
      ["out", "at 0\n"],
      ["yield", 0],
      ["out", "at 1\n"],
      ["yield", 1],
      ["out", "at 2\n"],
      ["yield", 4],
      ["result", null],
    ]);

    // Async generators stream the same way.
    session.send({
      op: "call",
      id: 2,
      hash: "agen-v1",
      exportName: "ticks",
      source: "export async function* ticks() { yield 'a'; yield 'b'; }",
      args: { json: [] },
      mode: "iter",
    });
    expect(await session.next()).toEqual({ event: "yield", id: 2, json: "a" });
    expect(await session.next()).toEqual({ event: "yield", id: 2, json: "b" });
    expect(await session.next()).toEqual({ event: "result", id: 2, json: null });
  } finally {
    session.kill();
  }
});

test("runner reports structured errors and mode mismatches", async () => {
  const session = await RunnerHarness.start();
  try {
    expect((await session.next()).event).toBe("hello");

    session.send({
      op: "call",
      id: 1,
      hash: "boom-v1",
      exportName: "boom",
      source:
        "export function boom(value) { console.error('warn ' + value); throw new RangeError('bad ' + value); }",
      args: { json: [7] },
      mode: "value",
    });
    expect(await session.next()).toEqual({
      event: "out",
      id: 1,
      stream: "stderr",
      data: "warn 7\n",
    });
    const failure = await session.next();
    expect(failure.event).toBe("error");
    expect(failure.etype).toBe("RangeError");
    expect(failure.message).toBe("bad 7");
    expect(failure.traceback).toContain("bad 7");

    session.send({
      op: "call",
      id: 2,
      hash: "gen-v2",
      exportName: "gen",
      source: "export function* gen() { yield 1; }",
      args: { json: [] },
      mode: "value",
    });
    const generatorMisuse = await session.next();
    expect(generatorMisuse.event).toBe("error");
    expect(generatorMisuse.message).toContain("call it with .remoteGen()");

    session.send({
      op: "call",
      id: 3,
      hash: "plain-v1",
      exportName: "plain",
      source: "export function plain() { return 5; }",
      args: { json: [] },
      mode: "iter",
    });
    const valueMisuse = await session.next();
    expect(valueMisuse.event).toBe("error");
    expect(valueMisuse.message).toContain("did not return a generator; use .remote()");

    session.send({
      op: "call",
      id: 4,
      hash: "date-v1",
      exportName: "bad",
      source: "export function bad() { return new Date(0); }",
      args: { json: [] },
      mode: "value",
    });
    const lossy = await session.next();
    expect(lossy.event).toBe("error");
    expect(lossy.etype).toBe("TypeError");
    expect(lossy.message).toContain("non-plain object");

    // The session must remain usable after user errors.
    session.send({
      op: "call",
      id: 5,
      hash: "plain-v1",
      exportName: "plain",
      args: { json: [] },
      mode: "value",
    });
    expect(await session.next()).toEqual({ event: "result", id: 5, json: 5 });
  } finally {
    session.kill();
  }
});
