import { afterAll, expect, test } from "bun:test";
import type { ChildProcess } from "node:child_process";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { resolve } from "node:path";

import { type Client, connect } from "../src";

const wantsSmoke = process.env.VMON_TS_SMOKE === "1";
const configuredUrl = process.env.VMON_SERVER_URL;
const configuredToken = process.env.VMON_API_TOKEN;
const localBinary = resolve(import.meta.dir, "../../../target/debug/vmon");

let spawnedChild: ChildProcess | null = null;
let spawnedHome: string | null = null;
let spawnedStdout = "";
let spawnedStderr = "";

if (!wantsSmoke) {
  console.log("SKIP sdk-ts smoke: set VMON_TS_SMOKE=1 to run the vmon API smoke test");
  test.skip("gated vmon API smoke", () => {});
} else if (
  configuredUrl !== undefined &&
  configuredUrl.length > 0 &&
  configuredToken !== undefined &&
  configuredToken.length > 0
) {
  test("health, volume CRUD, and sandbox listing", async () => {
    await exerciseApi(connect(configuredUrl, { token: configuredToken }));
  });
} else if (await Bun.file(localBinary).exists()) {
  test("health, volume CRUD, and sandbox listing", async () => {
    const target = await spawnLocalServer(localBinary);
    const client = connect(target.baseUrl, { token: target.token });
    await waitForHealth(client);
    await exerciseApi(client);
  });
} else {
  console.log(`SKIP sdk-ts smoke: VMON_TS_SMOKE=1 but ${localBinary} does not exist`);
  test.skip("gated vmon API smoke", () => {});
}

afterAll(async () => {
  await stopSpawnedServer();
  if (spawnedHome !== null) {
    await rm(spawnedHome, { recursive: true, force: true });
    spawnedHome = null;
  }
});

interface SpawnedTarget {
  baseUrl: string;
  token: string;
}

async function exerciseApi(client: Client): Promise<void> {
  const health = await client.health();
  expect(health.ok).toBe(true);

  const suffix = `${process.pid}_${Date.now().toString(36)}`;
  const volumeName = `ts_smoke_${suffix}`;
  let volumeCreated = false;

  try {
    await client.volumes.create(volumeName);
    volumeCreated = true;
    expect((await client.volumes.list()).map((volume) => volume.name)).toContain(volumeName);

    const sandboxes = await client.sandboxes.list();
    expect(Array.isArray(sandboxes)).toBe(true);
  } finally {
    if (volumeCreated) {
      await client.volumes.delete(volumeName).catch(() => {});
    }
  }

  expect((await client.volumes.list()).map((volume) => volume.name)).not.toContain(volumeName);
}

async function spawnLocalServer(binary: string): Promise<SpawnedTarget> {
  const port = await freeTcpPort();
  const token = `ts-smoke-${process.pid}-${Date.now().toString(36)}`;
  const home = await mkdtemp("/tmp/vmon-ts-sdk-");
  spawnedHome = home;
  spawnedStdout = "";
  spawnedStderr = "";
  spawnedChild = spawn(
    binary,
    ["serve", "--home", home, "--host", "127.0.0.1", "--port", String(port), "--token", token],
    { env: process.env, stdio: ["ignore", "pipe", "pipe"] },
  );
  spawnedChild.stdout?.on("data", (chunk: Buffer) => {
    spawnedStdout += chunk.toString("utf8");
  });
  spawnedChild.stderr?.on("data", (chunk: Buffer) => {
    spawnedStderr += chunk.toString("utf8");
  });
  return { baseUrl: `http://127.0.0.1:${port}`, token };
}

async function waitForHealth(client: Client): Promise<void> {
  const deadline = Date.now() + 10_000;
  let lastError: unknown = null;
  while (Date.now() < deadline) {
    if (spawnedChild !== null && spawnedChild.exitCode !== null) {
      throw new Error(serverFailureMessage("vmon serve exited before it became healthy"));
    }
    try {
      const health = await client.health();
      if (health.ok) {
        return;
      }
    } catch (error) {
      lastError = error;
    }
    // Integration startup waits on an external child process binding TCP; fake timers cannot advance that OS work.
    await Bun.sleep(50);
  }
  const detail = lastError instanceof Error ? lastError.message : "health check did not succeed";
  throw new Error(serverFailureMessage(`vmon serve did not become healthy: ${detail}`));
}

async function freeTcpPort(): Promise<number> {
  const server = createServer();
  const { promise, resolve: resolvePort, reject } = Promise.withResolvers<number>();
  server.once("error", reject);
  server.listen(0, "127.0.0.1", () => {
    const address = server.address();
    if (typeof address === "object" && address !== null) {
      const port = address.port;
      server.close((error) => {
        if (error) {
          reject(error);
        } else {
          resolvePort(port);
        }
      });
    } else {
      server.close();
      reject(new Error("failed to reserve a TCP port"));
    }
  });
  return promise;
}

async function stopSpawnedServer(): Promise<void> {
  const child = spawnedChild;
  spawnedChild = null;
  if (child === null) {
    return;
  }
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  child.kill("SIGTERM");
  // Cleanup is waiting for a real child process to exit; fake timers cannot deliver the OS exit signal.
  await Promise.race([childExit(child), Bun.sleep(2_000)]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await childExit(child);
  }
}

function childExit(child: ChildProcess): Promise<void> {
  const { promise, resolve: resolveExit } = Promise.withResolvers<void>();
  child.once("exit", () => resolveExit());
  return promise;
}

function serverFailureMessage(message: string): string {
  return `${message}\nstdout:\n${spawnedStdout}\nstderr:\n${spawnedStderr}`;
}
