import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseDsn } from "../src";

const saved = { ...process.env };
afterEach(() => {
  for (const key in process.env) if (!(key in saved)) delete process.env[key];
  for (const key in saved) process.env[key] = saved[key];
});

test("parses all DSN schemes and precedence", () => {
  expect(parseDsn("vmon://a,b:9000/api?token=d&discover=off&timeout=2")).toMatchObject({
    endpoints: [{ url: "http://a:8000/api" }, { url: "http://b:9000/api" }],
    token: "d",
    discover: false,
    timeout: 2,
  });
  expect(parseDsn("vmons://a").endpoints[0]?.url).toBe("https://a:8000");
  expect(parseDsn("http://a:12/root/").endpoints).toEqual([{ url: "http://a:12/root" }]);
  expect(parseDsn("vmon+unix:///tmp/vmond.sock").endpoints).toEqual([
    { url: "http://localhost", unix: "/tmp/vmond.sock" },
  ]);
  process.env.VMON_API_TOKEN = "env";
  expect(parseDsn("https://a", { token: "explicit" }).token).toBe("explicit");
});

test("resolves context DSNs and context token", () => {
  const home = mkdtempSync(join(tmpdir(), "vmon-ts-context-"));
  mkdirSync(join(home, "credentials"));
  writeFileSync(
    join(home, "contexts.json"),
    JSON.stringify({
      current: "dev",
      contexts: { dev: { name: "dev", endpoints: ["https://one:44"] } },
    }),
  );
  writeFileSync(join(home, "credentials/dev.token"), "context-token\n");
  process.env.VMON_HOME = home;
  expect(parseDsn("vmon+context://dev")).toMatchObject({
    endpoints: [{ url: "https://one:44" }],
    token: "context-token",
    context: "dev",
  });
  rmSync(home, { recursive: true, force: true });
});

test("rejects malformed and unsupported DSNs", () => {
  for (const value of [
    "vmon://a?wat=1",
    "vmon://a?discover=yes",
    "vmon://a?timeout=no",
    "http://u:p@a",
    "ftp://a",
    "vmon+unix://relative.sock",
  ])
    expect(() => parseDsn(value)).toThrow();
  expect(() => parseDsn("vmon://a?wat=1")).toThrow("invalid DSN parameter wat");
});
