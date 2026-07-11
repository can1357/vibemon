import { expect, test } from "bun:test";

import { connect, FunctionCall } from "../src";

const wantsRemoteSmoke = process.env.VMON_TS_REMOTE_SMOKE === "1";
const serverUrl = process.env.VMON_SERVER_URL;
const apiToken = process.env.VMON_API_TOKEN;
const remoteImage = process.env.VMON_TS_REMOTE_IMAGE;

if (
  !wantsRemoteSmoke ||
  serverUrl === undefined ||
  serverUrl.length === 0 ||
  apiToken === undefined ||
  apiToken.length === 0
) {
  console.log(
    "SKIP sdk-ts remote VM smoke: set VMON_TS_REMOTE_SMOKE=1, VMON_SERVER_URL, and VMON_API_TOKEN",
  );
  test.skip("gated remote function VM smoke", () => {});
} else {
  test("remote functions execute in a real Node guest over one warm session", async () => {
    const stdout: string[] = [];
    const client = connect(serverUrl, { token: apiToken });
    const imageOption = remoteImage === undefined ? {} : { image: remoteImage };
    const remoteAdd = client.remoteFunction(
      function remoteAdd(left: number, right: number) {
        console.log("vmon TypeScript remote smoke");
        return { sum: left + right };
      },
      {
        block_network: true,
        onStdout: (output) => stdout.push(output),
        ...imageOption,
      },
    );

    try {
      const coldStart = performance.now();
      await expect(remoteAdd.remote(19, 23)).resolves.toEqual({ sum: 42 });
      const coldMs = performance.now() - coldStart;
      const warmStart = performance.now();
      await expect(remoteAdd.remote(1, 2)).resolves.toEqual({ sum: 3 });
      const warmMs = performance.now() - warmStart;
      console.log(`cold=${coldMs.toFixed(0)}ms warm=${warmMs.toFixed(0)}ms`);
      expect(stdout).toEqual(["vmon TypeScript remote smoke\n", "vmon TypeScript remote smoke\n"]);
      // Warm calls reuse the persistent session: no sandbox boot, no exec spawn.
      expect(warmMs).toBeLessThan(coldMs);

      const spawned = await remoteAdd.spawn(20, 22);
      await expect(spawned.get()).resolves.toEqual({ sum: 42 });
      await expect(FunctionCall.gather(spawned)).resolves.toEqual([{ sum: 42 }]);
    } finally {
      await remoteAdd.terminate();
    }

    const squares = client.remoteFunction(
      function* squares(limit: number) {
        for (let index = 0; index < limit; index += 1) yield index * index;
      },
      { block_network: true, ...imageOption },
    );
    try {
      const streamed: number[] = [];
      for await (const value of squares.remoteGen(4)) streamed.push(value);
      expect(streamed).toEqual([0, 1, 4, 9]);
    } finally {
      await squares.terminate();
    }
  }, 600_000);
}
