import { expect, test } from "bun:test";

import { connect } from "../src";

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
  test("remote function executes in a real Node guest", async () => {
    const stdout: string[] = [];
    const remoteAdd = connect(serverUrl, { token: apiToken }).remoteFunction(
      function remoteAdd(left: number, right: number) {
        console.log("vmon TypeScript remote smoke");
        return { sum: left + right };
      },
      {
        block_network: true,
        onStdout: (output) => stdout.push(output),
        ...(remoteImage === undefined ? {} : { image: remoteImage }),
      },
    );

    try {
      await expect(remoteAdd.remote(19, 23)).resolves.toEqual({ sum: 42 });
      expect(stdout).toEqual(["vmon TypeScript remote smoke\n"]);
    } finally {
      await remoteAdd.terminate();
    }
  }, 600_000);
}
