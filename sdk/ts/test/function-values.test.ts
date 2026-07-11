import { expect, test } from "bun:test";
import type { ArtifactRef, ValueEnvelope } from "../src/gen/vmon/v1/api_pb";
import { ValueSerializer } from "../src/gen/vmon/v1/api_pb";
import type { PortableValue, ValueArtifactStore, ValueSerializerName } from "../src";
import { decodeValue, encodeValue } from "../src";

class MemoryArtifacts implements ValueArtifactStore {
  readonly values = new Map<string, Uint8Array>();
  async put(data: Uint8Array): Promise<ArtifactRef> {
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(data)));
    const key = Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
    this.values.set(key, data.slice());
    return {
      $typeName: "vmon.v1.ArtifactRef",
      digest: { $typeName: "vmon.v1.Digest", algorithm: 1, value: digest },
    };
  }
  async get(ref: ArtifactRef): Promise<Uint8Array> {
    if (!ref.digest) throw new Error("missing digest");
    const key = Array.from(ref.digest.value, (byte) => byte.toString(16).padStart(2, "0")).join("");
    const value = this.values.get(key);
    if (!value) throw new Error("missing artifact");
    return value.slice();
  }
}

const sample: PortableValue = {
  text: "portable ✓",
  count: 42,
  negative: -7,
  fraction: 1.25,
  enabled: true,
  nested: [null, "x", { ok: false }],
};

const serializers: ValueSerializerName[] = ["json", "cbor"];
async function cborFixture(bytes: number[]): Promise<ValueEnvelope> {
  const raw = Uint8Array.from(bytes);
  const envelope = await encodeValue(null, { serializer: "cbor" });
  envelope.storage = { case: "inlineData", value: raw };
  envelope.uncompressedSizeBytes = BigInt(raw.byteLength);
  if (!envelope.checksum) throw new Error("missing checksum");
  envelope.checksum.value = new Uint8Array(await crypto.subtle.digest("SHA-256", raw));
  return envelope;
}

test("JSON and CBOR envelopes round-trip with checksum and gzip", async () => {
  for (const serializer of serializers) {
    const envelope = await encodeValue(sample, { serializer, compression: "gzip" });
    expect(envelope.storage.case).toBe("inlineData");
    expect(envelope.checksum?.value.byteLength).toBe(32);
    expect(await decodeValue(envelope)).toEqual(sample);
  }
});

test("large values use immutable artifact references", async () => {
  const artifacts = new MemoryArtifacts();
  const envelope = await encodeValue(sample, {
    serializer: "cbor",
    inlineLimit: 0,
    artifactStore: artifacts,
  });
  expect(envelope.storage.case).toBe("artifact");
  expect(artifacts.values.size).toBe(1);
  expect(await decodeValue(envelope, { artifactStore: artifacts })).toEqual(sample);
});

test("checksum corruption and cloudpickle are rejected", async () => {
  const corrupt = await encodeValue(sample);
  if (!corrupt.checksum) throw new Error("missing checksum");
  corrupt.checksum.value[0] ^= 0xff;
  await expect(decodeValue(corrupt)).rejects.toThrow("checksum mismatch");

  const cloudpickle: ValueEnvelope = await encodeValue(sample);
  cloudpickle.serializer = ValueSerializer.CLOUDPICKLE;
  await expect(decodeValue(cloudpickle)).rejects.toThrow("Python-only");
});

test("non-portable values fail before encoding", async () => {
  await expect(encodeValue(Number.NaN)).rejects.toThrow("finite number");
});

test("I-JSON enforces safe integer boundaries while CBOR preserves larger exact doubles", async () => {
  for (const value of [Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER]) {
    expect(await decodeValue(await encodeValue(value, { serializer: "json" }))).toBe(value);
  }
  for (const value of [2 ** 53, -(2 ** 53)]) {
    await expect(encodeValue(value, { serializer: "json" })).rejects.toThrow(
      "I-JSON safe integer range",
    );
    expect(await decodeValue(await encodeValue(value, { serializer: "cbor" }))).toBe(value);
  }
});

test("CBOR bytes and 64-bit integers interoperate with shared codec fixtures", async () => {
  // Python cbor2.dumps([b"\x00\x01\xff", 2**53, -(2**53)-1], canonical=True)
  // and Go's canonical value codec produce these same bytes.
  const fixture = [
    0x83, 0x43, 0x00, 0x01, 0xff, 0x1b, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3b, 0x00,
    0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  ];
  const value: PortableValue = [new Uint8Array([0, 1, 255]), 2n ** 53n, -(2n ** 53n) - 1n];
  expect(await decodeValue(await cborFixture(fixture))).toEqual(value);

  const encoded = await encodeValue(value, { serializer: "cbor" });
  expect(encoded.storage.case).toBe("inlineData");
  if (encoded.storage.case !== "inlineData") throw new Error("expected inline CBOR");
  expect(Array.from(encoded.storage.value)).toEqual(fixture);
  expect(await decodeValue(encoded)).toEqual(value);
});

test("JSON rejects CBOR-only bytes and bigint values", async () => {
  await expect(encodeValue(new Uint8Array([1]), { serializer: "json" })).rejects.toThrow(
    "not supported by JSON",
  );
  await expect(encodeValue(2n ** 53n, { serializer: "json" })).rejects.toThrow(
    "not supported by JSON",
  );
});
