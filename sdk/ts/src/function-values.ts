import type { ArtifactRef, ValueEnvelope } from "./gen/vmon/v1/api_pb";
import { DigestAlgorithm, ValueCompression, ValueSerializer } from "./gen/vmon/v1/api_pb";

/** Values accepted by the portable JSON and CBOR function codecs. */
export type PortableValue =
  | null
  | boolean
  | number
  | string
  | PortableValue[]
  | { [key: string]: PortableValue };
/** Portable serializers supported by this SDK. */
export type ValueSerializerName = "json" | "cbor";
/** Compression supported by the portable value codec. */
export type ValueCompressionName = "none" | "gzip";
/** Artifact operations used when a value is too large to inline. */
export interface ValueArtifactStore {
  put(data: Uint8Array, mediaType: string): Promise<ArtifactRef>;
  get(ref: ArtifactRef): Promise<Uint8Array>;
}
/** Value encoding settings. */
export interface EncodeValueOptions {
  serializer?: ValueSerializerName;
  compression?: ValueCompressionName;
  artifactStore?: ValueArtifactStore;
  inlineLimit?: number;
  typeName?: string;
}
/** Value decoding settings. */
export interface DecodeValueOptions {
  artifactStore?: ValueArtifactStore;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

/** Encode a JSON-compatible value into a checksummed portable envelope. */
export async function encodeValue(
  value: PortableValue,
  options: EncodeValueOptions = {},
): Promise<ValueEnvelope> {
  const serializer = options.serializer ?? "json";
  if (serializer !== "json" && serializer !== "cbor") {
    if (serializer === "cloudpickle")
      throw new Error("cloudpickle values are Python-only and cannot be encoded by TypeScript");
    throw new Error(`unsupported value serializer ${String(serializer)}`);
  }
  const compression = options.compression ?? "none";
  if (compression !== "none" && compression !== "gzip")
    throw new Error(`unsupported value compression ${String(compression)}`);
  const raw =
    serializer === "json"
      ? encodeJson(value)
      : (checkPortable(value, "$", new Set<object>(), false), encodeCbor(value));
  const stored = compression === "gzip" ? await transformBytes(raw, "gzip", true) : raw;
  const checksum = await sha256(raw);
  const inlineLimit = options.inlineLimit ?? 256 * 1024;
  if (!Number.isSafeInteger(inlineLimit) || inlineLimit < 0)
    throw new RangeError("inlineLimit must be a non-negative safe integer");
  let storage: ValueEnvelope["storage"];
  if (stored.byteLength > inlineLimit) {
    if (!options.artifactStore)
      throw new Error("artifactStore is required for values exceeding inlineLimit");
    storage = {
      case: "artifact",
      value: await options.artifactStore.put(stored, mediaType(serializer, compression)),
    };
  } else {
    storage = { case: "inlineData", value: stored };
  }
  return {
    $typeName: "vmon.v1.ValueEnvelope",
    schemaVersion: 1,
    serializer: serializer === "json" ? ValueSerializer.JSON : ValueSerializer.CBOR,
    compression: compression === "gzip" ? ValueCompression.GZIP : ValueCompression.NONE,
    checksum: {
      $typeName: "vmon.v1.Digest",
      algorithm: DigestAlgorithm.SHA256,
      value: checksum,
    },
    uncompressedSizeBytes: BigInt(raw.byteLength),
    storage,
    pythonPresence: { case: undefined },
    typeNamePresence:
      options.typeName === undefined
        ? { case: undefined }
        : { case: "typeName", value: options.typeName },
  };
}

/** Decode and verify one portable value envelope. Cloudpickle is always rejected. */
export async function decodeValue(
  envelope: ValueEnvelope,
  options: DecodeValueOptions = {},
): Promise<PortableValue> {
  if (envelope.schemaVersion !== 1)
    throw new Error(`unsupported value envelope schema ${envelope.schemaVersion}`);
  if (envelope.serializer === ValueSerializer.CLOUDPICKLE)
    throw new Error("cloudpickle values are Python-only and cannot be decoded by TypeScript");
  if (envelope.serializer !== ValueSerializer.JSON && envelope.serializer !== ValueSerializer.CBOR)
    throw new Error("unsupported value serializer");
  let stored: Uint8Array;
  if (envelope.storage.case === "inlineData") stored = envelope.storage.value;
  else if (envelope.storage.case === "artifact") {
    if (!options.artifactStore)
      throw new Error("artifactStore is required to decode an artifact-backed value");
    stored = await options.artifactStore.get(envelope.storage.value);
    await verifyArtifact(envelope.storage.value, stored);
  } else throw new Error("value envelope has no storage");
  let raw: Uint8Array;
  if (envelope.compression === ValueCompression.NONE) raw = stored;
  else if (envelope.compression === ValueCompression.GZIP)
    raw = await transformBytes(stored, "gzip", false);
  else throw new Error("unsupported value compression");
  if (BigInt(raw.byteLength) !== envelope.uncompressedSizeBytes)
    throw new Error("value envelope size mismatch");
  if (!envelope.checksum || envelope.checksum.algorithm !== DigestAlgorithm.SHA256)
    throw new Error("value envelope requires a SHA-256 checksum");
  if (!equalBytes(await sha256(raw), envelope.checksum.value))
    throw new Error("value envelope checksum mismatch");
  const decoded: PortableValue =
    envelope.serializer === ValueSerializer.JSON
      ? new JsonReader(textDecoder.decode(raw)).parse()
      : decodeCbor(raw);
  checkPortable(decoded, "$", new Set<object>(), envelope.serializer === ValueSerializer.JSON);
  return decoded;
}

function encodeJson(value: PortableValue): Uint8Array {
  return textEncoder.encode(writeJson(value, "$", new Set<object>()));
}
function writeJson(value: unknown, path: string, active: Set<object>): string {
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "string") {
    checkUnicode(value, path);
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError(`${path} must be a finite number`);
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new TypeError(`${path} integer exceeds the I-JSON safe integer range`);
    }
    return JSON.stringify(value);
  }
  if (typeof value !== "object") throw new TypeError(`${path} is not a portable value`);
  if (active.has(value)) throw new TypeError(`${path} contains a cycle`);
  active.add(value);
  let encoded: string;
  if (Array.isArray(value)) {
    const items: string[] = [];
    for (let index = 0; index < value.length; index += 1) {
      items.push(writeJson(value[index], `${path}[${index}]`, active));
    }
    encoded = `[${items.join(",")}]`;
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null)
      throw new TypeError(`${path} must be a plain object`);
    if ("toJSON" in value) throw new TypeError(`${path} must not define toJSON`);
    const items: string[] = [];
    for (const key of Object.keys(value)) {
      items.push(
        `${JSON.stringify(key)}:${writeJson(Reflect.get(value, key), `${path}.${key}`, active)}`,
      );
      checkUnicode(key, `${path} key`);
    }
    encoded = `{${items.join(",")}}`;
  }
  active.delete(value);
  return encoded;
}

function checkPortable(
  value: unknown,
  path: string,
  active: Set<object>,
  iJson: boolean,
): asserts value is PortableValue {
  if (value === null || typeof value === "boolean") return;
  if (typeof value === "string") {
    checkUnicode(value, path);
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError(`${path} must be a finite number`);
    if (iJson && Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new TypeError(`${path} integer exceeds the I-JSON safe integer range`);
    }
    return;
  }
  if (typeof value !== "object") throw new TypeError(`${path} is not a portable value`);
  if (active.has(value)) throw new TypeError(`${path} contains a cycle`);
  active.add(value);
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1)
      checkPortable(value[index], `${path}[${index}]`, active, iJson);
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null)
      throw new TypeError(`${path} must be a plain object`);
    for (const key in value)
      checkPortable(Reflect.get(value, key), `${path}.${key}`, active, iJson);
    for (const key of Object.keys(value)) checkUnicode(key, `${path} key`);
  }
  active.delete(value);
}

async function sha256(data: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(data)));
}
function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < left.byteLength; index += 1) difference |= left[index] ^ right[index];
  return difference === 0;
}
function mediaType(serializer: ValueSerializerName, compression: ValueCompressionName): string {
  const base = serializer === "json" ? "application/json" : "application/cbor";
  return compression === "gzip" ? `${base}+gzip` : base;
}
async function transformBytes(
  data: Uint8Array,
  format: "gzip",
  compress: boolean,
): Promise<Uint8Array> {
  const stream = compress ? new CompressionStream(format) : new DecompressionStream(format);
  const writer = stream.writable.getWriter();
  await writer.write(new Uint8Array(data));
  await writer.close();
  return new Uint8Array(await new Response(stream.readable).arrayBuffer());
}

function encodeCbor(value: PortableValue): Uint8Array {
  const output: number[] = [];
  writeCbor(value, output);
  return Uint8Array.from(output);
}
function writeCbor(value: PortableValue, output: number[]): void {
  if (value === null) {
    output.push(0xf6);
    return;
  }
  if (value === false) {
    output.push(0xf4);
    return;
  }
  if (value === true) {
    output.push(0xf5);
    return;
  }
  if (typeof value === "number") {
    if (Number.isSafeInteger(value) && !Object.is(value, -0)) {
      if (value >= 0) writeHead(0, value, output);
      else writeHead(1, -1 - value, output);
    } else {
      output.push(0xfb);
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setFloat64(0, value);
      output.push(...bytes);
    }
    return;
  }
  if (typeof value === "string") {
    const bytes = textEncoder.encode(value);
    writeHead(3, bytes.byteLength, output);
    output.push(...bytes);
    return;
  }
  if (Array.isArray(value)) {
    writeHead(4, value.length, output);
    for (const item of value) writeCbor(item, output);
    return;
  }
  const entries = Object.entries(value);
  writeHead(5, entries.length, output);
  for (const [key, item] of entries) {
    writeCbor(key, output);
    writeCbor(item, output);
  }
}
function writeHead(major: number, value: number, output: number[]): void {
  if (value < 24) {
    output.push((major << 5) | value);
    return;
  }
  if (value <= 0xff) {
    output.push((major << 5) | 24, value);
    return;
  }
  if (value <= 0xffff) {
    output.push((major << 5) | 25, value >>> 8, value & 0xff);
    return;
  }
  if (value <= 0xffffffff) {
    output.push(
      (major << 5) | 26,
      (value >>> 24) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 8) & 0xff,
      value & 0xff,
    );
    return;
  }
  const high = Math.floor(value / 0x1_0000_0000);
  const low = value - high * 0x1_0000_0000;
  output.push(
    (major << 5) | 27,
    (high >>> 24) & 0xff,
    (high >>> 16) & 0xff,
    (high >>> 8) & 0xff,
    high & 0xff,
    (low >>> 24) & 0xff,
    (low >>> 16) & 0xff,
    (low >>> 8) & 0xff,
    low & 0xff,
  );
}
function decodeCbor(data: Uint8Array): PortableValue {
  const reader = new CborReader(data);
  const value = reader.value();
  if (!reader.done()) throw new Error("trailing bytes in CBOR value");
  return value;
}
function checkUnicode(value: string, path: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (!(low >= 0xdc00 && low <= 0xdfff))
        throw new TypeError(`${path} contains an unpaired surrogate`);
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw new TypeError(`${path} contains an unpaired surrogate`);
    }
  }
}

async function verifyArtifact(ref: ArtifactRef, data: Uint8Array): Promise<void> {
  if (!ref.digest || ref.digest.algorithm !== DigestAlgorithm.SHA256) {
    throw new Error("artifact reference requires a SHA-256 digest");
  }
  if (!equalBytes(await sha256(data), ref.digest.value))
    throw new Error("artifact digest mismatch");
}

class JsonReader {
  #offset = 0;
  constructor(readonly text: string) {}
  parse(): PortableValue {
    const value = this.value("$");
    this.whitespace();
    if (this.#offset !== this.text.length) throw new Error("trailing data in JSON value");
    return value;
  }
  value(path: string): PortableValue {
    this.whitespace();
    const char = this.text[this.#offset];
    if (char === '"') return this.string(path);
    if (char === "[") return this.array(path);
    if (char === "{") return this.object(path);
    if (this.text.startsWith("true", this.#offset)) {
      this.#offset += 4;
      return true;
    }
    if (this.text.startsWith("false", this.#offset)) {
      this.#offset += 5;
      return false;
    }
    if (this.text.startsWith("null", this.#offset)) {
      this.#offset += 4;
      return null;
    }
    return this.number(path);
  }
  string(path: string): string {
    const start = this.#offset++;
    let escaped = false;
    while (this.#offset < this.text.length) {
      const code = this.text.charCodeAt(this.#offset++);
      if (escaped) {
        if (code === 0x75) {
          for (let count = 0; count < 4; count += 1) {
            const hex = this.text.charCodeAt(this.#offset++);
            if (
              !(
                (hex >= 0x30 && hex <= 0x39) ||
                (hex >= 0x41 && hex <= 0x46) ||
                (hex >= 0x61 && hex <= 0x66)
              )
            ) {
              throw new Error("invalid JSON unicode escape");
            }
          }
        } else if (![0x22, 0x5c, 0x2f, 0x62, 0x66, 0x6e, 0x72, 0x74].includes(code)) {
          throw new Error("invalid JSON escape");
        }
        escaped = false;
      } else if (code === 0x5c) {
        escaped = true;
      } else if (code === 0x22) {
        const parsed: unknown = JSON.parse(this.text.slice(start, this.#offset));
        if (typeof parsed !== "string") throw new Error("invalid JSON string");
        checkUnicode(parsed, path);
        return parsed;
      } else if (code < 0x20) {
        throw new Error("unescaped control character in JSON string");
      }
    }
    throw new Error("unterminated JSON string");
  }
  array(path: string): PortableValue[] {
    this.#offset += 1;
    const result: PortableValue[] = [];
    this.whitespace();
    if (this.text[this.#offset] === "]") {
      this.#offset += 1;
      return result;
    }
    while (true) {
      result.push(this.value(`${path}[${result.length}]`));
      this.whitespace();
      const delimiter = this.text[this.#offset++];
      if (delimiter === "]") return result;
      if (delimiter !== ",") throw new Error("invalid JSON array delimiter");
    }
  }
  object(path: string): Record<string, PortableValue> {
    this.#offset += 1;
    const result: Record<string, PortableValue> = {};
    this.whitespace();
    if (this.text[this.#offset] === "}") {
      this.#offset += 1;
      return result;
    }
    while (true) {
      this.whitespace();
      if (this.text[this.#offset] !== '"') throw new Error("JSON object key must be a string");
      const key = this.string(`${path} key`);
      if (Object.hasOwn(result, key)) throw new Error(`duplicate JSON object key ${key}`);
      this.whitespace();
      if (this.text[this.#offset++] !== ":") throw new Error("invalid JSON object separator");
      const value = this.value(`${path}.${key}`);
      Object.defineProperty(result, key, {
        value,
        enumerable: true,
        configurable: true,
        writable: true,
      });
      this.whitespace();
      const delimiter = this.text[this.#offset++];
      if (delimiter === "}") return result;
      if (delimiter !== ",") throw new Error("invalid JSON object delimiter");
    }
  }
  number(path: string): number {
    const start = this.#offset;
    while (this.#offset < this.text.length && !",]} \t\n\r".includes(this.text[this.#offset]))
      this.#offset += 1;
    const token = this.text.slice(start, this.#offset);
    if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(token))
      throw new Error("invalid JSON number");
    const value = Number(token);
    if (!Number.isFinite(value)) throw new TypeError(`${path} must be a finite number`);
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new TypeError(`${path} integer exceeds the I-JSON safe integer range`);
    }
    return value;
  }
  whitespace(): void {
    while (this.#offset < this.text.length && /[\t\n\r ]/.test(this.text[this.#offset]))
      this.#offset += 1;
  }
}

class CborReader {
  #offset = 0;
  constructor(readonly data: Uint8Array) {}
  done(): boolean {
    return this.#offset === this.data.byteLength;
  }
  value(): PortableValue {
    const head = this.byte();
    const major = head >>> 5;
    const additional = head & 31;
    if (major === 0) return this.length(additional);
    if (major === 1) return -1 - this.length(additional);
    if (major === 3) return textDecoder.decode(this.bytes(this.length(additional)));
    if (major === 4) {
      const count = this.length(additional);
      const result: PortableValue[] = [];
      for (let index = 0; index < count; index += 1) result.push(this.value());
      return result;
    }
    if (major === 5) {
      const count = this.length(additional);
      const result: { [key: string]: PortableValue } = {};
      for (let index = 0; index < count; index += 1) {
        const key = this.value();
        if (typeof key !== "string") throw new Error("CBOR object key is not a string");
        if (Object.hasOwn(result, key)) throw new Error("duplicate CBOR object key");
        Object.defineProperty(result, key, {
          value: this.value(),
          enumerable: true,
          configurable: true,
          writable: true,
        });
      }
      return result;
    }
    if (major === 7 && additional === 20) return false;
    if (major === 7 && additional === 21) return true;
    if (major === 7 && additional === 22) return null;
    if (major === 7 && additional === 27) return this.float64();
    throw new Error("unsupported CBOR value");
  }
  length(additional: number): number {
    if (additional < 24) return additional;
    if (additional === 24) return this.byte();
    if (additional === 25) return this.byte() * 256 + this.byte();
    if (additional === 26)
      return this.byte() * 0x1_000000 + this.byte() * 0x10000 + this.byte() * 0x100 + this.byte();
    if (additional === 27) {
      const high =
        this.byte() * 0x1_000000 + this.byte() * 0x10000 + this.byte() * 0x100 + this.byte();
      const low =
        this.byte() * 0x1_000000 + this.byte() * 0x10000 + this.byte() * 0x100 + this.byte();
      const value = high * 0x1_0000_0000 + low;
      if (!Number.isSafeInteger(value))
        throw new Error("CBOR integer exceeds JavaScript safe integer range");
      return value;
    }
    throw new Error("indefinite CBOR values are not supported");
  }
  float64(): number {
    const bytes = this.bytes(8);
    const value = new DataView(bytes.buffer, bytes.byteOffset, 8).getFloat64(0);
    if (!Number.isFinite(value)) throw new Error("CBOR number must be finite");
    return value;
  }
  byte(): number {
    if (this.#offset >= this.data.byteLength) throw new Error("truncated CBOR value");
    return this.data[this.#offset++];
  }
  bytes(length: number): Uint8Array {
    if (this.#offset + length > this.data.byteLength) throw new Error("truncated CBOR value");
    const result = this.data.subarray(this.#offset, this.#offset + length);
    this.#offset += length;
    return result;
  }
}
