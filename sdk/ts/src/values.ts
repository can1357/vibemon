/** Serialized secret accepted by sandbox creation. */
export interface SecretWire {
  name: string;
  values: Record<string, string>;
}
/** Accepted secret input forms. */
export type SecretInput = Secret | SecretWire | Record<string, string>;

/** Validated in-memory secret value object. */
export class Secret {
  readonly name: string;
  readonly values: Readonly<Record<string, string>>;
  /** Validate and store secret environment values. */
  constructor(values: Record<string, string> = {}, name = "secret") {
    this.name = checkedName(name, "secret name");
    const checked: Record<string, string> = {};
    for (const key in values) {
      const value = values[key];
      checked[checkedName(key, "secret environment names")] = checkedValue(value);
    }
    this.values = Object.freeze(checked);
  }
  /** Build a secret from a value dictionary. */
  static fromDict(values: Record<string, string>, name = "secret"): Secret {
    return new Secret(values, name);
  }
  /** Capture selected process environment variables. */
  static fromEnv(names: Iterable<string>, name = "env"): Secret {
    const values: Record<string, string> = {};
    for (const raw of names) {
      const key = checkedName(raw, "secret environment names");
      const value = typeof process === "undefined" ? undefined : process.env[key];
      if (value !== undefined) values[key] = checkedValue(value);
    }
    return new Secret(values, name);
  }
  /** Return sorted secret variable names. */
  names(): string[] {
    return Object.keys(this.values).sort();
  }
  /** Copy secret values as an exec environment. */
  asEnv(): Record<string, string> {
    return { ...this.values };
  }
  /** Serialize the secret for sandbox creation. */
  toWire(): SecretWire {
    return { name: this.name, values: this.asEnv() };
  }
}

/** Persistent volume value object and mount helper. */
export class Volume {
  readonly name: string;
  /** Create a named persistent volume value. */
  constructor(name: string) {
    if (!name) throw new TypeError("volume name must not be empty");
    this.name = name;
  }
  /** Build a sandbox volume mount descriptor. */
  mount(readOnly = false): { name: string; read_only: boolean } {
    return { name: this.name, read_only: readOnly };
  }
}

/** Normalize secret inputs for sandbox creation. */
export function secretWires(inputs: Iterable<SecretInput>): SecretWire[] {
  const result: SecretWire[] = [];
  for (const input of inputs) {
    if (input instanceof Secret) result.push(input.toWire());
    else if (isSecretWire(input)) result.push(new Secret(input.values, input.name).toWire());
    else result.push(new Secret(input).toWire());
  }
  return result;
}

/** Merge secret inputs into an exec environment. */
export function mergeSecretEnv(inputs: Iterable<SecretInput>): Record<string, string> {
  const result: Record<string, string> = {};
  for (const input of inputs) {
    const values =
      input instanceof Secret ? input.values : isSecretWire(input) ? input.values : input;
    for (const key in values) result[key] = values[key];
  }
  return result;
}

function isSecretWire(input: SecretWire | Record<string, string>): input is SecretWire {
  return (
    typeof input.name === "string" && typeof input.values === "object" && input.values !== null
  );
}

function checkedName(name: string, label: string): string {
  if (!name || name.includes("=") || name.includes("\0"))
    throw new TypeError(`${label} must be non-empty and contain no '=' or NUL`);
  return name;
}
function checkedValue(value: string): string {
  if (value.includes("\0")) throw new TypeError("secret values must contain no NUL bytes");
  return value;
}
