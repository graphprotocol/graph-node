import "allocator/arena";
export { memory };

export class Result<V, E> {
  public value: V | null;
  public error: E | null;
}

/** Type hint for JSON values. */
export enum JSONValueKind {
  NULL = 0,
  BOOL = 1,
  NUMBER = 2,
  STRING = 3,
  ARRAY = 4,
  OBJECT = 5
}

/**
 * Pointer type for JSONValue data.
 *
 * Big enough to fit any pointer or native `this.data`.
 */
export type JSONValuePayload = u64;

export class JSONValue {
  kind: JSONValueKind;
  data: JSONValuePayload;

  isNull(): boolean {
    return this.kind == JSONValueKind.NULL;
  }

  toString(): string {
    assert(this.kind == JSONValueKind.STRING, "JSON value is not a string.");
    return changetype<string>(this.data as u32);
  }
}

export class JSONError {
  public line: u32;
  public column: u32;
  public message: string;
}

declare namespace json {
  function try_fromBytes(data: Bytes): Result<JSONValue, JSONError>;
}

export class Bytes extends Uint8Array {}

export function handleJsonError(data: Bytes): string {
  let result = json.try_fromBytes(data);
  if (result.error !== null) {
    return "ERROR: " + result.error.message;
  } else {
    return "OK: " + result.value.toString();
  }
}
