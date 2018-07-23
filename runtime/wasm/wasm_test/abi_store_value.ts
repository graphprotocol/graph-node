import "allocator/arena";

export { allocate_memory, free_memory };

enum ValueKind {
    STRING,
    INT,
    FLOAT,
    BOOL,
    ARRAY,
    NULL,
    BYTES,
}

// Big enough to fit any pointer or native `this.data`.
type Payload = u64

type Bytes = Uint8Array;

export class Value {
    kind: ValueKind
    data: Payload
}

export function value_from_string(str: string): Value {  
    let token = new Value();
    token.kind = ValueKind.STRING;
    token.data = str as u64;
    return token
}

export function value_from_int(int: i32): Value {
    let value = new Value();
    value.kind = ValueKind.INT;
    value.data = int as u64
    return value
}
  
export function value_from_float(float: f32): Value {
    let value = new Value();
    value.kind = ValueKind.FLOAT;
    value.data = reinterpret<u64>(float as f64)
    return value
}

export function value_from_bool(bool: boolean): Value {
    let value = new Value();
    value.kind = ValueKind.BOOL;
    value.data = bool ? 1 : 0;
    return value
}
  
export function array_from_values(str: string, float: f32): Value {
    let array = new Array<Value>();
    array.push(value_from_string(str));
    array.push(value_from_float(float));

    let value = new Value();
    value.kind = ValueKind.ARRAY;
    value.data = array as u64;
    return value
}
  
export function value_null(): Value {
    let value = new Value();
    value.kind = ValueKind.NULL;
    return value
}

export function value_from_bytes(bytes: Bytes): Value {
    let value = new Value();
    value.kind = ValueKind.BYTES;
    value.data = bytes as u64;
    return value
}
