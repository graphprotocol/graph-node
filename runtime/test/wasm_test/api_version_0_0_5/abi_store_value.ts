enum IndexForAscTypeId {
  STRING = 0,
  ARRAY_BUFFER = 1,
  UINT8_ARRAY = 6,
  BIG_DECIMAL = 12,
  ARRAY_STRING = 18,
  STORE_VALUE = 31,
}

export function id_of_type(type_id_index: IndexForAscTypeId): usize {
  switch (type_id_index) {
    case IndexForAscTypeId.STRING:
      return idof<string>();
    case IndexForAscTypeId.ARRAY_BUFFER:
      return idof<ArrayBuffer>();
    case IndexForAscTypeId.UINT8_ARRAY:
      return idof<Uint8Array>();
    case IndexForAscTypeId.BIG_DECIMAL:
      return idof<BigDecimal>();
    case IndexForAscTypeId.ARRAY_STRING:
      return idof<Array<string>>();
    case IndexForAscTypeId.STORE_VALUE:
      return idof<Value>();
    default:
      return 0;
  }
}

export function allocate(n: usize): usize {
  return __alloc(n);
}

enum ValueKind {
    STRING = 0,
    INT = 1,
    BIG_DECIMAL = 2,
    BOOL = 3,
    ARRAY = 4,
    NULL = 5,
    BYTES = 6,
    BIG_INT = 7,
}

// Big enough to fit any pointer or native `this.data`.
type Payload = u64

type Bytes = Uint8Array;
type BigInt = Uint8Array;

export class BigDecimal {
    exp!: BigInt
    digits!: BigInt
}

export class Value {
    kind: ValueKind
    data: Payload
}

export function value_from_string(str: string): Value {
    let token = new Value();
    token.kind = ValueKind.STRING;
    token.data = changetype<u32>(str);
    return token
}

export function value_from_int(int: i32): Value {
    let value = new Value();
    value.kind = ValueKind.INT;
    value.data = int as u64
    return value
}

export function value_from_big_decimal(float: BigInt): Value {
    let value = new Value();
    value.kind = ValueKind.BIG_DECIMAL;
    value.data = changetype<u32>(float);
    return value
}

export function value_from_bool(bool: boolean): Value {
    let value = new Value();
    value.kind = ValueKind.BOOL;
    value.data = bool ? 1 : 0;
    return value
}

export function array_from_values(str: string, i: i32): Value {
    let array = new Array<Value>();
    array.push(value_from_string(str));
    array.push(value_from_int(i));

    let value = new Value();
    value.kind = ValueKind.ARRAY;
    value.data = changetype<u32>(array);
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
    value.data = changetype<u32>(bytes);
    return value
}

export function value_from_bigint(bigint: BigInt): Value {
    let value = new Value();
    value.kind = ValueKind.BIG_INT;
    value.data = changetype<u32>(bigint);
    return value
}

export function value_from_array(array: Array<string>): Value {
    let value = new Value()
    value.kind = ValueKind.ARRAY
    value.data = changetype<u32>(array)
    return value
}

// Test that this does not cause undefined behaviour in Rust.
export function invalid_discriminant(): Value {
    let token = new Value();
    token.kind = 70;
    token.data = changetype<u32>("blebers");
    return token
}
