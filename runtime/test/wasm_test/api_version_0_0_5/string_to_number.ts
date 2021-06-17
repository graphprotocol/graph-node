enum IndexForAscTypeId {
  STRING = 0,
  ARRAY_BUFFER = 1,
  UINT8_ARRAY = 6,
}

export function id_of_type(type_id_index: IndexForAscTypeId): usize {
  switch (type_id_index) {
    case IndexForAscTypeId.STRING:
      return idof<string>();
    case IndexForAscTypeId.ARRAY_BUFFER:
      return idof<ArrayBuffer>();
    case IndexForAscTypeId.UINT8_ARRAY:
      return idof<Uint8Array>();
    default:
      return 0;
  }
}

export function allocate(n: usize): usize {
  return __alloc(n);
}

type BigInt = Uint8Array;

/** Host JSON interface */
declare namespace json {
    function toI64(decimal: string): i64
    function toU64(decimal: string): u64
    function toF64(decimal: string): f64
    function toBigInt(decimal: string): BigInt
}

export function testToI64(decimal: string): i64 {
    return json.toI64(decimal);
}

export function testToU64(decimal: string): u64 {
    return json.toU64(decimal);
}

export function testToF64(decimal: string): f64 {
    return json.toF64(decimal)
}

export function testToBigInt(decimal: string): BigInt {
    return json.toBigInt(decimal)
}
