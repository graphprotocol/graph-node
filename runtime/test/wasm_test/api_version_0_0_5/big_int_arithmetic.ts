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

declare namespace bigInt {
    function plus(x: BigInt, y: BigInt): BigInt
    function minus(x: BigInt, y: BigInt): BigInt
    function times(x: BigInt, y: BigInt): BigInt
    function dividedBy(x: BigInt, y: BigInt): BigInt
    function mod(x: BigInt, y: BigInt): BigInt
}

export function plus(x: BigInt, y: BigInt): BigInt {
    return bigInt.plus(x, y)
}

export function minus(x: BigInt, y: BigInt): BigInt {
    return bigInt.minus(x, y)
}

export function times(x: BigInt, y: BigInt): BigInt {
    return bigInt.times(x, y)
}

export function dividedBy(x: BigInt, y: BigInt): BigInt {
    return bigInt.dividedBy(x, y)
}

export function mod(x: BigInt, y: BigInt): BigInt {
    return bigInt.mod(x, y)
}
