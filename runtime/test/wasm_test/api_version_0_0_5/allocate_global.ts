// TODO: move to import file
__alloc(0);

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
let globalOne = bigInt.fromString("1")

type BigInt = Uint8Array;

declare namespace bigInt {
    function fromString(s: string): BigInt
}


export function assert_global_works(): void {
  let localOne = bigInt.fromString("1")
  assert(globalOne != localOne)
}
