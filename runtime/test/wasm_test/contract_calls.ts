enum IndexForAscTypeId {
  STRING = 0,
  ARRAY_BUFFER = 1,
  UINT8_ARRAY = 6,
  ARRAY_UINT8_ARRAY = 14,
}

export function id_of_type(type_id_index: IndexForAscTypeId): usize {
  switch (type_id_index) {
    case IndexForAscTypeId.STRING:
      return idof<string>();
    case IndexForAscTypeId.ARRAY_BUFFER:
      return idof<ArrayBuffer>();
    case IndexForAscTypeId.UINT8_ARRAY:
      return idof<Uint8Array>();
    case IndexForAscTypeId.ARRAY_UINT8_ARRAY:
      return idof<Array<Uint8Array>>();
    default:
      return 0;
  }
}

export function allocate(n: usize): usize {
  return __alloc(n);
}

type Address = Uint8Array;

export declare namespace ethereum {
  function call(call: Address): Array<Address> | null
}

export function callContract(address: Address): void {
  ethereum.call(address)
}
