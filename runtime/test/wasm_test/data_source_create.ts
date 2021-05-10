enum IndexForAscTypeId {
  STRING = 0,
  ARRAY_BUFFER = 1,
  UINT8_ARRAY = 6,
  ARRAY_STRING = 18,
}

export function id_of_type(type_id_index: IndexForAscTypeId): usize {
  switch (type_id_index) {
    case IndexForAscTypeId.STRING:
      return idof<string>();
    case IndexForAscTypeId.ARRAY_BUFFER:
      return idof<ArrayBuffer>();
    case IndexForAscTypeId.UINT8_ARRAY:
      return idof<Uint8Array>();
    case IndexForAscTypeId.ARRAY_STRING:
      return idof<Array<string>>();
    default:
      return 0;
  }
}

export function allocate(n: usize): usize {
  return __alloc(n);
}

declare namespace dataSource {
    function create(name: string, params: Array<string>): void
}

export function dataSourceCreate(name: string, params: Array<string>): void {
    dataSource.create(name, params)
}
