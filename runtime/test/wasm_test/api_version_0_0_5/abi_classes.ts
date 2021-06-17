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

// Sequence of 20 `u8`s.
type Address = Uint8Array;

// Clone the address to a new buffer, add 1 to the first and last bytes of the
// address and return the new address.
export function test_address(address: Address): Address {
  let new_address = address.subarray();

  // Add 1 to the first and last byte.
  new_address[0] += 1;
  new_address[address.length - 1] += 1;

  return new_address
}

// Sequence of 32 `u8`s.
type Uint = Uint8Array;

// Clone the Uint to a new buffer, add 1 to the first and last `u8`s and return
// the new Uint.
export function test_uint(address: Uint): Uint {
  let new_address = address.subarray();

  // Add 1 to the first byte.
  new_address[0] += 1;

  return new_address
}

// Return the string repeated twice.
export function repeat_twice(original: string): string {
  return original.repeat(2)
}

// Sequences of `u8`s.
type FixedBytes = Uint8Array;
type Bytes = Uint8Array;

// Concatenate two byte sequences into a new one.
export function concat(bytes1: Bytes, bytes2: FixedBytes): Bytes {
  let concated_buff = new ArrayBuffer(bytes1.byteLength + bytes2.byteLength);
  let concated_buff_ptr = changetype<usize>(concated_buff);

  let bytes1_ptr = changetype<usize>(bytes1);
  let bytes1_buff_ptr = load<usize>(bytes1_ptr);

  let bytes2_ptr = changetype<usize>(bytes2);
  let bytes2_buff_ptr = load<usize>(bytes2_ptr);

  // Move bytes1.
  memory.copy(concated_buff_ptr, bytes1_buff_ptr, bytes1.byteLength);
  concated_buff_ptr += bytes1.byteLength

  // Move bytes2.
  memory.copy(concated_buff_ptr, bytes2_buff_ptr, bytes2.byteLength);

  let new_typed_array = Uint8Array.wrap(concated_buff);

  return new_typed_array;
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
export class Value {
    kind: ValueKind
    data: Payload
}

export function test_array(strings: Array<string>): Array<string> {
  strings.push("5")
  return strings
}

export function byte_array_third_quarter(bytes: Uint8Array): Uint8Array {
  return bytes.subarray(bytes.length * 2/4, bytes.length * 3/4)
}
