import "allocator/arena";

export { allocate_memory };

// Sequence of 20 `u8`s.
type Address = Uint8Array;

const array_buffer_header_size = 8;

// Clone the address to a new buffer, add 1 to the first and last bytes of the
// address and return the new address.
export function test_address(address: Address): Address {
  let new_address = address.subarray();

  // Add 1 to the first and last bytes.
  new_address[0] += 1;
  new_address[address.length - 1] += 1;

  return new_address
}

// Sequence of 4 `u64`s.
type Uint = Uint64Array;

// Clone the Uint to a new buffer, add 1 to the first and last `u64`s and return
// the new Uint.
export function test_uint(address: Uint): Uint {
  let new_address = address.subarray();

  // Add 1 to the first and last bytes.
  new_address[0] += 1;
  new_address[address.length - 1] += 1;

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
  let concated = new ArrayBuffer(bytes1.byteLength + bytes2.byteLength);
  let concated_offset = changetype<usize>(concated) + array_buffer_header_size;
  let bytes1_start = load<usize>(changetype<usize>(bytes1)) + array_buffer_header_size;
  let bytes2_start = load<usize>(changetype<usize>(bytes2)) + array_buffer_header_size;

  // Move bytes1.
  move_memory(concated_offset, bytes1_start, bytes1.byteLength);
  concated_offset += bytes1.byteLength

  // Move bytes2.
  move_memory(concated_offset, bytes2_start, bytes2.byteLength);

  let new_typed_array = new Uint8Array(concated.byteLength);
  store<usize>(changetype<usize>(new_typed_array), changetype<usize>(concated));

  return new_typed_array;
}
