import "allocator/arena";

export { allocate_memory, free_memory };

// Sequence of 20 `u8`s.
type Address = ArrayBuffer;

const array_buffer_header_size = 8;

// Clone the address to a new buffer, add 1 to the first and last bytes of the
// address and return the new address.
export function test_address(address: Address): Address {
  let new_address = address.slice();

  // Add 1 to the first and last bytes.
  let first_byte_ptr = changetype<usize>(new_address) + array_buffer_header_size;
  let first_byte = load<u8>(first_byte_ptr);
  store<u8>(first_byte_ptr, first_byte + 1);

  // An address has 20 bytes.
  let last_byte_ptr = first_byte_ptr + 19;
  let last_byte = load<u8>(last_byte_ptr);
  store<u8>(last_byte_ptr, last_byte + 1);
  return new_address
}

// Sequence of 4 `u64`s.
type Uint = ArrayBuffer;

// Clone the Uint to a new buffer, add 1 to the first and last `u64`s and return
// the new Uint.
export function test_uint(address: Uint): ArrayBuffer {
  let new_uint = address.slice();

  // Add 1 to the first and last `u64`s.
  let first_u64_ptr = changetype<usize>(new_uint) + array_buffer_header_size;
  let first_u64 = load<u64>(first_u64_ptr);
  store<u8>(first_u64_ptr, first_u64 + 1);

  // An address has 4 u64s.
  let last_u64_ptr = first_u64_ptr + 3*sizeof<u64>();
  let last_u64 = load<u64>(last_u64_ptr);
  store<u8>(last_u64_ptr, last_u64 + 1);
  return new_uint
}

// Return the string repeated twice.
export function repeat_twice(original: string): string {
  return original.repeat(2)
}

// Sequences of `u8`s.
type FixedBytes = ArrayBuffer;
type Bytes = ArrayBuffer;

// Concatenate two byte sequences into a new one.
export function concat(bytes1: Bytes, bytes2: FixedBytes): Bytes {
  let concated = new ArrayBuffer(bytes1.byteLength + bytes2.byteLength);
  let concated_offset = changetype<usize>(concated) + array_buffer_header_size;
  let bytes1_start = changetype<usize>(bytes1) + array_buffer_header_size;
  let bytes2_start = changetype<usize>(bytes2) + array_buffer_header_size;

  // Move bytes1.
  move_memory(concated_offset, bytes1_start, bytes1.byteLength);
  concated_offset += bytes1.byteLength

  // Move bytes2.
  move_memory(concated_offset, bytes2_start, bytes2.byteLength);
  return concated;
}
