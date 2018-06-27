import "allocator/arena";

export { allocate_memory, free_memory };

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
