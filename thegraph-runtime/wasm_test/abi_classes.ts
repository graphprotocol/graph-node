import "allocator/arena";

export { allocate_memory, free_memory };

type Address = ArrayBuffer;

// Clone the address to a new buffer, add 1 to the first and last bytes of the
// address and return the new address.
export function test_address(address: Address): Address {
  let new_address = address.slice();
  
  // ArrayBuffer header size.
  let header_size = 8;

  // Add 1 to the first and last bytes.
  let first_byte_ptr = changetype<usize>(new_address) + header_size;
  let first_byte = load<u8>(first_byte_ptr);
  store<u8>(first_byte_ptr, first_byte + 1);

  // An address has 20 bytes.
  let last_byte_ptr = first_byte_ptr + 19;
  let last_byte = load<u8>(last_byte_ptr);
  store<u8>(last_byte_ptr, last_byte + 1);
  return new_address
}

// Return the string repeated twice.
export function repeat_twice(original: string): string {
  return original.repeat(2)
}
