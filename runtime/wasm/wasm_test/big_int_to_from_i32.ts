import "allocator/arena";

export { memory };

declare namespace typeConversion {
    function i32ToBigInt(i: i32): Uint8Array
    function bigIntToI32(n: Uint8Array): i32
}

export function big_int_to_i32(n: Uint8Array): i32 {
    return typeConversion.bigIntToI32(n)
}

export function i32_to_big_int(i: i32): Uint8Array {
    return typeConversion.i32ToBigInt(i)
}
