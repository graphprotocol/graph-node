import "allocator/arena";

export { allocate_memory };

enum TokenKind {
    ADDRESS,
    FIXED_BYTES,
    BYTES,
    INT,
    UINT,
    BOOL,
    STRING,
    FIXED_ARRAY,
    ARRAY
}

type Payload = u64

export class Token {
    kind: TokenKind
    data: Payload
}

declare namespace typeConversion {
    function i32ToBigInt(x: i32): Uint8Array
    function bigIntToI32(x: Uint8Array): i32
}

export function token_from_i32(int: i32): Token {
    let token: Token;
    token.kind = TokenKind.INT;
    token.data = typeConversion.i32ToBigInt(int) as u64;
    return token
}

export function token_to_i32(token: Token): i32 {
    assert(token.kind == TokenKind.INT, "Token is not an int.")
    return typeConversion.bigIntToI32(changetype<Uint8Array>(token.data as u32))
}
