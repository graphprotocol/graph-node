import "allocator/arena";

export { memory };

enum TokenKind {
    ADDRESS = 0,
    FIXED_BYTES = 1,
    BYTES = 2,
    INT = 3,
    UINT = 4,
    BOOL = 5,
    STRING = 6,
    FIXED_ARRAY = 7,
    ARRAY = 8
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
