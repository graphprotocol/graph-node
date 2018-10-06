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
    function u256ToI64(x: Uint64Array): i64
    function i64ToU256(x: i64): Uint64Array
}

export function token_from_i64(int: i64): Token {
    let token: Token;
    token.kind = TokenKind.INT;
    token.data = typeConversion.i64ToU256(int) as u64;
    return token
}

export function token_to_i64(token: Token): i64 {
    assert(token.kind == TokenKind.INT, "Token is not an int.")
    return typeConversion.u256ToI64(changetype<Uint64Array>(token.data as u32))
}
