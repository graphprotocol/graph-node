enum IndexForAscTypeId {
  STRING = 0,
  ARRAY_BUFFER = 1,
  UINT8_ARRAY = 6,
  ARRAY_ETHEREUM_VALUE = 15,
  ETHEREUM_VALUE = 30,
}

export function id_of_type(type_id_index: IndexForAscTypeId): usize {
  switch (type_id_index) {
    case IndexForAscTypeId.STRING:
      return idof<string>();
    case IndexForAscTypeId.ARRAY_BUFFER:
      return idof<ArrayBuffer>();
    case IndexForAscTypeId.UINT8_ARRAY:
      return idof<Uint8Array>();
    case IndexForAscTypeId.ARRAY_ETHEREUM_VALUE:
      return idof<Array<Token>>();
    case IndexForAscTypeId.ETHEREUM_VALUE:
      return idof<Token>();
    default:
      return 0;
  }
}

export function allocate(n: usize): usize {
  return __alloc(n);
}

// Sequence of 20 `u8`s.
type Address = Uint8Array;

// Sequences of `u8`s.
type Bytes = Uint8Array;

// Sequence of 4 `u64`s.
type Int = Uint64Array;
type Uint = Uint64Array;

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

// Big enough to fit any pointer or native this.data.
type Payload = u64

export class Token {
    kind: TokenKind
    data: Payload
}

export function token_to_address(token: Token): Address {
    assert(token.kind == TokenKind.ADDRESS, "Token is not an address.");
    return changetype<Address>(token.data as u32);
}

export function token_to_bytes(token: Token): Bytes {
    assert(token.kind == TokenKind.FIXED_BYTES 
            || token.kind == TokenKind.BYTES, "Token is not bytes.")
    return changetype<Bytes>(token.data as u32)
}
  
export function token_to_int(token: Token): Int {
    assert(token.kind == TokenKind.INT
            || token.kind == TokenKind.UINT, "Token is not an int or uint.")
    return changetype<Int>(token.data as u32)
}

export function token_to_uint(token: Token): Uint {
    assert(token.kind == TokenKind.INT
            || token.kind == TokenKind.UINT, "Token is not an int or uint.")
    return changetype<Uint>(token.data as u32)
}

export function token_to_bool(token: Token): boolean {
    assert(token.kind == TokenKind.BOOL, "Token is not a boolean.")
    return token.data != 0
}
  
export function token_to_string(token: Token): string {
    assert(token.kind == TokenKind.STRING, "Token is not a string.")
    return changetype<string>(token.data as u32)
}
  
export function token_to_array(token: Token): Array<Token> {
    assert(token.kind == TokenKind.FIXED_ARRAY ||
        token.kind == TokenKind.ARRAY, "Token is not an array.")
    return changetype<Array<Token>>(token.data as u32)
}


export function token_from_address(address: Address): Token {  
    let token = new Token();
    token.kind = TokenKind.ADDRESS;
    token.data = changetype<u32>(address);
    return token
}

export function token_from_bytes(bytes: Bytes): Token {
    let token = new Token();
    token.kind = TokenKind.BYTES;
    token.data = changetype<u32>(bytes);
    return token
}
  
export function token_from_int(int: Int): Token {
    let token = new Token();
    token.kind = TokenKind.INT;
    token.data = changetype<u32>(int);
    return token
}

export function token_from_uint(uint: Uint): Token {
    let token = new Token();
    token.kind = TokenKind.UINT;
    token.data = changetype<u32>(uint);
    return token
}

export function token_from_bool(bool: boolean): Token {
    let token = new Token();
    token.kind = TokenKind.BOOL;
    token.data = bool as u32;
    return token
}
  
export function token_from_string(str: string): Token {
    let token = new Token();
    token.kind = TokenKind.STRING;
    token.data = changetype<u32>(str);
    return token
}
  
export function token_from_array(array: Token): Token {
    let token = new Token();
    token.kind = TokenKind.ARRAY;
    token.data = changetype<u32>(array);
    return token
}
