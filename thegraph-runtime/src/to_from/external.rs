use ethabi;
use ethereum_types;

use thegraph::components::ethereum::EthereumEvent;
use thegraph::data::store;

use asc_abi::class::*;
use asc_abi::{AscHeap, AscPtr, FromAscObj, ToAscObj};

impl ToAscObj<ArrayBuffer<u8>> for ethereum_types::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u8> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<ArrayBuffer<u8>> for ethereum_types::H160 {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<u8>, heap: &H) -> Self {
        ethereum_types::H160(<[u8; 20]>::from_asc_obj(array_buffer, heap))
    }
}

impl ToAscObj<Uint8Array> for ethereum_types::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Uint8Array {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<Uint8Array> for ethereum_types::H160 {
    fn from_asc_obj<H: AscHeap>(typed_array: Uint8Array, heap: &H) -> Self {
        ethereum_types::H160(<[u8; 20]>::from_asc_obj(typed_array, heap))
    }
}

impl FromAscObj<Uint8Array> for ethereum_types::H256 {
    fn from_asc_obj<H: AscHeap>(typed_array: Uint8Array, heap: &H) -> Self {
        ethereum_types::H256(<[u8; 32]>::from_asc_obj(typed_array, heap))
    }
}

impl ToAscObj<ArrayBuffer<u8>> for ethereum_types::H256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u8> {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<Uint8Array> for ethereum_types::H256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Uint8Array {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<ArrayBuffer<u64>> for ethereum_types::U256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u64> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<ArrayBuffer<u64>> for ethereum_types::U256 {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<u64>, heap: &H) -> Self {
        ethereum_types::U256(<[u64; 4]>::from_asc_obj(array_buffer, heap))
    }
}

impl ToAscObj<Uint64Array> for ethereum_types::U256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Uint64Array {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<Uint64Array> for ethereum_types::U256 {
    fn from_asc_obj<H: AscHeap>(array_buffer: Uint64Array, heap: &H) -> Self {
        ethereum_types::U256(<[u64; 4]>::from_asc_obj(array_buffer, heap))
    }
}

impl ToAscObj<AscEnum<TokenKind>> for ethabi::Token {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEnum<TokenKind> {
        use ethabi::Token::*;

        let kind = TokenKind::get_kind(self);
        let payload = match self {
            Address(address) => heap.asc_new::<AscAddress, _>(address).to_payload(),
            FixedBytes(bytes) | Bytes(bytes) => {
                heap.asc_new::<Uint8Array, _>(&**bytes).to_payload()
            }
            Int(uint) | Uint(uint) => heap.asc_new::<AscU256, _>(uint).to_payload(),
            Bool(b) => *b as u64,
            String(string) => heap.asc_new(&**string).to_payload(),
            FixedArray(tokens) | Array(tokens) => heap.asc_new(&**tokens).to_payload(),
        };

        AscEnum { kind, payload }
    }
}

impl FromAscObj<AscEnum<TokenKind>> for ethabi::Token {
    fn from_asc_obj<H: AscHeap>(asc_enum: AscEnum<TokenKind>, heap: &H) -> Self {
        use ethabi::Token;

        let payload = asc_enum.payload;
        match asc_enum.kind {
            TokenKind::Bool => Token::Bool(payload != 0),
            TokenKind::Address => {
                let ptr: AscPtr<AscAddress> = AscPtr::from_payload(payload);
                Token::Address(heap.asc_get(ptr))
            }
            TokenKind::FixedBytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from_payload(payload);
                Token::FixedBytes(heap.asc_get(ptr))
            }
            TokenKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from_payload(payload);
                Token::Bytes(heap.asc_get(ptr))
            }
            TokenKind::Int => {
                let ptr: AscPtr<AscU256> = AscPtr::from_payload(payload);
                Token::Int(heap.asc_get(ptr))
            }
            TokenKind::Uint => {
                let ptr: AscPtr<AscU256> = AscPtr::from_payload(payload);
                Token::Int(heap.asc_get(ptr))
            }
            TokenKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from_payload(payload);
                Token::String(heap.asc_get(ptr))
            }
            TokenKind::FixedArray => {
                let ptr: AscEnumArray<TokenKind> = AscPtr::from_payload(payload);
                Token::FixedArray(heap.asc_get(ptr))
            }
            TokenKind::Array => {
                let ptr: AscEnumArray<TokenKind> = AscPtr::from_payload(payload);
                Token::Array(heap.asc_get(ptr))
            }
        }
    }
}

impl FromAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn from_asc_obj<H: AscHeap>(asc_enum: AscEnum<StoreValueKind>, heap: &H) -> Self {
        use self::store::Value;

        let payload = asc_enum.payload;
        match asc_enum.kind {
            StoreValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from_payload(payload);
                Value::String(heap.asc_get(ptr))
            }
            // This is just `i32::from_bytes` which is unstable.
            StoreValueKind::Int => {
                let int: i32 = unsafe { ::std::mem::transmute::<u32, i32>(payload as u32) };
                Value::Int(int)
            }
            StoreValueKind::Float => Value::Float(f64::from_bits(payload as u64) as f32),
            StoreValueKind::Bool => Value::Bool(payload != 0),
            StoreValueKind::Array => {
                let ptr: AscEnumArray<StoreValueKind> = AscPtr::from_payload(payload);
                Value::List(heap.asc_get(ptr))
            }
            StoreValueKind::Null => Value::Null,
        }
    }
}

impl ToAscObj<AscLogParam> for ethabi::LogParam {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscLogParam {
        AscLogParam {
            name: heap.asc_new(self.name.as_str()),
            value: heap.asc_new(&self.value),
        }
    }
}

impl ToAscObj<AscEthereumEvent> for EthereumEvent {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEthereumEvent {
        AscEthereumEvent {
            address: heap.asc_new(&self.address),
            event_signature: heap.asc_new(&self.event_signature),
            block_hash: heap.asc_new(&self.block_hash),
            params: heap.asc_new(self.params.as_slice()),
        }
    }
}
