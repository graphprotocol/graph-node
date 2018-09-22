use ethabi;
use graph::serde_json;

use graph::components::ethereum::{EthereumBlockData, EthereumEventData, EthereumTransactionData};
use graph::data::store;
use graph::web3::types as web3;

use asc_abi::class::*;
use asc_abi::{AscHeap, AscPtr, FromAscObj, ToAscObj};
use std::collections::HashMap;

use UnresolvedContractCall;

impl ToAscObj<ArrayBuffer<u8>> for web3::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u8> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<ArrayBuffer<u8>> for web3::H160 {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<u8>, heap: &H) -> Self {
        web3::H160(<[u8; 20]>::from_asc_obj(array_buffer, heap))
    }
}

impl ToAscObj<Uint8Array> for web3::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Uint8Array {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<Uint8Array> for web3::H160 {
    fn from_asc_obj<H: AscHeap>(typed_array: Uint8Array, heap: &H) -> Self {
        web3::H160(<[u8; 20]>::from_asc_obj(typed_array, heap))
    }
}

impl FromAscObj<Uint8Array> for web3::H256 {
    fn from_asc_obj<H: AscHeap>(typed_array: Uint8Array, heap: &H) -> Self {
        web3::H256(<[u8; 32]>::from_asc_obj(typed_array, heap))
    }
}

impl ToAscObj<ArrayBuffer<u8>> for web3::H256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u8> {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<Uint8Array> for web3::H256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Uint8Array {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<ArrayBuffer<u64>> for web3::U128 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u64> {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<ArrayBuffer<u64>> for web3::U256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<u64> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<ArrayBuffer<u64>> for web3::U256 {
    fn from_asc_obj<H: AscHeap>(array_buffer: ArrayBuffer<u64>, heap: &H) -> Self {
        web3::U256(<[u64; 4]>::from_asc_obj(array_buffer, heap))
    }
}

impl ToAscObj<Uint64Array> for web3::U128 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Uint64Array {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<Uint64Array> for web3::U256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> Uint64Array {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<Uint64Array> for web3::U256 {
    fn from_asc_obj<H: AscHeap>(array_buffer: Uint64Array, heap: &H) -> Self {
        web3::U256(<[u64; 4]>::from_asc_obj(array_buffer, heap))
    }
}

impl ToAscObj<AscEnum<EthereumValueKind>> for ethabi::Token {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEnum<EthereumValueKind> {
        use ethabi::Token::*;

        let kind = EthereumValueKind::get_kind(self);
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

        AscEnum {
            kind,
            payload: EnumPayload(payload),
        }
    }
}

impl FromAscObj<AscEnum<EthereumValueKind>> for ethabi::Token {
    fn from_asc_obj<H: AscHeap>(asc_enum: AscEnum<EthereumValueKind>, heap: &H) -> Self {
        use ethabi::Token;

        let payload = asc_enum.payload;
        match asc_enum.kind {
            EthereumValueKind::Bool => Token::Bool(bool::from(payload)),
            EthereumValueKind::Address => {
                let ptr: AscPtr<AscAddress> = AscPtr::from(payload);
                Token::Address(heap.asc_get(ptr))
            }
            EthereumValueKind::FixedBytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::FixedBytes(heap.asc_get(ptr))
            }
            EthereumValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::Bytes(heap.asc_get(ptr))
            }
            EthereumValueKind::Int => {
                let ptr: AscPtr<AscU256> = AscPtr::from(payload);
                Token::Int(heap.asc_get(ptr))
            }
            EthereumValueKind::Uint => {
                let ptr: AscPtr<AscU256> = AscPtr::from(payload);
                Token::Uint(heap.asc_get(ptr))
            }
            EthereumValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Token::String(heap.asc_get(ptr))
            }
            EthereumValueKind::FixedArray => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::FixedArray(heap.asc_get(ptr))
            }
            EthereumValueKind::Array => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
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
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Value::String(heap.asc_get(ptr))
            }
            // This is just `i32::from_bytes` which is unstable.
            StoreValueKind::Int => Value::Int(i32::from(payload)),
            StoreValueKind::Float => Value::Float(f32::from(payload)),
            StoreValueKind::Bool => Value::Bool(bool::from(payload)),
            StoreValueKind::Array => {
                let ptr: AscEnumArray<StoreValueKind> = AscPtr::from(payload);
                Value::List(heap.asc_get(ptr))
            }
            StoreValueKind::Null => Value::Null,
            StoreValueKind::Bytes => {
                let ptr: AscPtr<Bytes> = AscPtr::from(payload);
                let array: Vec<u8> = heap.asc_get(ptr);
                Value::Bytes(array.as_slice().into())
            }
            StoreValueKind::BigInt => {
                let ptr: AscPtr<BigInt> = AscPtr::from(payload);
                let array: Vec<u8> = heap.asc_get(ptr);
                Value::BigInt(store::scalar::BigInt::from_signed_bytes_le(&array))
            }
        }
    }
}

impl ToAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEnum<StoreValueKind> {
        use self::store::Value;

        let payload = match self {
            Value::String(string) => heap.asc_new(string.as_str()).into(),
            Value::Int(n) => EnumPayload::from(*n),
            Value::Float(n) => EnumPayload::from(*n),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::List(array) => heap.asc_new(array.as_slice()).into(),
            Value::Null => EnumPayload(0),
            Value::Bytes(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = heap.asc_new(bytes.as_slice());
                bytes_obj.into()
            }
            Value::BigInt(big_int) => {
                let bytes_obj: AscPtr<Uint8Array> = heap.asc_new(&*big_int.to_signed_bytes_le());
                bytes_obj.into()
            }
        };

        AscEnum {
            kind: StoreValueKind::get_kind(self),
            payload,
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

impl ToAscObj<AscJson> for serde_json::Map<String, serde_json::Value> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscJson {
        AscTypedMap {
            entries: heap.asc_new(&*self.iter().collect::<Vec<_>>()),
        }
    }
}

impl ToAscObj<AscEntity> for HashMap<String, store::Value> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEntity {
        AscTypedMap {
            entries: heap.asc_new(&*self.iter().collect::<Vec<_>>()),
        }
    }
}

impl ToAscObj<AscEntity> for store::Entity {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEntity {
        AscTypedMap {
            entries: heap.asc_new(&*self.iter().collect::<Vec<_>>()),
        }
    }
}

impl ToAscObj<AscEnum<JsonValueKind>> for serde_json::Value {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEnum<JsonValueKind> {
        use graph::serde_json::Value;

        let payload = match self {
            Value::Null => EnumPayload(0),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::Number(number) => heap.asc_new(&*number.to_string()).into(),
            Value::String(string) => heap.asc_new(string.as_str()).into(),
            Value::Array(array) => heap.asc_new(array.as_slice()).into(),
            Value::Object(object) => heap.asc_new(object).into(),
        };

        AscEnum {
            kind: JsonValueKind::get_kind(self),
            payload,
        }
    }
}

impl ToAscObj<AscEthereumBlock> for EthereumBlockData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEthereumBlock {
        AscEthereumBlock {
            hash: heap.asc_new(&self.hash),
            parent_hash: heap.asc_new(&self.parent_hash),
            uncles_hash: heap.asc_new(&self.uncles_hash),
            author: heap.asc_new(&self.author),
            state_root: heap.asc_new(&self.state_root),
            transactions_root: heap.asc_new(&self.transactions_root),
            receipts_root: heap.asc_new(&self.receipts_root),
            number: heap.asc_new::<AscU128, _>(&self.number),
            gas_used: heap.asc_new(&self.gas_used),
            gas_limit: heap.asc_new(&self.gas_limit),
            timestamp: heap.asc_new(&self.timestamp),
            difficulty: heap.asc_new(&self.difficulty),
            total_difficulty: heap.asc_new(&self.total_difficulty),
        }
    }
}

impl ToAscObj<AscEthereumTransaction> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEthereumTransaction {
        AscEthereumTransaction {
            hash: heap.asc_new(&self.hash),
            block_hash: heap.asc_new(&self.block_hash),
            block_number: heap.asc_new(&self.block_number),
            gas_used: heap.asc_new(&self.gas_used),
        }
    }
}

impl ToAscObj<AscEthereumEvent> for EthereumEventData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &H) -> AscEthereumEvent {
        AscEthereumEvent {
            address: heap.asc_new(&self.address),
            block: heap.asc_new(&self.block),
            transaction: heap.asc_new(&self.transaction),
            params: heap.asc_new(self.params.as_slice()),
        }
    }
}

impl FromAscObj<AscUnresolvedContractCall> for UnresolvedContractCall {
    fn from_asc_obj<H: AscHeap>(asc_call: AscUnresolvedContractCall, heap: &H) -> Self {
        UnresolvedContractCall {
            contract_name: heap.asc_get(asc_call.contract_name),
            contract_address: heap.asc_get(asc_call.contract_address),
            function_name: heap.asc_get(asc_call.function_name),
            function_args: heap.asc_get(asc_call.function_args),
        }
    }
}
