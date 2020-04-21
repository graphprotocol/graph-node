use ethabi;
use std::collections::HashMap;

use graph::components::ethereum::{
    EthereumBlockData, EthereumCallData, EthereumEventData, EthereumTransactionData,
};
use graph::data::store;
use graph::prelude::serde_json;
use graph::prelude::web3::types as web3;
use graph::prelude::{BigDecimal, BigInt};

use crate::asc_abi::class::*;
use crate::asc_abi::{AscHeap, AscPtr, AscType, FromAscObj, ToAscObj};

impl ToAscObj<Uint8Array> for web3::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Uint8Array {
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

impl ToAscObj<Uint8Array> for web3::H256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Uint8Array {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<AscBigInt> for web3::U128 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscBigInt {
        let mut bytes: [u8; 16] = [0; 16];
        self.to_little_endian(&mut bytes);
        bytes.to_asc_obj(heap)
    }
}

impl ToAscObj<AscBigInt> for BigInt {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscBigInt {
        let bytes = self.to_signed_bytes_le();
        bytes.to_asc_obj(heap)
    }
}

impl FromAscObj<AscBigInt> for BigInt {
    fn from_asc_obj<H: AscHeap>(array_buffer: AscBigInt, heap: &H) -> Self {
        let bytes = <Vec<u8>>::from_asc_obj(array_buffer, heap);
        BigInt::from_signed_bytes_le(&bytes)
    }
}

impl ToAscObj<AscBigDecimal> for BigDecimal {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscBigDecimal {
        // From the docs: "Note that a positive exponent indicates a negative power of 10",
        // so "exponent" is the opposite of what you'd expect.
        let (digits, negative_exp) = self.as_bigint_and_exponent();
        AscBigDecimal {
            exp: heap.asc_new(&BigInt::from(-negative_exp)),
            digits: heap.asc_new(&BigInt::from(digits)),
        }
    }
}

impl FromAscObj<AscBigDecimal> for BigDecimal {
    fn from_asc_obj<H: AscHeap>(big_decimal: AscBigDecimal, heap: &H) -> Self {
        heap.asc_get::<BigInt, _>(big_decimal.digits)
            .to_big_decimal(heap.asc_get(big_decimal.exp))
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
            StoreValueKind::Int => Value::Int(i32::from(payload)),
            StoreValueKind::BigDecimal => {
                let ptr: AscPtr<AscBigDecimal> = AscPtr::from(payload);
                Value::BigDecimal(heap.asc_get(ptr))
            }
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
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let array: Vec<u8> = heap.asc_get(ptr);
                Value::BigInt(store::scalar::BigInt::from_signed_bytes_le(&array))
            }
        }
    }
}

impl ToAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEnum<StoreValueKind> {
        use self::store::Value;

        let payload = match self {
            Value::String(string) => heap.asc_new(string.as_str()).into(),
            Value::Int(n) => EnumPayload::from(*n),
            Value::BigDecimal(n) => heap.asc_new(n).into(),
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
            _padding: 0,
            payload,
        }
    }
}

impl ToAscObj<AscJson> for serde_json::Map<String, serde_json::Value> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscJson {
        AscTypedMap {
            entries: heap.asc_new(&*self.iter().collect::<Vec<_>>()),
        }
    }
}

impl ToAscObj<AscEntity> for HashMap<String, store::Value> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEntity {
        AscTypedMap {
            entries: heap.asc_new(&*self.iter().collect::<Vec<_>>()),
        }
    }
}

impl ToAscObj<AscEntity> for store::Entity {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEntity {
        AscTypedMap {
            entries: heap.asc_new(&*self.iter().collect::<Vec<_>>()),
        }
    }
}

impl ToAscObj<AscEnum<JsonValueKind>> for serde_json::Value {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEnum<JsonValueKind> {
        use serde_json::Value;

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
            _padding: 0,
            payload,
        }
    }
}

impl From<i32> for LogLevel {
    fn from(i: i32) -> Self {
        match i {
            0 => LogLevel::Critical,
            1 => LogLevel::Error,
            2 => LogLevel::Warning,
            3 => LogLevel::Info,
            4 => LogLevel::Debug,
            _ => LogLevel::Debug,
        }
    }
}

impl ToAscObj<bool> for bool {
    fn to_asc_obj<H: AscHeap>(&self, _heap: &mut H) -> bool {
        *self
    }
}

impl<T: AscType> ToAscObj<AscWrapped<T>> for AscWrapped<T> {
    fn to_asc_obj<H: AscHeap>(&self, _heap: &mut H) -> AscWrapped<T> {
        *self
    }
}

impl<V, E, VAsc, EAsc> ToAscObj<AscResult<VAsc, EAsc>> for Result<V, E>
where
    V: ToAscObj<VAsc>,
    E: ToAscObj<EAsc>,
    VAsc: AscType,
    EAsc: AscType,
{
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscResult<VAsc, EAsc> {
        match self {
            Ok(value) => AscResult {
                value: {
                    let inner = heap.asc_new(value);
                    let wrapped = AscWrapped { inner };
                    heap.asc_new(&wrapped)
                },
                error: AscPtr::null(),
            },
            Err(e) => AscResult {
                value: AscPtr::null(),
                error: {
                    let inner = heap.asc_new(e);
                    let wrapped = AscWrapped { inner };
                    heap.asc_new(&wrapped)
                },
            },
        }
    }
}
