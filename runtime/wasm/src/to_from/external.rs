use ethabi;

use graph::runtime::{asc_get, asc_new, try_asc_get, AscPtr, AscType, AscValue, ToAscObj};
use graph::{data::store, runtime::DeterministicHostError};
use graph::{prelude::serde_json, runtime::FromAscObj};
use graph::{prelude::web3::types as web3, runtime::AscHeap};
use graph::{
    prelude::{BigDecimal, BigInt},
    runtime::TryFromAscObj,
};

use crate::asc_abi::class::*;

impl ToAscObj<Uint8Array> for web3::H160 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<Uint8Array, DeterministicHostError> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<Uint8Array> for web3::H160 {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: Uint8Array,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 20]>::from_asc_obj(typed_array, heap)?;
        Ok(Self(data))
    }
}

impl FromAscObj<Uint8Array> for web3::H256 {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: Uint8Array,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 32]>::from_asc_obj(typed_array, heap)?;
        Ok(Self(data))
    }
}

impl ToAscObj<Uint8Array> for web3::H256 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<Uint8Array, DeterministicHostError> {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<AscBigInt> for web3::U128 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBigInt, DeterministicHostError> {
        let mut bytes: [u8; 16] = [0; 16];
        self.to_little_endian(&mut bytes);
        bytes.to_asc_obj(heap)
    }
}

impl ToAscObj<AscBigInt> for BigInt {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBigInt, DeterministicHostError> {
        let bytes = self.to_signed_bytes_le();
        bytes.to_asc_obj(heap)
    }
}

impl FromAscObj<AscBigInt> for BigInt {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        array_buffer: AscBigInt,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let bytes = <Vec<u8>>::from_asc_obj(array_buffer, heap)?;
        Ok(BigInt::from_signed_bytes_le(&bytes))
    }
}

impl ToAscObj<AscBigDecimal> for BigDecimal {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBigDecimal, DeterministicHostError> {
        // From the docs: "Note that a positive exponent indicates a negative power of 10",
        // so "exponent" is the opposite of what you'd expect.
        let (digits, negative_exp) = self.as_bigint_and_exponent();
        Ok(AscBigDecimal {
            exp: asc_new(heap, &BigInt::from(-negative_exp))?,
            digits: asc_new(heap, &BigInt::from(digits))?,
        })
    }
}

impl TryFromAscObj<AscBigDecimal> for BigDecimal {
    fn try_from_asc_obj<H: AscHeap + ?Sized>(
        big_decimal: AscBigDecimal,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let digits: BigInt = asc_get(heap, big_decimal.digits)?;
        let exp: BigInt = asc_get(heap, big_decimal.exp)?;

        let bytes = exp.to_signed_bytes_le();
        let mut byte_array = if exp >= 0.into() { [0; 8] } else { [255; 8] };
        byte_array[..bytes.len()].copy_from_slice(&bytes);
        let big_decimal = BigDecimal::new(digits, i64::from_le_bytes(byte_array));

        // Validate the exponent.
        let exp = -big_decimal.as_bigint_and_exponent().1;
        let min_exp: i64 = BigDecimal::MIN_EXP.into();
        let max_exp: i64 = BigDecimal::MAX_EXP.into();
        if exp < min_exp || max_exp < exp {
            Err(DeterministicHostError(anyhow::anyhow!(
                "big decimal exponent `{}` is outside the `{}` to `{}` range",
                exp,
                min_exp,
                max_exp
            )))
        } else {
            Ok(big_decimal)
        }
    }
}

impl ToAscObj<AscEnum<EthereumValueKind>> for ethabi::Token {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEnum<EthereumValueKind>, DeterministicHostError> {
        use ethabi::Token::*;

        let kind = EthereumValueKind::get_kind(self);
        let payload = match self {
            Address(address) => asc_new::<AscAddress, _, _>(heap, address)?.to_payload(),
            FixedBytes(bytes) | Bytes(bytes) => {
                asc_new::<Uint8Array, _, _>(heap, &**bytes)?.to_payload()
            }
            Int(uint) => {
                let n = BigInt::from_signed_u256(&uint);
                asc_new(heap, &n)?.to_payload()
            }
            Uint(uint) => {
                let n = BigInt::from_unsigned_u256(&uint);
                asc_new(heap, &n)?.to_payload()
            }
            Bool(b) => *b as u64,
            String(string) => asc_new(heap, &**string)?.to_payload(),
            FixedArray(tokens) | Array(tokens) => asc_new(heap, &**tokens)?.to_payload(),
            Tuple(tokens) => asc_new(heap, &**tokens)?.to_payload(),
        };

        Ok(AscEnum {
            kind,
            _padding: 0,
            payload: EnumPayload(payload),
        })
    }
}

impl FromAscObj<AscEnum<EthereumValueKind>> for ethabi::Token {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_enum: AscEnum<EthereumValueKind>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        use ethabi::Token;

        let payload = asc_enum.payload;
        Ok(match asc_enum.kind {
            EthereumValueKind::Bool => Token::Bool(bool::from(payload)),
            EthereumValueKind::Address => {
                let ptr: AscPtr<AscAddress> = AscPtr::from(payload);
                Token::Address(asc_get(heap, ptr)?)
            }
            EthereumValueKind::FixedBytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::FixedBytes(asc_get(heap, ptr)?)
            }
            EthereumValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::Bytes(asc_get(heap, ptr)?)
            }
            EthereumValueKind::Int => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = asc_get(heap, ptr)?;
                Token::Int(n.to_signed_u256())
            }
            EthereumValueKind::Uint => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = asc_get(heap, ptr)?;
                Token::Uint(n.to_unsigned_u256())
            }
            EthereumValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Token::String(asc_get(heap, ptr)?)
            }
            EthereumValueKind::FixedArray => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::FixedArray(asc_get(heap, ptr)?)
            }
            EthereumValueKind::Array => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::Array(asc_get(heap, ptr)?)
            }
            EthereumValueKind::Tuple => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::Tuple(asc_get(heap, ptr)?)
            }
        })
    }
}

impl TryFromAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn try_from_asc_obj<H: AscHeap + ?Sized>(
        asc_enum: AscEnum<StoreValueKind>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        use self::store::Value;

        let payload = asc_enum.payload;
        Ok(match asc_enum.kind {
            StoreValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Value::String(asc_get(heap, ptr)?)
            }
            StoreValueKind::Int => Value::Int(i32::from(payload)),
            StoreValueKind::BigDecimal => {
                let ptr: AscPtr<AscBigDecimal> = AscPtr::from(payload);
                Value::BigDecimal(try_asc_get(heap, ptr)?)
            }
            StoreValueKind::Bool => Value::Bool(bool::from(payload)),
            StoreValueKind::Array => {
                let ptr: AscEnumArray<StoreValueKind> = AscPtr::from(payload);
                Value::List(try_asc_get(heap, ptr)?)
            }
            StoreValueKind::Null => Value::Null,
            StoreValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                let array: Vec<u8> = asc_get(heap, ptr)?;
                Value::Bytes(array.as_slice().into())
            }
            StoreValueKind::BigInt => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let array: Vec<u8> = asc_get(heap, ptr)?;
                Value::BigInt(store::scalar::BigInt::from_signed_bytes_le(&array))
            }
        })
    }
}

impl ToAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEnum<StoreValueKind>, DeterministicHostError> {
        use self::store::Value;

        let payload = match self {
            Value::String(string) => asc_new(heap, string.as_str())?.into(),
            Value::Int(n) => EnumPayload::from(*n),
            Value::BigDecimal(n) => asc_new(heap, n)?.into(),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::List(array) => asc_new(heap, array.as_slice())?.into(),
            Value::Null => EnumPayload(0),
            Value::Bytes(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = asc_new(heap, bytes.as_slice())?;
                bytes_obj.into()
            }
            Value::BigInt(big_int) => {
                let bytes_obj: AscPtr<Uint8Array> = asc_new(heap, &*big_int.to_signed_bytes_le())?;
                bytes_obj.into()
            }
        };

        Ok(AscEnum {
            kind: StoreValueKind::get_kind(self),
            _padding: 0,
            payload,
        })
    }
}

impl ToAscObj<AscJson> for serde_json::Map<String, serde_json::Value> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscJson, DeterministicHostError> {
        Ok(AscTypedMap {
            entries: asc_new(heap, &*self.iter().collect::<Vec<_>>())?,
        })
    }
}

// Used for serializing entities.
impl ToAscObj<AscEntity> for Vec<(String, store::Value)> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEntity, DeterministicHostError> {
        Ok(AscTypedMap {
            entries: asc_new(heap, self.as_slice())?,
        })
    }
}

impl ToAscObj<AscEnum<JsonValueKind>> for serde_json::Value {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEnum<JsonValueKind>, DeterministicHostError> {
        use serde_json::Value;

        let payload = match self {
            Value::Null => EnumPayload(0),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::Number(number) => asc_new(heap, &*number.to_string())?.into(),
            Value::String(string) => asc_new(heap, string.as_str())?.into(),
            Value::Array(array) => asc_new(heap, array.as_slice())?.into(),
            Value::Object(object) => asc_new(heap, object)?.into(),
        };

        Ok(AscEnum {
            kind: JsonValueKind::get_kind(self),
            _padding: 0,
            payload,
        })
    }
}

impl From<u32> for LogLevel {
    fn from(i: u32) -> Self {
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

impl<T: AscValue> ToAscObj<AscWrapped<T>> for AscWrapped<T> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscWrapped<T>, DeterministicHostError> {
        Ok(*self)
    }
}

impl<V, VAsc> ToAscObj<AscResult<AscPtr<VAsc>, bool>> for Result<V, bool>
where
    V: ToAscObj<VAsc>,
    VAsc: AscType,
{
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscResult<AscPtr<VAsc>, bool>, DeterministicHostError> {
        Ok(match self {
            Ok(value) => AscResult {
                value: {
                    let inner = asc_new(heap, value)?;
                    let wrapped = AscWrapped { inner };
                    asc_new(heap, &wrapped)?
                },
                error: AscPtr::null(),
            },
            Err(_) => AscResult {
                value: AscPtr::null(),
                error: {
                    let wrapped = AscWrapped { inner: true };
                    asc_new(heap, &wrapped)?
                },
            },
        })
    }
}
