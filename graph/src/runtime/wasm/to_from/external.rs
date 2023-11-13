use ethabi;
use wasmtime::{StoreContext, StoreContextMut};

use crate::data::value::Word;
use crate::prelude::{BigDecimal, BigInt};
use crate::runtime::gas::GasCounter;
use crate::runtime::{
    asc_get, asc_new, AscIndexId, AscPtr, AscType, AscValue, HostExportError, ToAscObj,
    WasmInstanceContext,
};
use crate::{data::store, runtime::DeterministicHostError};
use crate::{prelude::serde_json, runtime::FromAscObj};
use crate::{prelude::web3::types as web3, runtime::AscHeap};

use crate::runtime::asc_abi::class::*;

impl ToAscObj<Uint8Array> for web3::H160 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, HostExportError> {
        self.0.to_asc_obj(store, heap, gas)
    }
}

impl FromAscObj<Uint8Array> for web3::H160 {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: Uint8Array,
        store: &StoreContext<WasmInstanceContext>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 20]>::from_asc_obj(typed_array, store, heap, gas, depth)?;
        Ok(Self(data))
    }
}

impl FromAscObj<Uint8Array> for web3::H256 {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: Uint8Array,
        store: &StoreContext<WasmInstanceContext>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 32]>::from_asc_obj(typed_array, store, heap, gas, depth)?;
        Ok(Self(data))
    }
}

impl ToAscObj<Uint8Array> for web3::H256 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, HostExportError> {
        self.0.to_asc_obj(store, heap, gas)
    }
}

impl ToAscObj<AscBigInt> for web3::U128 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBigInt, HostExportError> {
        let mut bytes: [u8; 16] = [0; 16];
        self.to_little_endian(&mut bytes);
        bytes.to_asc_obj(store, heap, gas)
    }
}

impl ToAscObj<AscBigInt> for BigInt {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBigInt, HostExportError> {
        let bytes = self.to_signed_bytes_le();
        bytes.to_asc_obj(store, heap, gas)
    }
}

impl FromAscObj<AscBigInt> for BigInt {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        array_buffer: AscBigInt,
        store: &StoreContext<WasmInstanceContext>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let bytes = <Vec<u8>>::from_asc_obj(array_buffer, store, heap, gas, depth)?;
        Ok(BigInt::from_signed_bytes_le(&bytes)?)
    }
}

impl ToAscObj<AscBigDecimal> for BigDecimal {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBigDecimal, HostExportError> {
        // From the docs: "Note that a positive exponent indicates a negative power of 10",
        // so "exponent" is the opposite of what you'd expect.
        let (digits, negative_exp) = self.as_bigint_and_exponent();
        Ok(AscBigDecimal {
            exp: asc_new(store, heap, &BigInt::from(-negative_exp), gas)?,
            digits: asc_new(store, heap, &BigInt::new(digits)?, gas)?,
        })
    }
}

impl FromAscObj<AscBigDecimal> for BigDecimal {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        big_decimal: AscBigDecimal,
        store: &StoreContext<WasmInstanceContext>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let digits: BigInt = asc_get(&store, heap, big_decimal.digits, gas, depth)?;
        let exp: BigInt = asc_get(&store, heap, big_decimal.exp, gas, depth)?;

        let bytes = exp.to_signed_bytes_le();
        let mut byte_array = if exp >= 0.into() { [0; 8] } else { [255; 8] };
        byte_array[..bytes.len()].copy_from_slice(&bytes);
        let big_decimal = BigDecimal::new(digits, i64::from_le_bytes(byte_array));

        // Validate the exponent.
        let exp = -big_decimal.as_bigint_and_exponent().1;
        let min_exp: i64 = BigDecimal::MIN_EXP.into();
        let max_exp: i64 = BigDecimal::MAX_EXP.into();
        if exp < min_exp || max_exp < exp {
            Err(DeterministicHostError::from(anyhow::anyhow!(
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

impl ToAscObj<Array<AscPtr<AscString>>> for Vec<String> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Array<AscPtr<AscString>>, HostExportError> {
        let content: Result<Vec<_>, _> =
            self.iter().map(|x| asc_new(store, heap, x, gas)).collect();
        let content = content?;
        Array::new(store, &content, heap, gas)
    }
}

impl ToAscObj<AscEnum<EthereumValueKind>> for ethabi::Token {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEnum<EthereumValueKind>, HostExportError> {
        use ethabi::Token::*;

        let kind = EthereumValueKind::get_kind(self);
        let payload = match self {
            Address(address) => {
                asc_new::<AscAddress, _, _>(store, heap, address, gas)?.to_payload()
            }
            FixedBytes(bytes) | Bytes(bytes) => {
                asc_new::<Uint8Array, _, _>(store, heap, &**bytes, gas)?.to_payload()
            }
            Int(uint) => {
                let n = BigInt::from_signed_u256(uint);
                asc_new(store, heap, &n, gas)?.to_payload()
            }
            Uint(uint) => {
                let n = BigInt::from_unsigned_u256(uint);
                asc_new(store, heap, &n, gas)?.to_payload()
            }
            Bool(b) => *b as u64,
            String(string) => asc_new(store, heap, &**string, gas)?.to_payload(),
            FixedArray(tokens) | Array(tokens) => {
                asc_new(store, heap, &**tokens, gas)?.to_payload()
            }
            Tuple(tokens) => asc_new(store, heap, &**tokens, gas)?.to_payload(),
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
        store: &StoreContext<WasmInstanceContext>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        use ethabi::Token;

        let payload = asc_enum.payload;
        Ok(match asc_enum.kind {
            EthereumValueKind::Bool => Token::Bool(bool::from(payload)),
            EthereumValueKind::Address => {
                let ptr: AscPtr<AscAddress> = AscPtr::from(payload);
                Token::Address(asc_get(&store, heap, ptr, gas, depth)?)
            }
            EthereumValueKind::FixedBytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::FixedBytes(asc_get(&store, heap, ptr, gas, depth)?)
            }
            EthereumValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::Bytes(asc_get(&store, heap, ptr, gas, depth)?)
            }
            EthereumValueKind::Int => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = asc_get(&store, heap, ptr, gas, depth)?;
                Token::Int(n.to_signed_u256())
            }
            EthereumValueKind::Uint => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = asc_get(&store, heap, ptr, gas, depth)?;
                Token::Uint(n.to_unsigned_u256())
            }
            EthereumValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Token::String(asc_get(&store, heap, ptr, gas, depth)?)
            }
            EthereumValueKind::FixedArray => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::FixedArray(asc_get(&store, heap, ptr, gas, depth)?)
            }
            EthereumValueKind::Array => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::Array(asc_get(&store, heap, ptr, gas, depth)?)
            }
            EthereumValueKind::Tuple => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::Tuple(asc_get(&store, heap, ptr, gas, depth)?)
            }
        })
    }
}

impl FromAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_enum: AscEnum<StoreValueKind>,
        store: &StoreContext<WasmInstanceContext>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        use self::store::Value;

        let payload = asc_enum.payload;
        Ok(match asc_enum.kind {
            StoreValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Value::String(asc_get(&store, heap, ptr, gas, depth)?)
            }
            StoreValueKind::Int => Value::Int(i32::from(payload)),
            StoreValueKind::Int8 => Value::Int8(i64::from(payload)),
            StoreValueKind::BigDecimal => {
                let ptr: AscPtr<AscBigDecimal> = AscPtr::from(payload);
                Value::BigDecimal(asc_get(&store, heap, ptr, gas, depth)?)
            }
            StoreValueKind::Bool => Value::Bool(bool::from(payload)),
            StoreValueKind::Array => {
                let ptr: AscEnumArray<StoreValueKind> = AscPtr::from(payload);
                Value::List(asc_get(&store, heap, ptr, gas, depth)?)
            }
            StoreValueKind::Null => Value::Null,
            StoreValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                let array: Vec<u8> = asc_get(&store, heap, ptr, gas, depth)?;
                Value::Bytes(array.as_slice().into())
            }
            StoreValueKind::BigInt => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let array: Vec<u8> = asc_get(&store, heap, ptr, gas, depth)?;
                Value::BigInt(store::scalar::BigInt::from_signed_bytes_le(&array)?)
            }
        })
    }
}

impl ToAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEnum<StoreValueKind>, HostExportError> {
        use self::store::Value;

        let payload = match self {
            Value::String(string) => asc_new(store, heap, string.as_str(), gas)?.into(),
            Value::Int(n) => EnumPayload::from(*n),
            Value::Int8(n) => EnumPayload::from(*n),
            Value::BigDecimal(n) => asc_new(store, heap, n, gas)?.into(),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::List(array) => asc_new(store, heap, array.as_slice(), gas)?.into(),
            Value::Null => EnumPayload(0),
            Value::Bytes(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = asc_new(store, heap, bytes.as_slice(), gas)?;
                bytes_obj.into()
            }
            Value::BigInt(big_int) => {
                let bytes_obj: AscPtr<Uint8Array> =
                    asc_new(store, heap, &*big_int.to_signed_bytes_le(), gas)?;
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
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscJson, HostExportError> {
        Ok(AscTypedMap {
            entries: asc_new(store, heap, &*self.iter().collect::<Vec<_>>(), gas)?,
        })
    }
}

// Used for serializing entities.
impl ToAscObj<AscEntity> for Vec<(Word, store::Value)> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEntity, HostExportError> {
        Ok(AscTypedMap {
            entries: asc_new(store, heap, self.as_slice(), gas)?,
        })
    }
}

impl ToAscObj<AscEntity> for Vec<(&str, &store::Value)> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEntity, HostExportError> {
        Ok(AscTypedMap {
            entries: asc_new(store, heap, self.as_slice(), gas)?,
        })
    }
}

impl ToAscObj<Array<AscPtr<AscEntity>>> for Vec<Vec<(Word, store::Value)>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Array<AscPtr<AscEntity>>, HostExportError> {
        let content: Result<Vec<_>, _> =
            self.iter().map(|x| asc_new(store, heap, &x, gas)).collect();
        let content = content?;
        Array::new(store, &content, heap, gas)
    }
}

impl ToAscObj<AscEnum<JsonValueKind>> for serde_json::Value {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEnum<JsonValueKind>, HostExportError> {
        use serde_json::Value;

        let payload = match self {
            Value::Null => EnumPayload(0),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::Number(number) => asc_new(store, heap, &*number.to_string(), gas)?.into(),
            Value::String(string) => asc_new(store, heap, string.as_str(), gas)?.into(),
            Value::Array(array) => asc_new(store, heap, array.as_slice(), gas)?.into(),
            Value::Object(object) => asc_new(store, heap, object, gas)?.into(),
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
        _store: &mut StoreContextMut<WasmInstanceContext>,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscWrapped<T>, HostExportError> {
        Ok(*self)
    }
}

impl<V, VAsc> ToAscObj<AscResult<AscPtr<VAsc>, bool>> for Result<V, bool>
where
    V: ToAscObj<VAsc>,
    VAsc: AscType + AscIndexId,
    AscWrapped<AscPtr<VAsc>>: AscIndexId,
{
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscResult<AscPtr<VAsc>, bool>, HostExportError> {
        Ok(match self {
            Ok(value) => AscResult {
                value: {
                    let inner = asc_new(store, heap, value, gas)?;
                    let wrapped = AscWrapped { inner };
                    asc_new(store, heap, &wrapped, gas)?
                },
                error: AscPtr::null(),
            },
            Err(_) => AscResult {
                value: AscPtr::null(),
                error: {
                    let wrapped = AscWrapped { inner: true };
                    asc_new(store, heap, &wrapped, gas)?
                },
            },
        })
    }
}
