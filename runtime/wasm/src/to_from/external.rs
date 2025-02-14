use graph::abi;
use graph::abi::DynSolValueExt;
use graph::data::store::scalar::Timestamp;
use graph::data::value::Word;
use graph::prelude::{BigDecimal, BigInt};
use graph::runtime::gas::GasCounter;
use graph::runtime::{
    asc_get, asc_new, AscIndexId, AscPtr, AscType, AscValue, HostExportError, ToAscObj,
};
use graph::{data::store, runtime::DeterministicHostError};
use graph::{prelude::serde_json, runtime::FromAscObj};
use graph::{prelude::web3::types as web3, runtime::AscHeap};
use graph_runtime_derive::AscType;

use crate::asc_abi::class::*;

impl ToAscObj<Uint8Array> for web3::H160 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, HostExportError> {
        self.0.to_asc_obj(heap, gas)
    }
}

impl ToAscObj<Uint8Array> for web3::Bytes {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, HostExportError> {
        self.0.to_asc_obj(heap, gas)
    }
}

impl FromAscObj<Uint8Array> for web3::H160 {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: Uint8Array,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 20]>::from_asc_obj(typed_array, heap, gas, depth)?;
        Ok(Self(data))
    }
}

impl FromAscObj<Uint8Array> for web3::H256 {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        typed_array: Uint8Array,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 32]>::from_asc_obj(typed_array, heap, gas, depth)?;
        Ok(Self(data))
    }
}

impl ToAscObj<Uint8Array> for web3::H256 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, HostExportError> {
        self.0.to_asc_obj(heap, gas)
    }
}

impl ToAscObj<AscBigInt> for web3::U128 {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBigInt, HostExportError> {
        let mut bytes: [u8; 16] = [0; 16];
        self.to_little_endian(&mut bytes);
        bytes.to_asc_obj(heap, gas)
    }
}

impl ToAscObj<AscBigInt> for BigInt {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBigInt, HostExportError> {
        let bytes = self.to_signed_bytes_le();
        bytes.to_asc_obj(heap, gas)
    }
}

impl FromAscObj<AscBigInt> for BigInt {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        array_buffer: AscBigInt,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let bytes = <Vec<u8>>::from_asc_obj(array_buffer, heap, gas, depth)?;
        Ok(BigInt::from_signed_bytes_le(&bytes)?)
    }
}

impl ToAscObj<AscBigDecimal> for BigDecimal {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBigDecimal, HostExportError> {
        // From the docs: "Note that a positive exponent indicates a negative power of 10",
        // so "exponent" is the opposite of what you'd expect.
        let (digits, negative_exp) = self.as_bigint_and_exponent();
        Ok(AscBigDecimal {
            exp: asc_new(heap, &BigInt::from(-negative_exp), gas)?,
            digits: asc_new(heap, &BigInt::new(digits)?, gas)?,
        })
    }
}

impl FromAscObj<AscBigDecimal> for BigDecimal {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        big_decimal: AscBigDecimal,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let digits: BigInt = asc_get(heap, big_decimal.digits, gas, depth)?;
        let exp: BigInt = asc_get(heap, big_decimal.exp, gas, depth)?;

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
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Array<AscPtr<AscString>>, HostExportError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();
        let content = content?;
        Array::new(&content, heap, gas)
    }
}

impl ToAscObj<AscEnum<EthereumValueKind>> for abi::DynSolValue {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEnum<EthereumValueKind>, HostExportError> {
        let kind = EthereumValueKind::get_kind(self);

        let payload = match self {
            Self::Bool(val) => *val as u64,
            Self::Int(val, _) => {
                let bytes = val.to_le_bytes::<32>();
                let n = BigInt::from_signed_bytes_le(&bytes)?;

                asc_new(heap, &n, gas)?.to_payload()
            }
            Self::Uint(val, _) => {
                let bytes = val.to_le_bytes::<32>();
                let n = BigInt::from_unsigned_bytes_le(&bytes)?;

                asc_new(heap, &n, gas)?.to_payload()
            }
            Self::FixedBytes(val, _) => {
                asc_new::<Uint8Array, _, _>(heap, val.as_slice(), gas)?.to_payload()
            }
            Self::Address(val) => {
                asc_new::<AscAddress, _, _>(heap, val.as_slice(), gas)?.to_payload()
            }
            Self::Function(val) => {
                asc_new::<Uint8Array, _, _>(heap, val.as_slice(), gas)?.to_payload()
            }
            Self::Bytes(val) => asc_new::<Uint8Array, _, _>(heap, &**val, gas)?.to_payload(),
            Self::String(val) => asc_new(heap, &**val, gas)?.to_payload(),
            Self::Array(values) => asc_new(heap, &**values, gas)?.to_payload(),
            Self::FixedArray(values) => asc_new(heap, &**values, gas)?.to_payload(),
            Self::Tuple(values) => asc_new(heap, &**values, gas)?.to_payload(),
        };

        Ok(AscEnum {
            kind,
            _padding: 0,
            payload: EnumPayload(payload),
        })
    }
}

impl FromAscObj<AscEnum<EthereumValueKind>> for abi::DynSolValue {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_enum: AscEnum<EthereumValueKind>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        let payload = asc_enum.payload;

        let value = match asc_enum.kind {
            EthereumValueKind::Address => {
                let ptr: AscPtr<AscAddress> = AscPtr::from(payload);
                let bytes: [u8; 20] = asc_get(heap, ptr, gas, depth)?;

                Self::Address(bytes.into())
            }
            EthereumValueKind::FixedBytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                let bytes: Vec<u8> = asc_get(heap, ptr, gas, depth)?;

                Self::fixed_bytes_from_slice(&bytes)?
            }
            EthereumValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                let bytes: Vec<u8> = asc_get(heap, ptr, gas, depth)?;

                Self::Bytes(bytes)
            }
            EthereumValueKind::Int => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = asc_get(heap, ptr, gas, depth)?;
                let x = abi::I256::from_limbs(n.to_signed_u256().0);

                Self::Int(x, x.bits() as usize)
            }
            EthereumValueKind::Uint => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = asc_get(heap, ptr, gas, depth)?;
                let x = abi::U256::from_limbs(n.to_unsigned_u256().0);

                Self::Uint(x, x.bit_len())
            }
            EthereumValueKind::Bool => Self::Bool(bool::from(payload)),
            EthereumValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);

                Self::String(asc_get(heap, ptr, gas, depth)?)
            }
            EthereumValueKind::FixedArray => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);

                Self::FixedArray(asc_get(heap, ptr, gas, depth)?)
            }
            EthereumValueKind::Array => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);

                Self::Array(asc_get(heap, ptr, gas, depth)?)
            }
            EthereumValueKind::Tuple => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);

                Self::Tuple(asc_get(heap, ptr, gas, depth)?)
            }
            EthereumValueKind::Function => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                let bytes: [u8; 24] = asc_get(heap, ptr, gas, depth)?;

                Self::Function(bytes.into())
            }
        };

        Ok(value)
    }
}

impl FromAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_enum: AscEnum<StoreValueKind>,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        use self::store::Value;

        let payload = asc_enum.payload;
        Ok(match asc_enum.kind {
            StoreValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Value::String(asc_get(heap, ptr, gas, depth)?)
            }
            StoreValueKind::Int => Value::Int(i32::from(payload)),
            StoreValueKind::Int8 => Value::Int8(i64::from(payload)),
            StoreValueKind::Timestamp => {
                let ts = Timestamp::from_microseconds_since_epoch(i64::from(payload))
                    .map_err(|e| DeterministicHostError::Other(e.into()))?;

                Value::Timestamp(ts)
            }
            StoreValueKind::BigDecimal => {
                let ptr: AscPtr<AscBigDecimal> = AscPtr::from(payload);
                Value::BigDecimal(asc_get(heap, ptr, gas, depth)?)
            }
            StoreValueKind::Bool => Value::Bool(bool::from(payload)),
            StoreValueKind::Array => {
                let ptr: AscEnumArray<StoreValueKind> = AscPtr::from(payload);
                Value::List(asc_get(heap, ptr, gas, depth)?)
            }
            StoreValueKind::Null => Value::Null,
            StoreValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                let array: Vec<u8> = asc_get(heap, ptr, gas, depth)?;
                Value::Bytes(array.as_slice().into())
            }
            StoreValueKind::BigInt => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let array: Vec<u8> = asc_get(heap, ptr, gas, depth)?;
                Value::BigInt(store::scalar::BigInt::from_signed_bytes_le(&array)?)
            }
        })
    }
}

impl ToAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEnum<StoreValueKind>, HostExportError> {
        use self::store::Value;

        let payload = match self {
            Value::String(string) => asc_new(heap, string.as_str(), gas)?.into(),
            Value::Int(n) => EnumPayload::from(*n),
            Value::Int8(n) => EnumPayload::from(*n),
            Value::Timestamp(n) => EnumPayload::from(n),
            Value::BigDecimal(n) => asc_new(heap, n, gas)?.into(),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::List(array) => asc_new(heap, array.as_slice(), gas)?.into(),
            Value::Null => EnumPayload(0),
            Value::Bytes(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = asc_new(heap, bytes.as_slice(), gas)?;
                bytes_obj.into()
            }
            Value::BigInt(big_int) => {
                let bytes_obj: AscPtr<Uint8Array> =
                    asc_new(heap, &*big_int.to_signed_bytes_le(), gas)?;
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
        gas: &GasCounter,
    ) -> Result<AscJson, HostExportError> {
        Ok(AscTypedMap {
            entries: asc_new(heap, &*self.iter().collect::<Vec<_>>(), gas)?,
        })
    }
}

// Used for serializing entities.
impl ToAscObj<AscEntity> for Vec<(Word, store::Value)> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEntity, HostExportError> {
        Ok(AscTypedMap {
            entries: asc_new(heap, self.as_slice(), gas)?,
        })
    }
}

impl ToAscObj<AscEntity> for Vec<(&str, &store::Value)> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEntity, HostExportError> {
        Ok(AscTypedMap {
            entries: asc_new(heap, self.as_slice(), gas)?,
        })
    }
}

impl ToAscObj<Array<AscPtr<AscEntity>>> for Vec<Vec<(Word, store::Value)>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Array<AscPtr<AscEntity>>, HostExportError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, &x, gas)).collect();
        let content = content?;
        Array::new(&content, heap, gas)
    }
}

impl ToAscObj<AscEnum<JsonValueKind>> for serde_json::Value {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEnum<JsonValueKind>, HostExportError> {
        use serde_json::Value;

        let payload = match self {
            Value::Null => EnumPayload(0),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::Number(number) => asc_new(heap, &*number.to_string(), gas)?.into(),
            Value::String(string) => asc_new(heap, string.as_str(), gas)?.into(),
            Value::Array(array) => asc_new(heap, array.as_slice(), gas)?.into(),
            Value::Object(object) => asc_new(heap, object, gas)?.into(),
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
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscResult<AscPtr<VAsc>, bool>, HostExportError> {
        Ok(match self {
            Ok(value) => AscResult {
                value: {
                    let inner = asc_new(heap, value, gas)?;
                    let wrapped = AscWrapped { inner };
                    asc_new(heap, &wrapped, gas)?
                },
                error: AscPtr::null(),
            },
            Err(_) => AscResult {
                value: AscPtr::null(),
                error: {
                    let wrapped = AscWrapped { inner: true };
                    asc_new(heap, &wrapped, gas)?
                },
            },
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, AscType)]
pub enum AscSubgraphEntityOp {
    Create,
    Modify,
    Delete,
}

impl ToAscObj<AscEnum<YamlValueKind>> for serde_yaml::Value {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEnum<YamlValueKind>, HostExportError> {
        use serde_yaml::Value;

        let payload = match self {
            Value::Null => EnumPayload(0),
            Value::Bool(val) => EnumPayload::from(*val),
            Value::Number(val) => asc_new(heap, &val.to_string(), gas)?.into(),
            Value::String(val) => asc_new(heap, val, gas)?.into(),
            Value::Sequence(val) => asc_new(heap, val.as_slice(), gas)?.into(),
            Value::Mapping(val) => asc_new(heap, val, gas)?.into(),
            Value::Tagged(val) => asc_new(heap, val.as_ref(), gas)?.into(),
        };

        Ok(AscEnum {
            kind: YamlValueKind::get_kind(self),
            _padding: 0,
            payload,
        })
    }
}

impl ToAscObj<AscTypedMap<AscEnum<YamlValueKind>, AscEnum<YamlValueKind>>> for serde_yaml::Mapping {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTypedMap<AscEnum<YamlValueKind>, AscEnum<YamlValueKind>>, HostExportError> {
        Ok(AscTypedMap {
            entries: asc_new(heap, &*self.iter().collect::<Vec<_>>(), gas)?,
        })
    }
}

impl ToAscObj<AscYamlTaggedValue> for serde_yaml::value::TaggedValue {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscYamlTaggedValue, HostExportError> {
        Ok(AscYamlTaggedValue {
            tag: asc_new(heap, &self.tag.to_string(), gas)?,
            value: asc_new(heap, &self.value, gas)?,
        })
    }
}
