use ethabi;

use graph::{
    components::ethereum::{
        EthereumBlockData, EthereumCallData, EthereumEventData, EthereumTransactionData,
    },
    runtime::{AscPtr, AscType, ToAscObj},
};
use graph::{data::store, runtime::DeterministicHostError};
use graph::{prelude::serde_json, runtime::FromAscObj};
use graph::{prelude::web3::types as web3, runtime::AscHeap};
use graph::{
    prelude::{BigDecimal, BigInt},
    runtime::TryFromAscObj,
};

use crate::asc_abi::class::*;

use crate::UnresolvedContractCall;

impl ToAscObj<Uint8Array> for web3::H160 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<Uint8Array, DeterministicHostError> {
        self.0.to_asc_obj(heap)
    }
}

impl FromAscObj<Uint8Array> for web3::H160 {
    fn from_asc_obj<H: AscHeap>(
        typed_array: Uint8Array,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 20]>::from_asc_obj(typed_array, heap)?;
        Ok(Self(data))
    }
}

impl FromAscObj<Uint8Array> for web3::H256 {
    fn from_asc_obj<H: AscHeap>(
        typed_array: Uint8Array,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let data = <[u8; 32]>::from_asc_obj(typed_array, heap)?;
        Ok(Self(data))
    }
}

impl ToAscObj<Uint8Array> for web3::H256 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<Uint8Array, DeterministicHostError> {
        self.0.to_asc_obj(heap)
    }
}

impl ToAscObj<AscBigInt> for web3::U128 {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<AscBigInt, DeterministicHostError> {
        let mut bytes: [u8; 16] = [0; 16];
        self.to_little_endian(&mut bytes);
        bytes.to_asc_obj(heap)
    }
}

impl ToAscObj<AscBigInt> for BigInt {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<AscBigInt, DeterministicHostError> {
        let bytes = self.to_signed_bytes_le();
        bytes.to_asc_obj(heap)
    }
}

impl FromAscObj<AscBigInt> for BigInt {
    fn from_asc_obj<H: AscHeap>(
        array_buffer: AscBigInt,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let bytes = <Vec<u8>>::from_asc_obj(array_buffer, heap)?;
        Ok(BigInt::from_signed_bytes_le(&bytes))
    }
}

impl ToAscObj<AscBigDecimal> for BigDecimal {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscBigDecimal, DeterministicHostError> {
        // From the docs: "Note that a positive exponent indicates a negative power of 10",
        // so "exponent" is the opposite of what you'd expect.
        let (digits, negative_exp) = self.as_bigint_and_exponent();
        Ok(AscBigDecimal {
            exp: heap.asc_new(&BigInt::from(-negative_exp))?,
            digits: heap.asc_new(&BigInt::from(digits))?,
        })
    }
}

impl TryFromAscObj<AscBigDecimal> for BigDecimal {
    fn try_from_asc_obj<H: AscHeap>(
        big_decimal: AscBigDecimal,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        let digits: BigInt = heap.asc_get(big_decimal.digits)?;
        let exp: BigInt = heap.asc_get(big_decimal.exp)?;

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
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEnum<EthereumValueKind>, DeterministicHostError> {
        use ethabi::Token::*;

        let kind = EthereumValueKind::get_kind(self);
        let payload = match self {
            Address(address) => heap.asc_new::<AscAddress, _>(address)?.to_payload(),
            FixedBytes(bytes) | Bytes(bytes) => {
                heap.asc_new::<Uint8Array, _>(&**bytes)?.to_payload()
            }
            Int(uint) => {
                let n = BigInt::from_signed_u256(&uint);
                heap.asc_new(&n)?.to_payload()
            }
            Uint(uint) => {
                let n = BigInt::from_unsigned_u256(&uint);
                heap.asc_new(&n)?.to_payload()
            }
            Bool(b) => *b as u64,
            String(string) => heap.asc_new(&**string)?.to_payload(),
            FixedArray(tokens) | Array(tokens) => heap.asc_new(&**tokens)?.to_payload(),
            Tuple(tokens) => heap.asc_new(&**tokens)?.to_payload(),
        };

        Ok(AscEnum {
            kind,
            _padding: 0,
            payload: EnumPayload(payload),
        })
    }
}

impl FromAscObj<AscEnum<EthereumValueKind>> for ethabi::Token {
    fn from_asc_obj<H: AscHeap>(
        asc_enum: AscEnum<EthereumValueKind>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        use ethabi::Token;

        let payload = asc_enum.payload;
        Ok(match asc_enum.kind {
            EthereumValueKind::Bool => Token::Bool(bool::from(payload)),
            EthereumValueKind::Address => {
                let ptr: AscPtr<AscAddress> = AscPtr::from(payload);
                Token::Address(heap.asc_get(ptr)?)
            }
            EthereumValueKind::FixedBytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::FixedBytes(heap.asc_get(ptr)?)
            }
            EthereumValueKind::Bytes => {
                let ptr: AscPtr<Uint8Array> = AscPtr::from(payload);
                Token::Bytes(heap.asc_get(ptr)?)
            }
            EthereumValueKind::Int => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = heap.asc_get(ptr)?;
                Token::Int(n.to_signed_u256())
            }
            EthereumValueKind::Uint => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = heap.asc_get(ptr)?;
                Token::Uint(n.to_unsigned_u256())
            }
            EthereumValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Token::String(heap.asc_get(ptr)?)
            }
            EthereumValueKind::FixedArray => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::FixedArray(heap.asc_get(ptr)?)
            }
            EthereumValueKind::Array => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::Array(heap.asc_get(ptr)?)
            }
            EthereumValueKind::Tuple => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::Tuple(heap.asc_get(ptr)?)
            }
        })
    }
}

impl TryFromAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn try_from_asc_obj<H: AscHeap>(
        asc_enum: AscEnum<StoreValueKind>,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        use self::store::Value;

        let payload = asc_enum.payload;
        Ok(match asc_enum.kind {
            StoreValueKind::String => {
                let ptr: AscPtr<AscString> = AscPtr::from(payload);
                Value::String(heap.asc_get(ptr)?)
            }
            StoreValueKind::Int => Value::Int(i32::from(payload)),
            StoreValueKind::BigDecimal => {
                let ptr: AscPtr<AscBigDecimal> = AscPtr::from(payload);
                Value::BigDecimal(heap.try_asc_get(ptr)?)
            }
            StoreValueKind::Bool => Value::Bool(bool::from(payload)),
            StoreValueKind::Array => {
                let ptr: AscEnumArray<StoreValueKind> = AscPtr::from(payload);
                Value::List(heap.try_asc_get(ptr)?)
            }
            StoreValueKind::Null => Value::Null,
            StoreValueKind::Bytes => {
                let ptr: AscPtr<Bytes> = AscPtr::from(payload);
                let array: Vec<u8> = heap.asc_get(ptr)?;
                Value::Bytes(array.as_slice().into())
            }
            StoreValueKind::BigInt => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let array: Vec<u8> = heap.asc_get(ptr)?;
                Value::BigInt(store::scalar::BigInt::from_signed_bytes_le(&array))
            }
        })
    }
}

impl ToAscObj<AscEnum<StoreValueKind>> for store::Value {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEnum<StoreValueKind>, DeterministicHostError> {
        use self::store::Value;

        let payload = match self {
            Value::String(string) => heap.asc_new(string.as_str())?.into(),
            Value::Int(n) => EnumPayload::from(*n),
            Value::BigDecimal(n) => heap.asc_new(n)?.into(),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::List(array) => heap.asc_new(array.as_slice())?.into(),
            Value::Null => EnumPayload(0),
            Value::Bytes(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = heap.asc_new(bytes.as_slice())?;
                bytes_obj.into()
            }
            Value::BigInt(big_int) => {
                let bytes_obj: AscPtr<Uint8Array> = heap.asc_new(&*big_int.to_signed_bytes_le())?;
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

impl ToAscObj<AscLogParam> for ethabi::LogParam {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<AscLogParam, DeterministicHostError> {
        Ok(AscLogParam {
            name: heap.asc_new(self.name.as_str())?,
            value: heap.asc_new(&self.value)?,
        })
    }
}

impl ToAscObj<AscJson> for serde_json::Map<String, serde_json::Value> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<AscJson, DeterministicHostError> {
        Ok(AscTypedMap {
            entries: heap.asc_new(&*self.iter().collect::<Vec<_>>())?,
        })
    }
}

// Used for serializing entities.
impl ToAscObj<AscEntity> for Vec<(String, store::Value)> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<AscEntity, DeterministicHostError> {
        Ok(AscTypedMap {
            entries: heap.asc_new(self.as_slice())?,
        })
    }
}

impl ToAscObj<AscEnum<JsonValueKind>> for serde_json::Value {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEnum<JsonValueKind>, DeterministicHostError> {
        use serde_json::Value;

        let payload = match self {
            Value::Null => EnumPayload(0),
            Value::Bool(b) => EnumPayload::from(*b),
            Value::Number(number) => heap.asc_new(&*number.to_string())?.into(),
            Value::String(string) => heap.asc_new(string.as_str())?.into(),
            Value::Array(array) => heap.asc_new(array.as_slice())?.into(),
            Value::Object(object) => heap.asc_new(object)?.into(),
        };

        Ok(AscEnum {
            kind: JsonValueKind::get_kind(self),
            _padding: 0,
            payload,
        })
    }
}

impl ToAscObj<AscEthereumBlock> for EthereumBlockData {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumBlock, DeterministicHostError> {
        Ok(AscEthereumBlock {
            hash: heap.asc_new(&self.hash)?,
            parent_hash: heap.asc_new(&self.parent_hash)?,
            uncles_hash: heap.asc_new(&self.uncles_hash)?,
            author: heap.asc_new(&self.author)?,
            state_root: heap.asc_new(&self.state_root)?,
            transactions_root: heap.asc_new(&self.transactions_root)?,
            receipts_root: heap.asc_new(&self.receipts_root)?,
            number: heap.asc_new(&BigInt::from(self.number))?,
            gas_used: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_used))?,
            gas_limit: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_limit))?,
            timestamp: heap.asc_new(&BigInt::from_unsigned_u256(&self.timestamp))?,
            difficulty: heap.asc_new(&BigInt::from_unsigned_u256(&self.difficulty))?,
            total_difficulty: heap.asc_new(&BigInt::from_unsigned_u256(&self.total_difficulty))?,
            size: self
                .size
                .map(|size| heap.asc_new(&BigInt::from_unsigned_u256(&size)))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}

impl ToAscObj<AscEthereumTransaction> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumTransaction, DeterministicHostError> {
        Ok(AscEthereumTransaction {
            hash: heap.asc_new(&self.hash)?,
            index: heap.asc_new(&BigInt::from(self.index))?,
            from: heap.asc_new(&self.from)?,
            to: self
                .to
                .map(|to| heap.asc_new(&to))
                .unwrap_or(Ok(AscPtr::null()))?,
            value: heap.asc_new(&BigInt::from_unsigned_u256(&self.value))?,
            gas_used: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_used))?,
            gas_price: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_price))?,
        })
    }
}

impl ToAscObj<AscEthereumTransaction_0_0_2> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumTransaction_0_0_2, DeterministicHostError> {
        Ok(AscEthereumTransaction_0_0_2 {
            hash: heap.asc_new(&self.hash)?,
            index: heap.asc_new(&BigInt::from(self.index))?,
            from: heap.asc_new(&self.from)?,
            to: self
                .to
                .map(|to| heap.asc_new(&to))
                .unwrap_or(Ok(AscPtr::null()))?,
            value: heap.asc_new(&BigInt::from_unsigned_u256(&self.value))?,
            gas_used: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_used))?,
            gas_price: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_price))?,
            input: heap.asc_new(&*self.input.0)?,
        })
    }
}

impl<T: AscType> ToAscObj<AscEthereumEvent<T>> for EthereumEventData
where
    EthereumTransactionData: ToAscObj<T>,
{
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumEvent<T>, DeterministicHostError> {
        Ok(AscEthereumEvent {
            address: heap.asc_new(&self.address)?,
            log_index: heap.asc_new(&BigInt::from_unsigned_u256(&self.log_index))?,
            transaction_log_index: heap
                .asc_new(&BigInt::from_unsigned_u256(&self.transaction_log_index))?,
            log_type: self
                .log_type
                .clone()
                .map(|log_type| heap.asc_new(&log_type))
                .unwrap_or(Ok(AscPtr::null()))?,
            block: heap.asc_new(&self.block)?,
            transaction: heap.asc_new::<T, EthereumTransactionData>(&self.transaction)?,
            params: heap.asc_new(self.params.as_slice())?,
        })
    }
}

impl ToAscObj<AscEthereumCall> for EthereumCallData {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumCall, DeterministicHostError> {
        Ok(AscEthereumCall {
            address: heap.asc_new(&self.to)?,
            block: heap.asc_new(&self.block)?,
            transaction: heap.asc_new(&self.transaction)?,
            inputs: heap.asc_new(self.inputs.as_slice())?,
            outputs: heap.asc_new(self.outputs.as_slice())?,
        })
    }
}

impl ToAscObj<AscEthereumCall_0_0_3> for EthereumCallData {
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumCall_0_0_3, DeterministicHostError> {
        Ok(AscEthereumCall_0_0_3 {
            to: heap.asc_new(&self.to)?,
            from: heap.asc_new(&self.from)?,
            block: heap.asc_new(&self.block)?,
            transaction: heap.asc_new(&self.transaction)?,
            inputs: heap.asc_new(self.inputs.as_slice())?,
            outputs: heap.asc_new(self.outputs.as_slice())?,
        })
    }
}

impl FromAscObj<AscUnresolvedContractCall> for UnresolvedContractCall {
    fn from_asc_obj<H: AscHeap>(
        asc_call: AscUnresolvedContractCall,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        Ok(UnresolvedContractCall {
            contract_name: heap.asc_get(asc_call.contract_name)?,
            contract_address: heap.asc_get(asc_call.contract_address)?,
            function_name: heap.asc_get(asc_call.function_name)?,
            function_signature: None,
            function_args: heap.asc_get(asc_call.function_args)?,
        })
    }
}

impl FromAscObj<AscUnresolvedContractCall_0_0_4> for UnresolvedContractCall {
    fn from_asc_obj<H: AscHeap>(
        asc_call: AscUnresolvedContractCall_0_0_4,
        heap: &H,
    ) -> Result<Self, DeterministicHostError> {
        Ok(UnresolvedContractCall {
            contract_name: heap.asc_get(asc_call.contract_name)?,
            contract_address: heap.asc_get(asc_call.contract_address)?,
            function_name: heap.asc_get(asc_call.function_name)?,
            function_signature: Some(heap.asc_get(asc_call.function_signature)?),
            function_args: heap.asc_get(asc_call.function_args)?,
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

impl<T: AscType> ToAscObj<AscWrapped<T>> for AscWrapped<T> {
    fn to_asc_obj<H: AscHeap>(
        &self,
        _heap: &mut H,
    ) -> Result<AscWrapped<T>, DeterministicHostError> {
        Ok(*self)
    }
}

impl<V, E, VAsc, EAsc> ToAscObj<AscResult<VAsc, EAsc>> for Result<V, E>
where
    V: ToAscObj<VAsc>,
    E: ToAscObj<EAsc>,
    VAsc: AscType,
    EAsc: AscType,
{
    fn to_asc_obj<H: AscHeap>(
        &self,
        heap: &mut H,
    ) -> Result<AscResult<VAsc, EAsc>, DeterministicHostError> {
        Ok(match self {
            Ok(value) => AscResult {
                value: {
                    let inner = heap.asc_new(value)?;
                    let wrapped = AscWrapped { inner };
                    heap.asc_new(&wrapped)?
                },
                error: AscPtr::null(),
            },
            Err(e) => AscResult {
                value: AscPtr::null(),
                error: {
                    let inner = heap.asc_new(e)?;
                    let wrapped = AscWrapped { inner };
                    heap.asc_new(&wrapped)?
                },
            },
        })
    }
}
