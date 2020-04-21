use graph::prelude::ethabi;
use graph_runtime_wasm::{AscEnum, AscHeap, AscPtr, AscType, AscValue, FromAscObj, ToAscObj};

#[repr(u32)]
#[derive(AscType, Copy, Clone)]
pub(crate) enum EthereumValueKind {
    Address,
    FixedBytes,
    Bytes,
    Int,
    Uint,
    Bool,
    String,
    FixedArray,
    Array,
    Tuple,
}

impl EthereumValueKind {
    pub(crate) fn get_kind(token: &ethabi::Token) -> Self {
        match token {
            ethabi::Token::Address(_) => EthereumValueKind::Address,
            ethabi::Token::FixedBytes(_) => EthereumValueKind::FixedBytes,
            ethabi::Token::Bytes(_) => EthereumValueKind::Bytes,
            ethabi::Token::Int(_) => EthereumValueKind::Int,
            ethabi::Token::Uint(_) => EthereumValueKind::Uint,
            ethabi::Token::Bool(_) => EthereumValueKind::Bool,
            ethabi::Token::String(_) => EthereumValueKind::String,
            ethabi::Token::FixedArray(_) => EthereumValueKind::FixedArray,
            ethabi::Token::Array(_) => EthereumValueKind::Array,
            ethabi::Token::Tuple(_) => EthereumValueKind::Tuple,
        }
    }
}

impl Default for EthereumValueKind {
    fn default() -> Self {
        EthereumValueKind::Address
    }
}

impl AscValue for EthereumValueKind {}

impl ToAscObj<AscEnum<EthereumValueKind>> for ethabi::Token {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEnum<EthereumValueKind> {
        use Token::*;

        let kind = EthereumValueKind::get_kind(self);
        let payload = match self {
            Address(address) => heap.asc_new::<AscAddress, _>(address).to_payload(),
            FixedBytes(bytes) | Bytes(bytes) => {
                heap.asc_new::<Uint8Array, _>(&**bytes).to_payload()
            }
            Int(uint) => {
                let n = BigInt::from_signed_u256(&uint);
                heap.asc_new(&n).to_payload()
            }
            Uint(uint) => {
                let n = BigInt::from_unsigned_u256(&uint);
                heap.asc_new(&n).to_payload()
            }
            Bool(b) => *b as u64,
            String(string) => heap.asc_new(&**string).to_payload(),
            FixedArray(tokens) | Array(tokens) => heap.asc_new(&**tokens).to_payload(),
            Tuple(tokens) => heap.asc_new(&**tokens).to_payload(),
        };

        AscEnum {
            kind,
            _padding: 0,
            payload: EnumPayload(payload),
        }
    }
}

impl FromAscObj<AscEnum<EthereumValueKind>> for Token {
    fn from_asc_obj<H: AscHeap>(asc_enum: AscEnum<EthereumValueKind>, heap: &H) -> Self {
        use Token;

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
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = heap.asc_get(ptr);
                Token::Int(n.to_signed_u256())
            }
            EthereumValueKind::Uint => {
                let ptr: AscPtr<AscBigInt> = AscPtr::from(payload);
                let n: BigInt = heap.asc_get(ptr);
                Token::Uint(n.to_unsigned_u256())
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
            EthereumValueKind::Tuple => {
                let ptr: AscEnumArray<EthereumValueKind> = AscPtr::from(payload);
                Token::Tuple(heap.asc_get(ptr))
            }
        }
    }
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscLogParam {
    pub name: AscPtr<AscString>,
    pub value: AscPtr<AscEnum<EthereumValueKind>>,
}

impl ToAscObj<AscLogParam> for LogParam {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscLogParam {
        AscLogParam {
            name: heap.asc_new(self.name.as_str()),
            value: heap.asc_new(&self.value),
        }
    }
}

pub(crate) type AscLogParamArray = Array<AscPtr<AscLogParam>>;

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumBlock {
    pub hash: AscPtr<AscH256>,
    pub parent_hash: AscPtr<AscH256>,
    pub uncles_hash: AscPtr<AscH256>,
    pub author: AscPtr<AscH160>,
    pub state_root: AscPtr<AscH256>,
    pub transactions_root: AscPtr<AscH256>,
    pub receipts_root: AscPtr<AscH256>,
    pub number: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_limit: AscPtr<AscBigInt>,
    pub timestamp: AscPtr<AscBigInt>,
    pub difficulty: AscPtr<AscBigInt>,
    pub total_difficulty: AscPtr<AscBigInt>,
    pub size: AscPtr<AscBigInt>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumTransaction {
    pub hash: AscPtr<AscH256>,
    pub index: AscPtr<AscBigInt>,
    pub from: AscPtr<AscH160>,
    pub to: AscPtr<AscH160>,
    pub value: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_price: AscPtr<AscBigInt>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumTransaction_0_0_2 {
    pub hash: AscPtr<AscH256>,
    pub index: AscPtr<AscBigInt>,
    pub from: AscPtr<AscH160>,
    pub to: AscPtr<AscH160>,
    pub value: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_price: AscPtr<AscBigInt>,
    pub input: AscPtr<Bytes>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumEvent<T>
where
    T: AscType,
{
    pub address: AscPtr<AscAddress>,
    pub log_index: AscPtr<AscBigInt>,
    pub transaction_log_index: AscPtr<AscBigInt>,
    pub log_type: AscPtr<AscString>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<T>,
    pub params: AscPtr<AscLogParamArray>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumCall {
    pub address: AscPtr<AscAddress>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<AscEthereumTransaction>,
    pub inputs: AscPtr<AscLogParamArray>,
    pub outputs: AscPtr<AscLogParamArray>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumCall_0_0_3 {
    pub to: AscPtr<AscAddress>,
    pub from: AscPtr<AscAddress>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<AscEthereumTransaction>,
    pub inputs: AscPtr<AscLogParamArray>,
    pub outputs: AscPtr<AscLogParamArray>,
}

impl ToAscObj<AscEthereumBlock> for EthereumBlockData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEthereumBlock {
        AscEthereumBlock {
            hash: heap.asc_new(&self.hash),
            parent_hash: heap.asc_new(&self.parent_hash),
            uncles_hash: heap.asc_new(&self.uncles_hash),
            author: heap.asc_new(&self.author),
            state_root: heap.asc_new(&self.state_root),
            transactions_root: heap.asc_new(&self.transactions_root),
            receipts_root: heap.asc_new(&self.receipts_root),
            number: heap.asc_new(&BigInt::from(self.number)),
            gas_used: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_used)),
            gas_limit: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_limit)),
            timestamp: heap.asc_new(&BigInt::from_unsigned_u256(&self.timestamp)),
            difficulty: heap.asc_new(&BigInt::from_unsigned_u256(&self.difficulty)),
            total_difficulty: heap.asc_new(&BigInt::from_unsigned_u256(&self.total_difficulty)),
            size: self
                .size
                .map(|size| heap.asc_new(&BigInt::from_unsigned_u256(&size)))
                .unwrap_or_else(|| AscPtr::null()),
        }
    }
}

impl ToAscObj<AscEthereumTransaction> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEthereumTransaction {
        AscEthereumTransaction {
            hash: heap.asc_new(&self.hash),
            index: heap.asc_new(&BigInt::from(self.index)),
            from: heap.asc_new(&self.from),
            to: self
                .to
                .map(|to| heap.asc_new(&to))
                .unwrap_or_else(|| AscPtr::null()),
            value: heap.asc_new(&BigInt::from_unsigned_u256(&self.value)),
            gas_used: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_used)),
            gas_price: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_price)),
        }
    }
}

impl ToAscObj<AscEthereumTransaction_0_0_2> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEthereumTransaction_0_0_2 {
        AscEthereumTransaction_0_0_2 {
            hash: heap.asc_new(&self.hash),
            index: heap.asc_new(&BigInt::from(self.index)),
            from: heap.asc_new(&self.from),
            to: self
                .to
                .map(|to| heap.asc_new(&to))
                .unwrap_or_else(|| AscPtr::null()),
            value: heap.asc_new(&BigInt::from_unsigned_u256(&self.value)),
            gas_used: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_used)),
            gas_price: heap.asc_new(&BigInt::from_unsigned_u256(&self.gas_price)),
            input: heap.asc_new(&*self.input.0),
        }
    }
}

impl<T: AscType> ToAscObj<AscEthereumEvent<T>> for EthereumEventData
where
    EthereumTransactionData: ToAscObj<T>,
{
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEthereumEvent<T> {
        AscEthereumEvent {
            address: heap.asc_new(&self.address),
            log_index: heap.asc_new(&BigInt::from_unsigned_u256(&self.log_index)),
            transaction_log_index: heap
                .asc_new(&BigInt::from_unsigned_u256(&self.transaction_log_index)),
            log_type: self
                .log_type
                .clone()
                .map(|log_type| heap.asc_new(&log_type))
                .unwrap_or_else(|| AscPtr::null()),
            block: heap.asc_new(&self.block),
            transaction: heap.asc_new::<T, EthereumTransactionData>(&self.transaction),
            params: heap.asc_new(self.params.as_slice()),
        }
    }
}

impl ToAscObj<AscEthereumCall> for EthereumCallData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEthereumCall {
        AscEthereumCall {
            address: heap.asc_new(&self.to),
            block: heap.asc_new(&self.block),
            transaction: heap.asc_new(&self.transaction),
            inputs: heap.asc_new(self.inputs.as_slice()),
            outputs: heap.asc_new(self.outputs.as_slice()),
        }
    }
}

impl ToAscObj<AscEthereumCall_0_0_3> for EthereumCallData {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> AscEthereumCall_0_0_3 {
        AscEthereumCall_0_0_3 {
            to: heap.asc_new(&self.to),
            from: heap.asc_new(&self.from),
            block: heap.asc_new(&self.block),
            transaction: heap.asc_new(&self.transaction),
            inputs: heap.asc_new(self.inputs.as_slice()),
            outputs: heap.asc_new(self.outputs.as_slice()),
        }
    }
}

pub struct UnresolvedContractCall {
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_signature: Option<String>,
    pub function_args: Vec<Token>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscUnresolvedContractCall {
    pub contract_name: AscPtr<AscString>,
    pub contract_address: AscPtr<AscAddress>,
    pub function_name: AscPtr<AscString>,
    pub function_args: AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>,
}

impl FromAscObj<AscUnresolvedContractCall> for UnresolvedContractCall {
    fn from_asc_obj<H: AscHeap>(asc_call: AscUnresolvedContractCall, heap: &H) -> Self {
        UnresolvedContractCall {
            contract_name: heap.asc_get(asc_call.contract_name),
            contract_address: heap.asc_get(asc_call.contract_address),
            function_name: heap.asc_get(asc_call.function_name),
            function_signature: None,
            function_args: heap.asc_get(asc_call.function_args),
        }
    }
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscUnresolvedContractCall_0_0_4 {
    pub contract_name: AscPtr<AscString>,
    pub contract_address: AscPtr<AscAddress>,
    pub function_name: AscPtr<AscString>,
    pub function_signature: AscPtr<AscString>,
    pub function_args: AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>,
}

impl FromAscObj<AscUnresolvedContractCall_0_0_4> for UnresolvedContractCall {
    fn from_asc_obj<H: AscHeap>(asc_call: AscUnresolvedContractCall_0_0_4, heap: &H) -> Self {
        UnresolvedContractCall {
            contract_name: heap.asc_get(asc_call.contract_name),
            contract_address: heap.asc_get(asc_call.contract_address),
            function_name: heap.asc_get(asc_call.function_name),
            function_signature: Some(heap.asc_get(asc_call.function_signature)),
            function_args: heap.asc_get(asc_call.function_args),
        }
    }
}
