// use diesel::dsl::Asc;
use graph::runtime::{AscIndexId, AscPtr, AscType, DeterministicHostError, IndexForAscTypeId};
use graph::semver::Version;
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{Array, AscString, Uint8Array};

pub(crate) type AscHash = Uint8Array;

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBlock {
    pub number: u64,
    pub previous_block: u64,
    pub genesis_unix_timestamp: u64,
    pub clock_unix_timestamp: u64,
    pub root_num: u64,
    pub transaction_count: u32,
    pub version: u32,
    pub id: AscPtr<Uint8Array>,
    pub previous_id: AscPtr<Uint8Array>,
    pub last_entry_hash: AscPtr<Uint8Array>,
    pub transactions: AscPtr<AscTransactionArray>,
    pub account_changes_file_ref: AscPtr<AscString>,
    pub has_split_account_changes: bool,
    pub _padding: Padding3,
}

impl AscIndexId for AscBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscInstruction {
    pub ordinal: u32,
    pub parent_ordinal: u32,
    pub depth: u32,
    pub program_id: AscPtr<AscHash>,
    pub account_keys: AscPtr<AscHashArray>,
    pub data: AscPtr<AscHash>,
    pub balance_changes: AscPtr<AscBalanceChangeArray>,
    pub account_changes: AscPtr<AscAccountChangeArray>,
    pub error: AscPtr<AscInstructionError>,
    pub failed: bool,
    pub _padding: Padding3,
}

// special type for the rare occurance where we need to pad three bytes
pub(crate) struct Padding3([u8; 3]);

impl Padding3 {
    pub fn new() -> Self {
        Padding3 { 0: [0, 0, 0] }
    }
}

impl AscType for Padding3 {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        Ok(vec![0, 0, 0])
    }

    fn from_asc_bytes(
        _asc_obj: &[u8],
        _api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Padding3 { 0: [0, 0, 0] })
    }
}

// special type for the rare occurance where we need to pad three bytes
pub(crate) struct Padding7([u8; 7]);

impl Padding7 {
    pub fn new() -> Self {
        Padding7 {
            0: [0, 0, 0, 0, 0, 0, 0],
        }
    }
}

impl AscType for Padding7 {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        Ok(vec![0, 0, 0, 0, 0, 0, 0])
    }

    fn from_asc_bytes(
        _asc_obj: &[u8],
        _api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Padding7 {
            0: [0, 0, 0, 0, 0, 0, 0],
        })
    }
}

impl AscIndexId for AscInstruction {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaInstruction;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscInstructionWithInfo {
    pub block_num: u64,
    pub instruction: AscPtr<AscInstruction>,
    pub block_id: AscPtr<Uint8Array>,
    pub transaction_id: AscPtr<Uint8Array>,
    pub _padding: u32,
}

impl AscIndexId for AscInstructionWithInfo {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaInstruction;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTransaction {
    pub index: u64,
    pub id: AscPtr<Uint8Array>,
    pub additional_signatures: AscPtr<AscHashArray>,
    pub header: AscPtr<AscMessageHeader>,
    pub account_keys: AscPtr<AscHashArray>,
    pub recent_blockhash: AscPtr<Uint8Array>,
    pub log_messages: AscPtr<AscStringArray>,
    pub instructions: AscPtr<AscInstructionArray>,
    pub error: AscPtr<AscTransactionError>,
    pub failed: bool,
    pub _padding: Padding7,
}

impl AscIndexId for AscTransaction {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaTransaction;
}

pub struct AscTransactionArray(pub(crate) Array<AscPtr<AscTransaction>>);

impl AscType for AscTransactionArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscTransactionArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaArrayTransaction;
}

pub struct AscInstructionArray(pub(crate) Array<AscPtr<AscInstruction>>);

impl AscType for AscInstructionArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscInstructionArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaArrayInstruction;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscMessageHeader {
    pub num_required_signatures: u32,
    pub num_readonly_signed_accounts: u32,
    pub num_readonly_unsigned_accounts: u32,
    pub _padding: u32,
}

impl AscIndexId for AscMessageHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaMessageHeader;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTransactionError {
    pub error: AscPtr<AscString>,
}

impl AscIndexId for AscTransactionError {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaTransactionError;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscInstructionError {
    pub error: AscPtr<AscString>,
}

impl AscIndexId for AscInstructionError {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaInstructionError;
}

pub struct AscHashArray(pub(crate) Array<AscPtr<Uint8Array>>);

impl AscType for AscHashArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscStringArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaArrayHash;
}

pub struct AscStringArray(pub(crate) Array<AscPtr<AscString>>);

impl AscType for AscStringArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscHashArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaArrayString;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBalanceChange {
    pub prev_lamports: u64,
    pub new_lamports: u64,
    pub pub_key: AscPtr<Uint8Array>,
    pub _padding: u32,
}

impl AscIndexId for AscBalanceChange {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaBalanceChange;
}

pub struct AscBalanceChangeArray(pub(crate) Array<AscPtr<AscBalanceChange>>);

impl AscType for AscBalanceChangeArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscBalanceChangeArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaBalanceChangeArray;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscAccountChange {
    pub pub_key: AscPtr<Uint8Array>,
    pub prev_data: AscPtr<Uint8Array>,
    pub new_data: AscPtr<Uint8Array>,
    pub _padding: u32, // in order to put next field on 8-byte boundary
    pub new_data_length: u64,
}

impl AscIndexId for AscAccountChange {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaAccountChange;
}

pub struct AscAccountChangeArray(pub(crate) Array<AscPtr<AscAccountChange>>);

impl AscType for AscAccountChangeArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscAccountChangeArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SolanaAccountChangeArray;
}
