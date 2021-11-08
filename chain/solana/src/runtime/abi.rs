use crate::codec;
use crate::trigger::InstructionWithInfo;
use graph::anyhow::anyhow;

use crate::codec::{InstructionError, MessageHeader, TransactionError};
use graph::runtime::{asc_new, AscHeap, DeterministicHostError, ToAscObj};
use graph_runtime_wasm::asc_abi::class::Array;

pub(crate) use super::generated::*;

impl ToAscObj<AscBlock> for codec::Block {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlock, DeterministicHostError> {
        Ok(AscBlock {
            id: asc_new(heap, self.id.as_slice())?,
            number: self.number,
            version: self.version,
            previous_id: asc_new(heap, self.previous_id.as_slice())?,
            previous_block: self.previous_block,
            genesis_unix_timestamp: self.genesis_unix_timestamp,
            clock_unix_timestamp: self.clock_unix_timestamp,
            root_num: self.root_num,
            last_entry_hash: asc_new(heap, self.last_entry_hash.as_slice())?,
            transactions: asc_new(heap, &self.transactions)?,
            transaction_count: self.transaction_count,
            has_split_account_changes: self.has_split_account_changes,
            account_changes_file_ref: asc_new(heap, &self.account_changes_file_ref)?,
            _padding: Padding3::new(),
        })
    }
}

impl ToAscObj<AscInstruction> for codec::Instruction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscInstruction, DeterministicHostError> {
        Ok(AscInstruction {
            program_id: asc_new(heap, self.program_id.as_slice())?,
            account_keys: asc_new(heap, &self.account_keys)?,
            data: asc_new(heap, self.data.as_slice())?,
            ordinal: self.ordinal,
            parent_ordinal: self.parent_ordinal,
            depth: self.depth,
            balance_changes: asc_new(heap, &self.balance_changes)?,
            account_changes: asc_new(heap, &self.account_changes)?,
            error: asc_new(heap, &self.error)?,
            failed: false,
            _padding: Padding3::new(),
        })
    }
}

impl ToAscObj<AscInstructionError> for Option<codec::InstructionError> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscInstructionError, DeterministicHostError> {
        match self {
            None => {
                let blank = InstructionError {
                    error: String::from(""),
                };
                Ok(blank.to_asc_obj(heap)?)
            }
            Some(e) => Ok(e.to_asc_obj(heap)?),
        }
    }
}

impl ToAscObj<AscMessageHeader> for codec::MessageHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscMessageHeader, DeterministicHostError> {
        Ok(AscMessageHeader {
            num_required_signatures: self.num_required_signatures,
            num_readonly_signed_accounts: self.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: self.num_readonly_unsigned_accounts,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscTransaction> for codec::Transaction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTransaction, DeterministicHostError> {
        Ok(AscTransaction {
            id: asc_new(heap, self.id.as_slice())?,
            index: self.index,
            additional_signatures: asc_new(heap, &self.additional_signatures)?,
            header: asc_new(heap, &self.header)?,
            account_keys: asc_new(heap, &self.account_keys)?,
            recent_blockhash: asc_new(heap, self.recent_blockhash.as_slice())?,
            log_messages: asc_new(heap, &self.log_messages)?,
            instructions: asc_new(heap, &self.instructions)?,
            failed: self.failed,
            error: asc_new(heap, &self.error)?,
            _padding: Padding7::new(),
        })
    }
}

impl ToAscObj<AscMessageHeader> for Option<codec::MessageHeader> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscMessageHeader, DeterministicHostError> {
        match self {
            None => {
                let blank = MessageHeader {
                    num_required_signatures: 0,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                };
                Ok(blank.to_asc_obj(heap)?)
            }
            Some(e) => Ok(e.to_asc_obj(heap)?),
        }
    }
}

impl ToAscObj<AscTransactionError> for Option<codec::TransactionError> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTransactionError, DeterministicHostError> {
        match self {
            None => {
                let blank = TransactionError {
                    error: String::from(""),
                };
                Ok(blank.to_asc_obj(heap)?)
            }
            Some(e) => Ok(e.to_asc_obj(heap)?),
        }
    }
}

impl ToAscObj<AscTransactionArray> for Vec<codec::Transaction> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTransactionArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscTransactionArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscTransactionError> for codec::TransactionError {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTransactionError, DeterministicHostError> {
        Ok(AscTransactionError {
            error: asc_new(heap, &self.error)?,
        })
    }
}

impl ToAscObj<AscInstructionError> for codec::InstructionError {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscInstructionError, DeterministicHostError> {
        Ok(AscInstructionError {
            error: asc_new(heap, &self.error)?,
        })
    }
}

impl ToAscObj<AscBalanceChange> for codec::BalanceChange {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBalanceChange, DeterministicHostError> {
        Ok(AscBalanceChange {
            prev_lamports: self.prev_lamports,
            new_lamports: self.new_lamports,
            pub_key: asc_new(heap, self.pubkey.as_slice())?,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscBalanceChangeArray> for Vec<codec::BalanceChange> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBalanceChangeArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscBalanceChangeArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscAccountChange> for codec::AccountChange {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscAccountChange, DeterministicHostError> {
        Ok(AscAccountChange {
            pub_key: asc_new(heap, self.pubkey.as_slice())?,
            prev_data: asc_new(heap, self.prev_data.as_slice())?,
            new_data: asc_new(heap, self.new_data.as_slice())?,
            new_data_length: self.new_data_length,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscAccountChangeArray> for Vec<codec::AccountChange> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscAccountChangeArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscAccountChangeArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscInstructionArray> for Vec<codec::Instruction> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscInstructionArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscInstructionArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscInstructionWithInfo> for InstructionWithInfo {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscInstructionWithInfo, DeterministicHostError> {
        Ok(AscInstructionWithInfo {
            block_num: self.block_num,
            instruction: asc_new(heap, &self.instruction)?,
            block_id: asc_new(heap, self.block_id.as_slice())?,
            transaction_id: asc_new(heap, self.transaction_id.as_slice())?,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscHashArray> for Vec<Vec<u8>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscHashArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self
            .iter()
            .map(|x| {
                asc_new(heap, x.as_slice())
                // asc_new(heap, slice.as_ref())?
            })
            .collect();
        let content = content?;
        Ok(AscHashArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscStringArray> for Vec<String> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscStringArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self
            .iter()
            .map(|x| {
                asc_new(heap, x)
                // asc_new(heap, slice.as_ref())?
            })
            .collect();
        let content = content?;
        Ok(AscStringArray(Array::new(&*content, heap)?))
    }
}

#[test]
fn block_to_asc_ptr() {
    use graph::data::subgraph::API_VERSION_0_0_5;
    use graph::prelude::hex;

    let mut heap = BytesHeap::new(API_VERSION_0_0_5);
    let block = codec::Block {
        id: hex::decode("01").expect(format!("Invalid hash value").as_ref()),
        number: 0,
        version: 0,
        previous_id: vec![],
        previous_block: 0,
        genesis_unix_timestamp: 0,
        clock_unix_timestamp: 0,
        root_num: 0,
        last_entry_hash: vec![],
        transactions: vec![],
        transaction_count: 0,
        has_split_account_changes: false,
        account_changes_file_ref: "".to_string(),
    };

    let asc_block = asc_new(&mut heap, &block);
    assert!(asc_block.is_ok());
}

#[test]
fn transaction_to_asc_ptr() {
    use graph::data::subgraph::API_VERSION_0_0_5;
    use graph::prelude::hex;

    let mut heap = BytesHeap::new(API_VERSION_0_0_5);
    let transaction = codec::Transaction {
        id: vec![],
        index: 0,
        additional_signatures: vec![],
        header: None,
        account_keys: vec![],
        recent_blockhash: vec![],
        log_messages: vec![],
        instructions: vec![],
        failed: false,
        error: None,
    };

    let asc_transaction = asc_new(&mut heap, &transaction);
    assert!(asc_transaction.is_ok());
}

#[test]
fn instruction_to_asc_ptr() {
    use graph::data::subgraph::API_VERSION_0_0_5;
    use graph::prelude::hex;
    use prost::Message;

    let balance_change_1 = codec::BalanceChange {
        pubkey: hex::decode("01").expect(format!("Invalid hash value").as_ref()),
        prev_lamports: 1,
        new_lamports: 2,
    };

    let account_change_1 = codec::AccountChange {
        pubkey: hex::decode("01").expect(format!("Invalid hash value").as_ref()),
        prev_data: hex::decode("02").expect(format!("Invalid hash value").as_ref()),
        new_data: hex::decode("03").expect(format!("Invalid hash value").as_ref()),
        new_data_length: 1,
    };

    let mut heap = BytesHeap::new(API_VERSION_0_0_5);
    let instruction = codec::Instruction {
        program_id: hex::decode("01").expect(format!("Invalid hash value").as_ref()),
        account_keys: vec![
            hex::decode("01").unwrap().encode_to_vec(),
            hex::decode("02").unwrap().encode_to_vec(),
        ],
        data: hex::decode("01").expect(format!("Invalid hash value").as_ref()),
        ordinal: 1,
        parent_ordinal: 2,
        depth: 42,
        balance_changes: vec![balance_change_1],
        account_changes: vec![account_change_1],
        failed: false,
        error: None,
    };

    let asc_instruction = asc_new(&mut heap, &instruction);
    assert!(asc_instruction.is_ok());
}

struct BytesHeap {
    api_version: graph::semver::Version,
    memory: Vec<u8>,
}

#[test]
impl BytesHeap {
    fn new(api_version: graph::semver::Version) -> Self {
        Self {
            api_version,
            memory: vec![],
        }
    }
}

impl AscHeap for BytesHeap {
    fn raw_new(&mut self, bytes: &[u8]) -> Result<u32, DeterministicHostError> {
        self.memory.extend_from_slice(bytes);
        Ok((self.memory.len() - bytes.len()) as u32)
    }

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, DeterministicHostError> {
        let memory_byte_count = self.memory.len();
        if memory_byte_count == 0 {
            return Err(DeterministicHostError::from(anyhow!(
                "No memory is allocated"
            )));
        }

        let start_offset = offset as usize;
        let end_offset_exclusive = start_offset + size as usize;

        if start_offset >= memory_byte_count {
            return Err(DeterministicHostError::from(anyhow!(
                "Start offset {} is outside of allocated memory, max offset is {}",
                start_offset,
                memory_byte_count - 1
            )));
        }

        if end_offset_exclusive > memory_byte_count {
            return Err(DeterministicHostError::from(anyhow!(
                "End of offset {} is outside of allocated memory, max offset is {}",
                end_offset_exclusive,
                memory_byte_count - 1
            )));
        }

        return Ok(Vec::from(&self.memory[start_offset..end_offset_exclusive]));
    }

    fn api_version(&self) -> graph::semver::Version {
        self.api_version.clone()
    }

    fn asc_type_id(
        &mut self,
        type_id_index: graph::runtime::IndexForAscTypeId,
    ) -> Result<u32, DeterministicHostError> {
        // Not totally clear what is the purpose of this method, why not a default implementation here?
        Ok(type_id_index as u32)
    }
}
