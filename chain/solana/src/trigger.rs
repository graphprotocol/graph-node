use graph::blockchain;
use graph::blockchain::Block;
use graph::blockchain::TriggerData;
use graph::cheap_clone::CheapClone;
use graph::prelude::hex;
use graph::prelude::web3::types::H256;
use graph::prelude::BlockNumber;
use graph::runtime::asc_new;
use graph::runtime::AscHeap;
use graph::runtime::AscPtr;
use graph::runtime::DeterministicHostError;
use std::{cmp::Ordering, sync::Arc};

use crate::codec;

// Logging the block is too verbose, so this strips the block from the trigger for Debug.
impl std::fmt::Debug for SolanaTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        pub enum MappingTriggerWithoutBlock<'a> {
            Block,
            Instruction { instruction: &'a codec::Instruction },
        }

        let trigger_without_block = match self {
            SolanaTrigger::Block(_) => MappingTriggerWithoutBlock::Block,
            SolanaTrigger::Instruction(instruction_with_block) => {
                MappingTriggerWithoutBlock::Instruction {
                    instruction: &instruction_with_block.instruction,
                }
            }
        };

        write!(f, "{:?}", trigger_without_block)
    }
}

impl blockchain::MappingTrigger for SolanaTrigger {
    fn to_asc_ptr<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError> {
        Ok(match self {
            SolanaTrigger::Block(block) => asc_new(heap, block.as_ref())?.erase(),
            SolanaTrigger::Instruction(instruction_with_block) => {
                asc_new(heap, instruction_with_block.as_ref())?.erase()
            }
        })
    }
}

#[derive(Clone)]
pub enum SolanaTrigger {
    Block(Arc<codec::Block>),
    Instruction(Arc<InstructionWithInfo>),
}

impl CheapClone for SolanaTrigger {
    fn cheap_clone(&self) -> SolanaTrigger {
        match self {
            SolanaTrigger::Block(block) => SolanaTrigger::Block(block.cheap_clone()),
            SolanaTrigger::Instruction(instruction_with_block) => {
                SolanaTrigger::Instruction(instruction_with_block.cheap_clone())
            }
        }
    }
}

impl PartialEq for SolanaTrigger {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Block(a_ptr), Self::Block(b_ptr)) => a_ptr == b_ptr,
            (Self::Instruction(a), Self::Instruction(b)) => {
                let i = &a.instruction;
                let j = &b.instruction;

                return i.program_id == j.program_id
                    && i.ordinal == j.ordinal
                    && i.parent_ordinal == j.parent_ordinal
                    && i.depth == j.depth;
            }

            (Self::Block(_), Self::Instruction(_)) | (Self::Instruction(_), Self::Block(_)) => {
                false
            }
        }
    }
}

impl Eq for SolanaTrigger {}

impl SolanaTrigger {
    pub fn block_number(&self) -> BlockNumber {
        match self {
            SolanaTrigger::Block(block) => block.number(),
            SolanaTrigger::Instruction(instruction_with_info) => instruction_with_info.number(),
        }
    }

    pub fn block_hash(&self) -> H256 {
        match self {
            SolanaTrigger::Block(block) => block.ptr().hash_as_h256(),
            SolanaTrigger::Instruction(instruction_with_block) => {
                H256::from_slice(instruction_with_block.block_id.as_slice())
            }
        }
    }
}

impl Ord for SolanaTrigger {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Keep the order when comparing two block triggers
            (Self::Block(..), Self::Block(..)) => Ordering::Equal,

            // Block triggers always come last
            (Self::Block(..), _) => Ordering::Greater,
            (_, Self::Block(..)) => Ordering::Less,

            // We assumed the provide instructions are ordered correctly, so we say they
            // are equal here and array ordering will be used.
            (Self::Instruction(..), Self::Instruction(..)) => Ordering::Equal,
        }
    }
}

impl PartialOrd for SolanaTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TriggerData for SolanaTrigger {
    fn error_context(&self) -> std::string::String {
        match self {
            SolanaTrigger::Block(..) => {
                format!("Block #{} ({})", self.block_number(), self.block_hash())
            }

            SolanaTrigger::Instruction(instruction_with_block) => {
                format!(
                    "Instruction #{} (from #{}) for program {} (Block #{} ({})",
                    instruction_with_block.instruction.ordinal,
                    instruction_with_block.instruction.parent_ordinal,
                    hex::encode(&instruction_with_block.instruction.program_id),
                    self.block_number(),
                    self.block_hash()
                )
            }
        }
    }
}

pub struct InstructionWithInfo {
    pub instruction: codec::Instruction,
    pub block_num: u64,
    pub block_id: Vec<u8>,
    pub transaction_id: Vec<u8>,
}

impl InstructionWithInfo {
    pub fn number(&self) -> BlockNumber {
        let num = self.block_num as i32;
        num.into()
    }
}
