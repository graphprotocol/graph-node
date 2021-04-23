//! Stores all the gas costs is one place so they can be compared easily.
//! Determinism: Once deployed, none of these values can be changed without a version upgrade.

use super::*;

/// Using 10 gas = ~1ns for WASM instructions.
const GAS_PER_SECOND: u64 = 10_000_000_000;

/// Set max gas to 1 hour worth of gas per handler. The intent here is to have the determinism
/// cutoff be very high, while still allowing more reasonable timer based cutoffs. Having a unit
/// like 10 gas for ~1ns allows us to be granular in instructions which are aggregated into metered
/// blocks via https://docs.rs/pwasm-utils/0.16.0/pwasm_utils/fn.inject_gas_counter.html But we can
/// still charge very high numbers for other things.
pub const MAX_GAS_PER_HANDLER: u64 = 3600 * GAS_PER_SECOND;

/// Base gas cost for calling any host export.
/// Security: This must be non-zero.
const HOST_EXPORT: u64 = 100_000;

/// Gas for instructions are aggregated into blocks, so hopefully gas calls each have relatively large gas.
/// But in the case they don't, we don't want the overhead of calling out into a host export to be
/// the dominant cost that causes unexpectedly high execution times.
pub const HOST_EXPORT_GAS: Gas = Gas(HOST_EXPORT);

/// As a heuristic for the cost of host fns it makes sense to reason in terms of bandwidth and
/// calculate the cost from there. Because we don't have benchmarks for each host fn, we go with
/// pessimistic assumption of performance of 10 MB/s, which nonetheless allows for 36 GB to be
/// processed through host exports by a single handler at a 1 hour budget.
const DEFAULT_BYTE_PER_SECOND: u64 = 10_000_000;

/// With the current parameters DEFAULT_GAS_PER_BYTE = 1_000.
const DEFAULT_GAS_PER_BYTE: u64 = GAS_PER_SECOND / DEFAULT_BYTE_PER_SECOND;

pub(crate) const DEFAULT_BASE_COST: u64 = 100_000;

pub(crate) const DEFAULT_GAS_OP: GasOp = GasOp {
    base_cost: DEFAULT_BASE_COST,
    size_mult: DEFAULT_GAS_PER_BYTE,
};

// Allow up to 25,000 ethereum calls
pub const ETHEREUM_CALL: Gas = Gas(MAX_GAS_PER_HANDLER / 25_000);

// Allow up to 100,000 data sources to be created
pub const CREATE_DATA_SOURCE: Gas = Gas(MAX_GAS_PER_HANDLER / 100_000);

// Allow up to 100,000 logs
pub const LOG: Gas = Gas(MAX_GAS_PER_HANDLER / 100_000);

// Saving to the store is one of the most expensive operations.
pub const STORE_SET: GasOp = GasOp {
    // Allow up to 250k entities saved.
    base_cost: MAX_GAS_PER_HANDLER / 250_000,
    // If the size roughly corresponds to bytes, allow 1GB to be saved.
    size_mult: MAX_GAS_PER_HANDLER / 1_000_000_000,
};

// Reading from the store is much cheaper than writing.
pub const STORE_GET: GasOp = GasOp {
    base_cost: MAX_GAS_PER_HANDLER / 10_000_000,
    size_mult: MAX_GAS_PER_HANDLER / 10_000_000_000,
};

pub const STORE_REMOVE: GasOp = STORE_SET;

pub struct GasRules;

impl Rules for GasRules {
    fn instruction_cost(&self, instruction: &Instruction) -> Option<u32> {
        use Instruction::*;
        let weight = match instruction {
            // These are taken from this post: https://github.com/paritytech/substrate/pull/7361#issue-506217103
            // from the table under the "Schedule" dropdown. Each decimal is multiplied by 10.
            I64Const(_) => 16,
            I64Load(_, _) => 1573,
            I64Store(_, _) => 2263,
            Select => 61,
            Instruction::If(_) => 79,
            Br(_) => 30,
            BrIf(_) => 63,
            BrTable(data) => 146 + (1030000 * (1 + data.table.len() as u32)),
            Call(_) => 951,
            // TODO: To figure out the param cost we need to look up the function
            CallIndirect(_, _) => 1995,
            GetLocal(_) => 18,
            SetLocal(_) => 21,
            TeeLocal(_) => 21,
            GetGlobal(_) => 66,
            SetGlobal(_) => 107,
            CurrentMemory(_) => 23,
            GrowMemory(_) => 435000,
            I64Clz => 23,
            I64Ctz => 23,
            I64Popcnt => 29,
            I64Eqz => 24,
            I64ExtendSI32 => 22,
            I64ExtendUI32 => 22,
            I32WrapI64 => 23,
            I64Eq => 26,
            I64Ne => 25,
            I64LtS => 25,
            I64LtU => 26,
            I64GtS => 25,
            I64GtU => 25,
            I64LeS => 25,
            I64LeU => 26,
            I64GeS => 26,
            I64GeU => 25,
            I64Add => 25,
            I64Sub => 26,
            I64Mul => 25,
            I64DivS => 82,
            I64DivU => 72,
            I64RemS => 81,
            I64RemU => 73,
            I64And => 25,
            I64Or => 25,
            I64Xor => 26,
            I64Shl => 25,
            I64ShrS => 26,
            I64ShrU => 26,
            I64Rotl => 25,
            I64Rotr => 26,

            // These are similar enough to something above so just referencing a similar
            // instruction
            I32Load(_, _)
            | F32Load(_, _)
            | F64Load(_, _)
            | I32Load8S(_, _)
            | I32Load8U(_, _)
            | I32Load16S(_, _)
            | I32Load16U(_, _)
            | I64Load8S(_, _)
            | I64Load8U(_, _)
            | I64Load16S(_, _)
            | I64Load16U(_, _)
            | I64Load32S(_, _)
            | I64Load32U(_, _) => 1573,

            I32Store(_, _)
            | F32Store(_, _)
            | F64Store(_, _)
            | I32Store8(_, _)
            | I32Store16(_, _)
            | I64Store8(_, _)
            | I64Store16(_, _)
            | I64Store32(_, _) => 2263,

            I32Const(_) | F32Const(_) | F64Const(_) => 16,
            I32Eqz => 26,
            I32Eq => 26,
            I32Ne => 25,
            I32LtS => 25,
            I32LtU => 26,
            I32GtS => 25,
            I32GtU => 25,
            I32LeS => 25,
            I32LeU => 26,
            I32GeS => 26,
            I32GeU => 25,
            I32Add => 25,
            I32Sub => 26,
            I32Mul => 25,
            I32DivS => 82,
            I32DivU => 72,
            I32RemS => 81,
            I32RemU => 73,
            I32And => 25,
            I32Or => 25,
            I32Xor => 26,
            I32Shl => 25,
            I32ShrS => 26,
            I32ShrU => 26,
            I32Rotl => 25,
            I32Rotr => 26,
            I32Clz => 23,
            I32Popcnt => 29,
            I32Ctz => 23,

            // Float weights not calculated by reference source material. Making up
            // some conservative values. The point here is not to be perfect but just
            // to have some reasonable upper bound.
            F64ReinterpretI64 | F32ReinterpretI32 | F64PromoteF32 | F64ConvertUI64
            | F64ConvertSI64 | F64ConvertUI32 | F64ConvertSI32 | F32DemoteF64 | F32ConvertUI64
            | F32ConvertSI64 | F32ConvertUI32 | F32ConvertSI32 | I64TruncUF64 | I64TruncSF64
            | I64TruncUF32 | I64TruncSF32 | I32TruncUF64 | I32TruncSF64 | I32TruncUF32
            | I32TruncSF32 | F64Copysign | F64Max | F64Min | F64Mul | F64Sub | F64Add
            | F64Trunc | F64Floor | F64Ceil | F64Neg | F64Abs | F64Nearest | F32Copysign
            | F32Max | F32Min | F32Mul | F32Sub | F32Add | F32Nearest | F32Trunc | F32Floor
            | F32Ceil | F32Neg | F32Abs | F32Eq | F32Ne | F32Lt | F32Gt | F32Le | F32Ge | F64Eq
            | F64Ne | F64Lt | F64Gt | F64Le | F64Ge | I32ReinterpretF32 | I64ReinterpretF64 => 100,
            F64Div | F64Sqrt | F32Div | F32Sqrt => 1000,

            // More invented weights
            Block(_) => 1000,
            Loop(_) => 1000,
            Else => 1000,
            End => 1000,
            Return => 1000,
            Drop => 1000,
            Nop => 1,
            Unreachable => 1,
        };
        Some(weight)
    }

    fn memory_grow_cost(&self) -> Option<MemoryGrowCost> {
        // Each page is 64KiB which is 65536 bytes.
        const PAGE: u64 = 64 * 1024;
        // 1 GB
        const GIB: u64 = 1073741824;
        // 12GiB to pages for the max memory allocation
        // In practice this will never be hit unless we also
        // free pages because this is 32bit WASM.
        const MAX_PAGES: u64 = 12 * GIB / PAGE;
        // This ends up at 439,453,125 per page.
        const GAS_PER_PAGE: u64 = MAX_GAS_PER_HANDLER / MAX_PAGES;
        let gas_per_page = NonZeroU32::new(GAS_PER_PAGE.try_into().unwrap()).unwrap();

        Some(MemoryGrowCost::Linear(gas_per_page))
    }
}
