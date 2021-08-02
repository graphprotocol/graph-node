/// An instruction format
///
/// Every opcode has a corresponding instruction format
/// which is represented by both the `InstructionFormat`
/// and the `InstructionData` enums.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum InstructionFormat {
    /// AtomicCas(imms=(flags: ir::MemFlags), vals=3)
    AtomicCas,
    /// AtomicRmw(imms=(flags: ir::MemFlags, op: ir::AtomicRmwOp), vals=2)
    AtomicRmw,
    /// Binary(imms=(), vals=2)
    Binary,
    /// BinaryImm64(imms=(imm: ir::immediates::Imm64), vals=1)
    BinaryImm64,
    /// BinaryImm8(imms=(imm: ir::immediates::Uimm8), vals=1)
    BinaryImm8,
    /// Branch(imms=(destination: ir::Block), vals=1)
    Branch,
    /// BranchFloat(imms=(cond: ir::condcodes::FloatCC, destination: ir::Block), vals=1)
    BranchFloat,
    /// BranchIcmp(imms=(cond: ir::condcodes::IntCC, destination: ir::Block), vals=2)
    BranchIcmp,
    /// BranchInt(imms=(cond: ir::condcodes::IntCC, destination: ir::Block), vals=1)
    BranchInt,
    /// BranchTable(imms=(destination: ir::Block, table: ir::JumpTable), vals=1)
    BranchTable,
    /// BranchTableBase(imms=(table: ir::JumpTable), vals=0)
    BranchTableBase,
    /// BranchTableEntry(imms=(imm: ir::immediates::Uimm8, table: ir::JumpTable), vals=2)
    BranchTableEntry,
    /// Call(imms=(func_ref: ir::FuncRef), vals=0)
    Call,
    /// CallIndirect(imms=(sig_ref: ir::SigRef), vals=1)
    CallIndirect,
    /// CondTrap(imms=(code: ir::TrapCode), vals=1)
    CondTrap,
    /// CopySpecial(imms=(src: isa::RegUnit, dst: isa::RegUnit), vals=0)
    CopySpecial,
    /// CopyToSsa(imms=(src: isa::RegUnit), vals=0)
    CopyToSsa,
    /// FloatCompare(imms=(cond: ir::condcodes::FloatCC), vals=2)
    FloatCompare,
    /// FloatCond(imms=(cond: ir::condcodes::FloatCC), vals=1)
    FloatCond,
    /// FloatCondTrap(imms=(cond: ir::condcodes::FloatCC, code: ir::TrapCode), vals=1)
    FloatCondTrap,
    /// FuncAddr(imms=(func_ref: ir::FuncRef), vals=0)
    FuncAddr,
    /// HeapAddr(imms=(heap: ir::Heap, imm: ir::immediates::Uimm32), vals=1)
    HeapAddr,
    /// IndirectJump(imms=(table: ir::JumpTable), vals=1)
    IndirectJump,
    /// IntCompare(imms=(cond: ir::condcodes::IntCC), vals=2)
    IntCompare,
    /// IntCompareImm(imms=(cond: ir::condcodes::IntCC, imm: ir::immediates::Imm64), vals=1)
    IntCompareImm,
    /// IntCond(imms=(cond: ir::condcodes::IntCC), vals=1)
    IntCond,
    /// IntCondTrap(imms=(cond: ir::condcodes::IntCC, code: ir::TrapCode), vals=1)
    IntCondTrap,
    /// IntSelect(imms=(cond: ir::condcodes::IntCC), vals=3)
    IntSelect,
    /// Jump(imms=(destination: ir::Block), vals=0)
    Jump,
    /// Load(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=1)
    Load,
    /// LoadComplex(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=0)
    LoadComplex,
    /// LoadNoOffset(imms=(flags: ir::MemFlags), vals=1)
    LoadNoOffset,
    /// MultiAry(imms=(), vals=0)
    MultiAry,
    /// NullAry(imms=(), vals=0)
    NullAry,
    /// RegFill(imms=(src: ir::StackSlot, dst: isa::RegUnit), vals=1)
    RegFill,
    /// RegMove(imms=(src: isa::RegUnit, dst: isa::RegUnit), vals=1)
    RegMove,
    /// RegSpill(imms=(src: isa::RegUnit, dst: ir::StackSlot), vals=1)
    RegSpill,
    /// Shuffle(imms=(mask: ir::Immediate), vals=2)
    Shuffle,
    /// StackLoad(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=0)
    StackLoad,
    /// StackStore(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=1)
    StackStore,
    /// Store(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=2)
    Store,
    /// StoreComplex(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=1)
    StoreComplex,
    /// StoreNoOffset(imms=(flags: ir::MemFlags), vals=2)
    StoreNoOffset,
    /// TableAddr(imms=(table: ir::Table, offset: ir::immediates::Offset32), vals=1)
    TableAddr,
    /// Ternary(imms=(), vals=3)
    Ternary,
    /// TernaryImm8(imms=(imm: ir::immediates::Uimm8), vals=2)
    TernaryImm8,
    /// Trap(imms=(code: ir::TrapCode), vals=0)
    Trap,
    /// Unary(imms=(), vals=1)
    Unary,
    /// UnaryBool(imms=(imm: bool), vals=0)
    UnaryBool,
    /// UnaryConst(imms=(constant_handle: ir::Constant), vals=0)
    UnaryConst,
    /// UnaryGlobalValue(imms=(global_value: ir::GlobalValue), vals=0)
    UnaryGlobalValue,
    /// UnaryIeee32(imms=(imm: ir::immediates::Ieee32), vals=0)
    UnaryIeee32,
    /// UnaryIeee64(imms=(imm: ir::immediates::Ieee64), vals=0)
    UnaryIeee64,
    /// UnaryImm(imms=(imm: ir::immediates::Imm64), vals=0)
    UnaryImm,
}

impl<'a> From<&'a InstructionData> for InstructionFormat {
    fn from(inst: &'a InstructionData) -> Self {
        match *inst {
            InstructionData::AtomicCas { .. } => {
                Self::AtomicCas
            }
            InstructionData::AtomicRmw { .. } => {
                Self::AtomicRmw
            }
            InstructionData::Binary { .. } => {
                Self::Binary
            }
            InstructionData::BinaryImm64 { .. } => {
                Self::BinaryImm64
            }
            InstructionData::BinaryImm8 { .. } => {
                Self::BinaryImm8
            }
            InstructionData::Branch { .. } => {
                Self::Branch
            }
            InstructionData::BranchFloat { .. } => {
                Self::BranchFloat
            }
            InstructionData::BranchIcmp { .. } => {
                Self::BranchIcmp
            }
            InstructionData::BranchInt { .. } => {
                Self::BranchInt
            }
            InstructionData::BranchTable { .. } => {
                Self::BranchTable
            }
            InstructionData::BranchTableBase { .. } => {
                Self::BranchTableBase
            }
            InstructionData::BranchTableEntry { .. } => {
                Self::BranchTableEntry
            }
            InstructionData::Call { .. } => {
                Self::Call
            }
            InstructionData::CallIndirect { .. } => {
                Self::CallIndirect
            }
            InstructionData::CondTrap { .. } => {
                Self::CondTrap
            }
            InstructionData::CopySpecial { .. } => {
                Self::CopySpecial
            }
            InstructionData::CopyToSsa { .. } => {
                Self::CopyToSsa
            }
            InstructionData::FloatCompare { .. } => {
                Self::FloatCompare
            }
            InstructionData::FloatCond { .. } => {
                Self::FloatCond
            }
            InstructionData::FloatCondTrap { .. } => {
                Self::FloatCondTrap
            }
            InstructionData::FuncAddr { .. } => {
                Self::FuncAddr
            }
            InstructionData::HeapAddr { .. } => {
                Self::HeapAddr
            }
            InstructionData::IndirectJump { .. } => {
                Self::IndirectJump
            }
            InstructionData::IntCompare { .. } => {
                Self::IntCompare
            }
            InstructionData::IntCompareImm { .. } => {
                Self::IntCompareImm
            }
            InstructionData::IntCond { .. } => {
                Self::IntCond
            }
            InstructionData::IntCondTrap { .. } => {
                Self::IntCondTrap
            }
            InstructionData::IntSelect { .. } => {
                Self::IntSelect
            }
            InstructionData::Jump { .. } => {
                Self::Jump
            }
            InstructionData::Load { .. } => {
                Self::Load
            }
            InstructionData::LoadComplex { .. } => {
                Self::LoadComplex
            }
            InstructionData::LoadNoOffset { .. } => {
                Self::LoadNoOffset
            }
            InstructionData::MultiAry { .. } => {
                Self::MultiAry
            }
            InstructionData::NullAry { .. } => {
                Self::NullAry
            }
            InstructionData::RegFill { .. } => {
                Self::RegFill
            }
            InstructionData::RegMove { .. } => {
                Self::RegMove
            }
            InstructionData::RegSpill { .. } => {
                Self::RegSpill
            }
            InstructionData::Shuffle { .. } => {
                Self::Shuffle
            }
            InstructionData::StackLoad { .. } => {
                Self::StackLoad
            }
            InstructionData::StackStore { .. } => {
                Self::StackStore
            }
            InstructionData::Store { .. } => {
                Self::Store
            }
            InstructionData::StoreComplex { .. } => {
                Self::StoreComplex
            }
            InstructionData::StoreNoOffset { .. } => {
                Self::StoreNoOffset
            }
            InstructionData::TableAddr { .. } => {
                Self::TableAddr
            }
            InstructionData::Ternary { .. } => {
                Self::Ternary
            }
            InstructionData::TernaryImm8 { .. } => {
                Self::TernaryImm8
            }
            InstructionData::Trap { .. } => {
                Self::Trap
            }
            InstructionData::Unary { .. } => {
                Self::Unary
            }
            InstructionData::UnaryBool { .. } => {
                Self::UnaryBool
            }
            InstructionData::UnaryConst { .. } => {
                Self::UnaryConst
            }
            InstructionData::UnaryGlobalValue { .. } => {
                Self::UnaryGlobalValue
            }
            InstructionData::UnaryIeee32 { .. } => {
                Self::UnaryIeee32
            }
            InstructionData::UnaryIeee64 { .. } => {
                Self::UnaryIeee64
            }
            InstructionData::UnaryImm { .. } => {
                Self::UnaryImm
            }
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "enable-serde", derive(Serialize, Deserialize))]
#[allow(missing_docs)]
pub enum InstructionData {
    AtomicCas {
        opcode: Opcode,
        args: [Value; 3],
        flags: ir::MemFlags,
    },
    AtomicRmw {
        opcode: Opcode,
        args: [Value; 2],
        flags: ir::MemFlags,
        op: ir::AtomicRmwOp,
    },
    Binary {
        opcode: Opcode,
        args: [Value; 2],
    },
    BinaryImm64 {
        opcode: Opcode,
        arg: Value,
        imm: ir::immediates::Imm64,
    },
    BinaryImm8 {
        opcode: Opcode,
        arg: Value,
        imm: ir::immediates::Uimm8,
    },
    Branch {
        opcode: Opcode,
        args: ValueList,
        destination: ir::Block,
    },
    BranchFloat {
        opcode: Opcode,
        args: ValueList,
        cond: ir::condcodes::FloatCC,
        destination: ir::Block,
    },
    BranchIcmp {
        opcode: Opcode,
        args: ValueList,
        cond: ir::condcodes::IntCC,
        destination: ir::Block,
    },
    BranchInt {
        opcode: Opcode,
        args: ValueList,
        cond: ir::condcodes::IntCC,
        destination: ir::Block,
    },
    BranchTable {
        opcode: Opcode,
        arg: Value,
        destination: ir::Block,
        table: ir::JumpTable,
    },
    BranchTableBase {
        opcode: Opcode,
        table: ir::JumpTable,
    },
    BranchTableEntry {
        opcode: Opcode,
        args: [Value; 2],
        imm: ir::immediates::Uimm8,
        table: ir::JumpTable,
    },
    Call {
        opcode: Opcode,
        args: ValueList,
        func_ref: ir::FuncRef,
    },
    CallIndirect {
        opcode: Opcode,
        args: ValueList,
        sig_ref: ir::SigRef,
    },
    CondTrap {
        opcode: Opcode,
        arg: Value,
        code: ir::TrapCode,
    },
    CopySpecial {
        opcode: Opcode,
        src: isa::RegUnit,
        dst: isa::RegUnit,
    },
    CopyToSsa {
        opcode: Opcode,
        src: isa::RegUnit,
    },
    FloatCompare {
        opcode: Opcode,
        args: [Value; 2],
        cond: ir::condcodes::FloatCC,
    },
    FloatCond {
        opcode: Opcode,
        arg: Value,
        cond: ir::condcodes::FloatCC,
    },
    FloatCondTrap {
        opcode: Opcode,
        arg: Value,
        cond: ir::condcodes::FloatCC,
        code: ir::TrapCode,
    },
    FuncAddr {
        opcode: Opcode,
        func_ref: ir::FuncRef,
    },
    HeapAddr {
        opcode: Opcode,
        arg: Value,
        heap: ir::Heap,
        imm: ir::immediates::Uimm32,
    },
    IndirectJump {
        opcode: Opcode,
        arg: Value,
        table: ir::JumpTable,
    },
    IntCompare {
        opcode: Opcode,
        args: [Value; 2],
        cond: ir::condcodes::IntCC,
    },
    IntCompareImm {
        opcode: Opcode,
        arg: Value,
        cond: ir::condcodes::IntCC,
        imm: ir::immediates::Imm64,
    },
    IntCond {
        opcode: Opcode,
        arg: Value,
        cond: ir::condcodes::IntCC,
    },
    IntCondTrap {
        opcode: Opcode,
        arg: Value,
        cond: ir::condcodes::IntCC,
        code: ir::TrapCode,
    },
    IntSelect {
        opcode: Opcode,
        args: [Value; 3],
        cond: ir::condcodes::IntCC,
    },
    Jump {
        opcode: Opcode,
        args: ValueList,
        destination: ir::Block,
    },
    Load {
        opcode: Opcode,
        arg: Value,
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    LoadComplex {
        opcode: Opcode,
        args: ValueList,
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    LoadNoOffset {
        opcode: Opcode,
        arg: Value,
        flags: ir::MemFlags,
    },
    MultiAry {
        opcode: Opcode,
        args: ValueList,
    },
    NullAry {
        opcode: Opcode,
    },
    RegFill {
        opcode: Opcode,
        arg: Value,
        src: ir::StackSlot,
        dst: isa::RegUnit,
    },
    RegMove {
        opcode: Opcode,
        arg: Value,
        src: isa::RegUnit,
        dst: isa::RegUnit,
    },
    RegSpill {
        opcode: Opcode,
        arg: Value,
        src: isa::RegUnit,
        dst: ir::StackSlot,
    },
    Shuffle {
        opcode: Opcode,
        args: [Value; 2],
        mask: ir::Immediate,
    },
    StackLoad {
        opcode: Opcode,
        stack_slot: ir::StackSlot,
        offset: ir::immediates::Offset32,
    },
    StackStore {
        opcode: Opcode,
        arg: Value,
        stack_slot: ir::StackSlot,
        offset: ir::immediates::Offset32,
    },
    Store {
        opcode: Opcode,
        args: [Value; 2],
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    StoreComplex {
        opcode: Opcode,
        args: ValueList,
        flags: ir::MemFlags,
        offset: ir::immediates::Offset32,
    },
    StoreNoOffset {
        opcode: Opcode,
        args: [Value; 2],
        flags: ir::MemFlags,
    },
    TableAddr {
        opcode: Opcode,
        arg: Value,
        table: ir::Table,
        offset: ir::immediates::Offset32,
    },
    Ternary {
        opcode: Opcode,
        args: [Value; 3],
    },
    TernaryImm8 {
        opcode: Opcode,
        args: [Value; 2],
        imm: ir::immediates::Uimm8,
    },
    Trap {
        opcode: Opcode,
        code: ir::TrapCode,
    },
    Unary {
        opcode: Opcode,
        arg: Value,
    },
    UnaryBool {
        opcode: Opcode,
        imm: bool,
    },
    UnaryConst {
        opcode: Opcode,
        constant_handle: ir::Constant,
    },
    UnaryGlobalValue {
        opcode: Opcode,
        global_value: ir::GlobalValue,
    },
    UnaryIeee32 {
        opcode: Opcode,
        imm: ir::immediates::Ieee32,
    },
    UnaryIeee64 {
        opcode: Opcode,
        imm: ir::immediates::Ieee64,
    },
    UnaryImm {
        opcode: Opcode,
        imm: ir::immediates::Imm64,
    },
}

impl InstructionData {
    /// Get the opcode of this instruction.
    pub fn opcode(&self) -> Opcode {
        match *self {
            Self::AtomicCas { opcode, .. } |
            Self::AtomicRmw { opcode, .. } |
            Self::Binary { opcode, .. } |
            Self::BinaryImm64 { opcode, .. } |
            Self::BinaryImm8 { opcode, .. } |
            Self::Branch { opcode, .. } |
            Self::BranchFloat { opcode, .. } |
            Self::BranchIcmp { opcode, .. } |
            Self::BranchInt { opcode, .. } |
            Self::BranchTable { opcode, .. } |
            Self::BranchTableBase { opcode, .. } |
            Self::BranchTableEntry { opcode, .. } |
            Self::Call { opcode, .. } |
            Self::CallIndirect { opcode, .. } |
            Self::CondTrap { opcode, .. } |
            Self::CopySpecial { opcode, .. } |
            Self::CopyToSsa { opcode, .. } |
            Self::FloatCompare { opcode, .. } |
            Self::FloatCond { opcode, .. } |
            Self::FloatCondTrap { opcode, .. } |
            Self::FuncAddr { opcode, .. } |
            Self::HeapAddr { opcode, .. } |
            Self::IndirectJump { opcode, .. } |
            Self::IntCompare { opcode, .. } |
            Self::IntCompareImm { opcode, .. } |
            Self::IntCond { opcode, .. } |
            Self::IntCondTrap { opcode, .. } |
            Self::IntSelect { opcode, .. } |
            Self::Jump { opcode, .. } |
            Self::Load { opcode, .. } |
            Self::LoadComplex { opcode, .. } |
            Self::LoadNoOffset { opcode, .. } |
            Self::MultiAry { opcode, .. } |
            Self::NullAry { opcode, .. } |
            Self::RegFill { opcode, .. } |
            Self::RegMove { opcode, .. } |
            Self::RegSpill { opcode, .. } |
            Self::Shuffle { opcode, .. } |
            Self::StackLoad { opcode, .. } |
            Self::StackStore { opcode, .. } |
            Self::Store { opcode, .. } |
            Self::StoreComplex { opcode, .. } |
            Self::StoreNoOffset { opcode, .. } |
            Self::TableAddr { opcode, .. } |
            Self::Ternary { opcode, .. } |
            Self::TernaryImm8 { opcode, .. } |
            Self::Trap { opcode, .. } |
            Self::Unary { opcode, .. } |
            Self::UnaryBool { opcode, .. } |
            Self::UnaryConst { opcode, .. } |
            Self::UnaryGlobalValue { opcode, .. } |
            Self::UnaryIeee32 { opcode, .. } |
            Self::UnaryIeee64 { opcode, .. } |
            Self::UnaryImm { opcode, .. } => {
                opcode
            }
        }
    }

    /// Get the controlling type variable operand.
    pub fn typevar_operand(&self, pool: &ir::ValueListPool) -> Option<Value> {
        match *self {
            Self::BranchTableBase { .. } |
            Self::CopySpecial { .. } |
            Self::CopyToSsa { .. } |
            Self::FuncAddr { .. } |
            Self::NullAry { .. } |
            Self::StackLoad { .. } |
            Self::Trap { .. } |
            Self::UnaryBool { .. } |
            Self::UnaryConst { .. } |
            Self::UnaryGlobalValue { .. } |
            Self::UnaryIeee32 { .. } |
            Self::UnaryIeee64 { .. } |
            Self::UnaryImm { .. } => {
                None
            }
            Self::BinaryImm64 { arg, .. } |
            Self::BinaryImm8 { arg, .. } |
            Self::BranchTable { arg, .. } |
            Self::CondTrap { arg, .. } |
            Self::FloatCond { arg, .. } |
            Self::FloatCondTrap { arg, .. } |
            Self::HeapAddr { arg, .. } |
            Self::IndirectJump { arg, .. } |
            Self::IntCompareImm { arg, .. } |
            Self::IntCond { arg, .. } |
            Self::IntCondTrap { arg, .. } |
            Self::Load { arg, .. } |
            Self::LoadNoOffset { arg, .. } |
            Self::RegFill { arg, .. } |
            Self::RegMove { arg, .. } |
            Self::RegSpill { arg, .. } |
            Self::StackStore { arg, .. } |
            Self::TableAddr { arg, .. } |
            Self::Unary { arg, .. } => {
                Some(arg)
            }
            Self::AtomicRmw { args: ref args_arity2, .. } |
            Self::Binary { args: ref args_arity2, .. } |
            Self::BranchTableEntry { args: ref args_arity2, .. } |
            Self::FloatCompare { args: ref args_arity2, .. } |
            Self::IntCompare { args: ref args_arity2, .. } |
            Self::Shuffle { args: ref args_arity2, .. } |
            Self::Store { args: ref args_arity2, .. } |
            Self::StoreNoOffset { args: ref args_arity2, .. } |
            Self::TernaryImm8 { args: ref args_arity2, .. } => {
                Some(args_arity2[0])
            }
            Self::IntSelect { args: ref args_arity3, .. } => {
                Some(args_arity3[0])
            }
            Self::Ternary { args: ref args_arity3, .. } => {
                Some(args_arity3[1])
            }
            Self::AtomicCas { args: ref args_arity3, .. } => {
                Some(args_arity3[2])
            }
            Self::Branch { ref args, .. } |
            Self::BranchFloat { ref args, .. } |
            Self::BranchIcmp { ref args, .. } |
            Self::BranchInt { ref args, .. } |
            Self::Call { ref args, .. } |
            Self::CallIndirect { ref args, .. } |
            Self::Jump { ref args, .. } |
            Self::LoadComplex { ref args, .. } |
            Self::MultiAry { ref args, .. } |
            Self::StoreComplex { ref args, .. } => {
                args.get(0, pool)
            }
        }
    }

    /// Get the value arguments to this instruction.
    pub fn arguments<'a>(&'a self, pool: &'a ir::ValueListPool) -> &[Value] {
        match *self {
            Self::BranchTableBase { .. } |
            Self::CopySpecial { .. } |
            Self::CopyToSsa { .. } |
            Self::FuncAddr { .. } |
            Self::NullAry { .. } |
            Self::StackLoad { .. } |
            Self::Trap { .. } |
            Self::UnaryBool { .. } |
            Self::UnaryConst { .. } |
            Self::UnaryGlobalValue { .. } |
            Self::UnaryIeee32 { .. } |
            Self::UnaryIeee64 { .. } |
            Self::UnaryImm { .. } => {
                &[]
            }
            Self::AtomicRmw { args: ref args_arity2, .. } |
            Self::Binary { args: ref args_arity2, .. } |
            Self::BranchTableEntry { args: ref args_arity2, .. } |
            Self::FloatCompare { args: ref args_arity2, .. } |
            Self::IntCompare { args: ref args_arity2, .. } |
            Self::Shuffle { args: ref args_arity2, .. } |
            Self::Store { args: ref args_arity2, .. } |
            Self::StoreNoOffset { args: ref args_arity2, .. } |
            Self::TernaryImm8 { args: ref args_arity2, .. } => {
                args_arity2
            }
            Self::AtomicCas { args: ref args_arity3, .. } |
            Self::IntSelect { args: ref args_arity3, .. } |
            Self::Ternary { args: ref args_arity3, .. } => {
                args_arity3
            }
            Self::BinaryImm64 { ref arg, .. } |
            Self::BinaryImm8 { ref arg, .. } |
            Self::BranchTable { ref arg, .. } |
            Self::CondTrap { ref arg, .. } |
            Self::FloatCond { ref arg, .. } |
            Self::FloatCondTrap { ref arg, .. } |
            Self::HeapAddr { ref arg, .. } |
            Self::IndirectJump { ref arg, .. } |
            Self::IntCompareImm { ref arg, .. } |
            Self::IntCond { ref arg, .. } |
            Self::IntCondTrap { ref arg, .. } |
            Self::Load { ref arg, .. } |
            Self::LoadNoOffset { ref arg, .. } |
            Self::RegFill { ref arg, .. } |
            Self::RegMove { ref arg, .. } |
            Self::RegSpill { ref arg, .. } |
            Self::StackStore { ref arg, .. } |
            Self::TableAddr { ref arg, .. } |
            Self::Unary { ref arg, .. } => {
                core::slice::from_ref(arg)
            }
            Self::Branch { ref args, .. } |
            Self::BranchFloat { ref args, .. } |
            Self::BranchIcmp { ref args, .. } |
            Self::BranchInt { ref args, .. } |
            Self::Call { ref args, .. } |
            Self::CallIndirect { ref args, .. } |
            Self::Jump { ref args, .. } |
            Self::LoadComplex { ref args, .. } |
            Self::MultiAry { ref args, .. } |
            Self::StoreComplex { ref args, .. } => {
                args.as_slice(pool)
            }
        }
    }

    /// Get mutable references to the value arguments to this
    /// instruction.
    pub fn arguments_mut<'a>(&'a mut self, pool: &'a mut ir::ValueListPool) -> &mut [Value] {
        match *self {
            Self::BranchTableBase { .. } |
            Self::CopySpecial { .. } |
            Self::CopyToSsa { .. } |
            Self::FuncAddr { .. } |
            Self::NullAry { .. } |
            Self::StackLoad { .. } |
            Self::Trap { .. } |
            Self::UnaryBool { .. } |
            Self::UnaryConst { .. } |
            Self::UnaryGlobalValue { .. } |
            Self::UnaryIeee32 { .. } |
            Self::UnaryIeee64 { .. } |
            Self::UnaryImm { .. } => {
                &mut []
            }
            Self::AtomicRmw { args: ref mut args_arity2, .. } |
            Self::Binary { args: ref mut args_arity2, .. } |
            Self::BranchTableEntry { args: ref mut args_arity2, .. } |
            Self::FloatCompare { args: ref mut args_arity2, .. } |
            Self::IntCompare { args: ref mut args_arity2, .. } |
            Self::Shuffle { args: ref mut args_arity2, .. } |
            Self::Store { args: ref mut args_arity2, .. } |
            Self::StoreNoOffset { args: ref mut args_arity2, .. } |
            Self::TernaryImm8 { args: ref mut args_arity2, .. } => {
                args_arity2
            }
            Self::AtomicCas { args: ref mut args_arity3, .. } |
            Self::IntSelect { args: ref mut args_arity3, .. } |
            Self::Ternary { args: ref mut args_arity3, .. } => {
                args_arity3
            }
            Self::BinaryImm64 { ref mut arg, .. } |
            Self::BinaryImm8 { ref mut arg, .. } |
            Self::BranchTable { ref mut arg, .. } |
            Self::CondTrap { ref mut arg, .. } |
            Self::FloatCond { ref mut arg, .. } |
            Self::FloatCondTrap { ref mut arg, .. } |
            Self::HeapAddr { ref mut arg, .. } |
            Self::IndirectJump { ref mut arg, .. } |
            Self::IntCompareImm { ref mut arg, .. } |
            Self::IntCond { ref mut arg, .. } |
            Self::IntCondTrap { ref mut arg, .. } |
            Self::Load { ref mut arg, .. } |
            Self::LoadNoOffset { ref mut arg, .. } |
            Self::RegFill { ref mut arg, .. } |
            Self::RegMove { ref mut arg, .. } |
            Self::RegSpill { ref mut arg, .. } |
            Self::StackStore { ref mut arg, .. } |
            Self::TableAddr { ref mut arg, .. } |
            Self::Unary { ref mut arg, .. } => {
                core::slice::from_mut(arg)
            }
            Self::Branch { ref mut args, .. } |
            Self::BranchFloat { ref mut args, .. } |
            Self::BranchIcmp { ref mut args, .. } |
            Self::BranchInt { ref mut args, .. } |
            Self::Call { ref mut args, .. } |
            Self::CallIndirect { ref mut args, .. } |
            Self::Jump { ref mut args, .. } |
            Self::LoadComplex { ref mut args, .. } |
            Self::MultiAry { ref mut args, .. } |
            Self::StoreComplex { ref mut args, .. } => {
                args.as_mut_slice(pool)
            }
        }
    }

    /// Take out the value list with all the value arguments and return
    /// it.
    ///
    /// This leaves the value list in the instruction empty. Use
    /// `put_value_list` to put the value list back.
    pub fn take_value_list(&mut self) -> Option<ir::ValueList> {
        match *self {
            Self::Branch { ref mut args, .. } |
            Self::BranchFloat { ref mut args, .. } |
            Self::BranchIcmp { ref mut args, .. } |
            Self::BranchInt { ref mut args, .. } |
            Self::Call { ref mut args, .. } |
            Self::CallIndirect { ref mut args, .. } |
            Self::Jump { ref mut args, .. } |
            Self::LoadComplex { ref mut args, .. } |
            Self::MultiAry { ref mut args, .. } |
            Self::StoreComplex { ref mut args, .. } => {
                Some(args.take())
            }
            _ => {
                None
            }
        }
    }

    /// Put back a value list.
    ///
    /// After removing a value list with `take_value_list()`, use this
    /// method to put it back. It is required that this instruction has
    /// a format that accepts a value list, and that the existing value
    /// list is empty. This avoids leaking list pool memory.
    pub fn put_value_list(&mut self, vlist: ir::ValueList) {
        let args = match *self {
            Self::Branch { ref mut args, .. } => args,
            Self::BranchFloat { ref mut args, .. } => args,
            Self::BranchIcmp { ref mut args, .. } => args,
            Self::BranchInt { ref mut args, .. } => args,
            Self::Call { ref mut args, .. } => args,
            Self::CallIndirect { ref mut args, .. } => args,
            Self::Jump { ref mut args, .. } => args,
            Self::LoadComplex { ref mut args, .. } => args,
            Self::MultiAry { ref mut args, .. } => args,
            Self::StoreComplex { ref mut args, .. } => args,
            _ => panic!("No value list: {:?}", self),
        };
        debug_assert!(args.is_empty(), "Value list already in use");
        *args = vlist;
    }

    /// Compare two `InstructionData` for equality.
    ///
    /// This operation requires a reference to a `ValueListPool` to
    /// determine if the contents of any `ValueLists` are equal.
    pub fn eq(&self, other: &Self, pool: &ir::ValueListPool) -> bool {
        if ::core::mem::discriminant(self) != ::core::mem::discriminant(other) {
            return false;
        }
        match (self, other) {
            (&Self::AtomicCas { opcode: ref opcode1, args: ref args1, flags: ref flags1 }, &Self::AtomicCas { opcode: ref opcode2, args: ref args2, flags: ref flags2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && args1 == args2
            }
            (&Self::AtomicRmw { opcode: ref opcode1, args: ref args1, flags: ref flags1, op: ref op1 }, &Self::AtomicRmw { opcode: ref opcode2, args: ref args2, flags: ref flags2, op: ref op2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && op1 == op2
                && args1 == args2
            }
            (&Self::Binary { opcode: ref opcode1, args: ref args1 }, &Self::Binary { opcode: ref opcode2, args: ref args2 }) => {
                opcode1 == opcode2
                && args1 == args2
            }
            (&Self::BinaryImm64 { opcode: ref opcode1, arg: ref arg1, imm: ref imm1 }, &Self::BinaryImm64 { opcode: ref opcode2, arg: ref arg2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && arg1 == arg2
            }
            (&Self::BinaryImm8 { opcode: ref opcode1, arg: ref arg1, imm: ref imm1 }, &Self::BinaryImm8 { opcode: ref opcode2, arg: ref arg2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && arg1 == arg2
            }
            (&Self::Branch { opcode: ref opcode1, args: ref args1, destination: ref destination1 }, &Self::Branch { opcode: ref opcode2, args: ref args2, destination: ref destination2 }) => {
                opcode1 == opcode2
                && destination1 == destination2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::BranchFloat { opcode: ref opcode1, args: ref args1, cond: ref cond1, destination: ref destination1 }, &Self::BranchFloat { opcode: ref opcode2, args: ref args2, cond: ref cond2, destination: ref destination2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && destination1 == destination2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::BranchIcmp { opcode: ref opcode1, args: ref args1, cond: ref cond1, destination: ref destination1 }, &Self::BranchIcmp { opcode: ref opcode2, args: ref args2, cond: ref cond2, destination: ref destination2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && destination1 == destination2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::BranchInt { opcode: ref opcode1, args: ref args1, cond: ref cond1, destination: ref destination1 }, &Self::BranchInt { opcode: ref opcode2, args: ref args2, cond: ref cond2, destination: ref destination2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && destination1 == destination2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::BranchTable { opcode: ref opcode1, arg: ref arg1, destination: ref destination1, table: ref table1 }, &Self::BranchTable { opcode: ref opcode2, arg: ref arg2, destination: ref destination2, table: ref table2 }) => {
                opcode1 == opcode2
                && destination1 == destination2
                && table1 == table2
                && arg1 == arg2
            }
            (&Self::BranchTableBase { opcode: ref opcode1, table: ref table1 }, &Self::BranchTableBase { opcode: ref opcode2, table: ref table2 }) => {
                opcode1 == opcode2
                && table1 == table2
            }
            (&Self::BranchTableEntry { opcode: ref opcode1, args: ref args1, imm: ref imm1, table: ref table1 }, &Self::BranchTableEntry { opcode: ref opcode2, args: ref args2, imm: ref imm2, table: ref table2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && table1 == table2
                && args1 == args2
            }
            (&Self::Call { opcode: ref opcode1, args: ref args1, func_ref: ref func_ref1 }, &Self::Call { opcode: ref opcode2, args: ref args2, func_ref: ref func_ref2 }) => {
                opcode1 == opcode2
                && func_ref1 == func_ref2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::CallIndirect { opcode: ref opcode1, args: ref args1, sig_ref: ref sig_ref1 }, &Self::CallIndirect { opcode: ref opcode2, args: ref args2, sig_ref: ref sig_ref2 }) => {
                opcode1 == opcode2
                && sig_ref1 == sig_ref2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::CondTrap { opcode: ref opcode1, arg: ref arg1, code: ref code1 }, &Self::CondTrap { opcode: ref opcode2, arg: ref arg2, code: ref code2 }) => {
                opcode1 == opcode2
                && code1 == code2
                && arg1 == arg2
            }
            (&Self::CopySpecial { opcode: ref opcode1, src: ref src1, dst: ref dst1 }, &Self::CopySpecial { opcode: ref opcode2, src: ref src2, dst: ref dst2 }) => {
                opcode1 == opcode2
                && src1 == src2
                && dst1 == dst2
            }
            (&Self::CopyToSsa { opcode: ref opcode1, src: ref src1 }, &Self::CopyToSsa { opcode: ref opcode2, src: ref src2 }) => {
                opcode1 == opcode2
                && src1 == src2
            }
            (&Self::FloatCompare { opcode: ref opcode1, args: ref args1, cond: ref cond1 }, &Self::FloatCompare { opcode: ref opcode2, args: ref args2, cond: ref cond2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && args1 == args2
            }
            (&Self::FloatCond { opcode: ref opcode1, arg: ref arg1, cond: ref cond1 }, &Self::FloatCond { opcode: ref opcode2, arg: ref arg2, cond: ref cond2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && arg1 == arg2
            }
            (&Self::FloatCondTrap { opcode: ref opcode1, arg: ref arg1, cond: ref cond1, code: ref code1 }, &Self::FloatCondTrap { opcode: ref opcode2, arg: ref arg2, cond: ref cond2, code: ref code2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && code1 == code2
                && arg1 == arg2
            }
            (&Self::FuncAddr { opcode: ref opcode1, func_ref: ref func_ref1 }, &Self::FuncAddr { opcode: ref opcode2, func_ref: ref func_ref2 }) => {
                opcode1 == opcode2
                && func_ref1 == func_ref2
            }
            (&Self::HeapAddr { opcode: ref opcode1, arg: ref arg1, heap: ref heap1, imm: ref imm1 }, &Self::HeapAddr { opcode: ref opcode2, arg: ref arg2, heap: ref heap2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && heap1 == heap2
                && imm1 == imm2
                && arg1 == arg2
            }
            (&Self::IndirectJump { opcode: ref opcode1, arg: ref arg1, table: ref table1 }, &Self::IndirectJump { opcode: ref opcode2, arg: ref arg2, table: ref table2 }) => {
                opcode1 == opcode2
                && table1 == table2
                && arg1 == arg2
            }
            (&Self::IntCompare { opcode: ref opcode1, args: ref args1, cond: ref cond1 }, &Self::IntCompare { opcode: ref opcode2, args: ref args2, cond: ref cond2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && args1 == args2
            }
            (&Self::IntCompareImm { opcode: ref opcode1, arg: ref arg1, cond: ref cond1, imm: ref imm1 }, &Self::IntCompareImm { opcode: ref opcode2, arg: ref arg2, cond: ref cond2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && imm1 == imm2
                && arg1 == arg2
            }
            (&Self::IntCond { opcode: ref opcode1, arg: ref arg1, cond: ref cond1 }, &Self::IntCond { opcode: ref opcode2, arg: ref arg2, cond: ref cond2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && arg1 == arg2
            }
            (&Self::IntCondTrap { opcode: ref opcode1, arg: ref arg1, cond: ref cond1, code: ref code1 }, &Self::IntCondTrap { opcode: ref opcode2, arg: ref arg2, cond: ref cond2, code: ref code2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && code1 == code2
                && arg1 == arg2
            }
            (&Self::IntSelect { opcode: ref opcode1, args: ref args1, cond: ref cond1 }, &Self::IntSelect { opcode: ref opcode2, args: ref args2, cond: ref cond2 }) => {
                opcode1 == opcode2
                && cond1 == cond2
                && args1 == args2
            }
            (&Self::Jump { opcode: ref opcode1, args: ref args1, destination: ref destination1 }, &Self::Jump { opcode: ref opcode2, args: ref args2, destination: ref destination2 }) => {
                opcode1 == opcode2
                && destination1 == destination2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::Load { opcode: ref opcode1, arg: ref arg1, flags: ref flags1, offset: ref offset1 }, &Self::Load { opcode: ref opcode2, arg: ref arg2, flags: ref flags2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && offset1 == offset2
                && arg1 == arg2
            }
            (&Self::LoadComplex { opcode: ref opcode1, args: ref args1, flags: ref flags1, offset: ref offset1 }, &Self::LoadComplex { opcode: ref opcode2, args: ref args2, flags: ref flags2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && offset1 == offset2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::LoadNoOffset { opcode: ref opcode1, arg: ref arg1, flags: ref flags1 }, &Self::LoadNoOffset { opcode: ref opcode2, arg: ref arg2, flags: ref flags2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && arg1 == arg2
            }
            (&Self::MultiAry { opcode: ref opcode1, args: ref args1 }, &Self::MultiAry { opcode: ref opcode2, args: ref args2 }) => {
                opcode1 == opcode2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::NullAry { opcode: ref opcode1 }, &Self::NullAry { opcode: ref opcode2 }) => {
                opcode1 == opcode2
            }
            (&Self::RegFill { opcode: ref opcode1, arg: ref arg1, src: ref src1, dst: ref dst1 }, &Self::RegFill { opcode: ref opcode2, arg: ref arg2, src: ref src2, dst: ref dst2 }) => {
                opcode1 == opcode2
                && src1 == src2
                && dst1 == dst2
                && arg1 == arg2
            }
            (&Self::RegMove { opcode: ref opcode1, arg: ref arg1, src: ref src1, dst: ref dst1 }, &Self::RegMove { opcode: ref opcode2, arg: ref arg2, src: ref src2, dst: ref dst2 }) => {
                opcode1 == opcode2
                && src1 == src2
                && dst1 == dst2
                && arg1 == arg2
            }
            (&Self::RegSpill { opcode: ref opcode1, arg: ref arg1, src: ref src1, dst: ref dst1 }, &Self::RegSpill { opcode: ref opcode2, arg: ref arg2, src: ref src2, dst: ref dst2 }) => {
                opcode1 == opcode2
                && src1 == src2
                && dst1 == dst2
                && arg1 == arg2
            }
            (&Self::Shuffle { opcode: ref opcode1, args: ref args1, mask: ref mask1 }, &Self::Shuffle { opcode: ref opcode2, args: ref args2, mask: ref mask2 }) => {
                opcode1 == opcode2
                && mask1 == mask2
                && args1 == args2
            }
            (&Self::StackLoad { opcode: ref opcode1, stack_slot: ref stack_slot1, offset: ref offset1 }, &Self::StackLoad { opcode: ref opcode2, stack_slot: ref stack_slot2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && stack_slot1 == stack_slot2
                && offset1 == offset2
            }
            (&Self::StackStore { opcode: ref opcode1, arg: ref arg1, stack_slot: ref stack_slot1, offset: ref offset1 }, &Self::StackStore { opcode: ref opcode2, arg: ref arg2, stack_slot: ref stack_slot2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && stack_slot1 == stack_slot2
                && offset1 == offset2
                && arg1 == arg2
            }
            (&Self::Store { opcode: ref opcode1, args: ref args1, flags: ref flags1, offset: ref offset1 }, &Self::Store { opcode: ref opcode2, args: ref args2, flags: ref flags2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && offset1 == offset2
                && args1 == args2
            }
            (&Self::StoreComplex { opcode: ref opcode1, args: ref args1, flags: ref flags1, offset: ref offset1 }, &Self::StoreComplex { opcode: ref opcode2, args: ref args2, flags: ref flags2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && offset1 == offset2
                && args1.as_slice(pool) == args2.as_slice(pool)
            }
            (&Self::StoreNoOffset { opcode: ref opcode1, args: ref args1, flags: ref flags1 }, &Self::StoreNoOffset { opcode: ref opcode2, args: ref args2, flags: ref flags2 }) => {
                opcode1 == opcode2
                && flags1 == flags2
                && args1 == args2
            }
            (&Self::TableAddr { opcode: ref opcode1, arg: ref arg1, table: ref table1, offset: ref offset1 }, &Self::TableAddr { opcode: ref opcode2, arg: ref arg2, table: ref table2, offset: ref offset2 }) => {
                opcode1 == opcode2
                && table1 == table2
                && offset1 == offset2
                && arg1 == arg2
            }
            (&Self::Ternary { opcode: ref opcode1, args: ref args1 }, &Self::Ternary { opcode: ref opcode2, args: ref args2 }) => {
                opcode1 == opcode2
                && args1 == args2
            }
            (&Self::TernaryImm8 { opcode: ref opcode1, args: ref args1, imm: ref imm1 }, &Self::TernaryImm8 { opcode: ref opcode2, args: ref args2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
                && args1 == args2
            }
            (&Self::Trap { opcode: ref opcode1, code: ref code1 }, &Self::Trap { opcode: ref opcode2, code: ref code2 }) => {
                opcode1 == opcode2
                && code1 == code2
            }
            (&Self::Unary { opcode: ref opcode1, arg: ref arg1 }, &Self::Unary { opcode: ref opcode2, arg: ref arg2 }) => {
                opcode1 == opcode2
                && arg1 == arg2
            }
            (&Self::UnaryBool { opcode: ref opcode1, imm: ref imm1 }, &Self::UnaryBool { opcode: ref opcode2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
            }
            (&Self::UnaryConst { opcode: ref opcode1, constant_handle: ref constant_handle1 }, &Self::UnaryConst { opcode: ref opcode2, constant_handle: ref constant_handle2 }) => {
                opcode1 == opcode2
                && constant_handle1 == constant_handle2
            }
            (&Self::UnaryGlobalValue { opcode: ref opcode1, global_value: ref global_value1 }, &Self::UnaryGlobalValue { opcode: ref opcode2, global_value: ref global_value2 }) => {
                opcode1 == opcode2
                && global_value1 == global_value2
            }
            (&Self::UnaryIeee32 { opcode: ref opcode1, imm: ref imm1 }, &Self::UnaryIeee32 { opcode: ref opcode2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
            }
            (&Self::UnaryIeee64 { opcode: ref opcode1, imm: ref imm1 }, &Self::UnaryIeee64 { opcode: ref opcode2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
            }
            (&Self::UnaryImm { opcode: ref opcode1, imm: ref imm1 }, &Self::UnaryImm { opcode: ref opcode2, imm: ref imm2 }) => {
                opcode1 == opcode2
                && imm1 == imm2
            }
            _ => unreachable!()
        }
    }

    /// Hash an `InstructionData`.
    ///
    /// This operation requires a reference to a `ValueListPool` to
    /// hash the contents of any `ValueLists`.
    pub fn hash<H: ::core::hash::Hasher>(&self, state: &mut H, pool: &ir::ValueListPool) {
        match *self {
            Self::AtomicCas{opcode, ref args, flags} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::AtomicRmw{opcode, ref args, flags, op} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&op, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::Binary{opcode, ref args} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::BinaryImm64{opcode, ref arg, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::BinaryImm8{opcode, ref arg, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Branch{opcode, ref args, destination} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::BranchFloat{opcode, ref args, cond, destination} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::BranchIcmp{opcode, ref args, cond, destination} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::BranchInt{opcode, ref args, cond, destination} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::BranchTable{opcode, ref arg, destination, table} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(&table, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::BranchTableBase{opcode, table} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&table, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::BranchTableEntry{opcode, ref args, imm, table} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&table, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::Call{opcode, ref args, func_ref} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&func_ref, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::CallIndirect{opcode, ref args, sig_ref} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&sig_ref, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::CondTrap{opcode, ref arg, code} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&code, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::CopySpecial{opcode, src, dst} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&src, state);
                ::core::hash::Hash::hash(&dst, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::CopyToSsa{opcode, src} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&src, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::FloatCompare{opcode, ref args, cond} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::FloatCond{opcode, ref arg, cond} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::FloatCondTrap{opcode, ref arg, cond, code} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(&code, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::FuncAddr{opcode, func_ref} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&func_ref, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::HeapAddr{opcode, ref arg, heap, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&heap, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::IndirectJump{opcode, ref arg, table} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&table, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::IntCompare{opcode, ref args, cond} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::IntCompareImm{opcode, ref arg, cond, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::IntCond{opcode, ref arg, cond} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::IntCondTrap{opcode, ref arg, cond, code} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(&code, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::IntSelect{opcode, ref args, cond} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&cond, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::Jump{opcode, ref args, destination} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&destination, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::Load{opcode, ref arg, flags, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::LoadComplex{opcode, ref args, flags, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::LoadNoOffset{opcode, ref arg, flags} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::MultiAry{opcode, ref args} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::NullAry{opcode} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::RegFill{opcode, ref arg, src, dst} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&src, state);
                ::core::hash::Hash::hash(&dst, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::RegMove{opcode, ref arg, src, dst} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&src, state);
                ::core::hash::Hash::hash(&dst, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::RegSpill{opcode, ref arg, src, dst} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&src, state);
                ::core::hash::Hash::hash(&dst, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Shuffle{opcode, ref args, mask} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&mask, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::StackLoad{opcode, stack_slot, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&stack_slot, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::StackStore{opcode, ref arg, stack_slot, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&stack_slot, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Store{opcode, ref args, flags, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::StoreComplex{opcode, ref args, flags, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(args.as_slice(pool), state);
            }
            Self::StoreNoOffset{opcode, ref args, flags} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&flags, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::TableAddr{opcode, ref arg, table, offset} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&table, state);
                ::core::hash::Hash::hash(&offset, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::Ternary{opcode, ref args} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::TernaryImm8{opcode, ref args, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(args, state);
            }
            Self::Trap{opcode, code} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&code, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::Unary{opcode, ref arg} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(arg, state);
            }
            Self::UnaryBool{opcode, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryConst{opcode, constant_handle} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&constant_handle, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryGlobalValue{opcode, global_value} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&global_value, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryIeee32{opcode, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryIeee64{opcode, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&(), state);
            }
            Self::UnaryImm{opcode, imm} => {
                ::core::hash::Hash::hash( &::core::mem::discriminant(self), state);
                ::core::hash::Hash::hash(&opcode, state);
                ::core::hash::Hash::hash(&imm, state);
                ::core::hash::Hash::hash(&(), state);
            }
        }
    }
}

/// An instruction opcode.
///
/// All instructions from all supported ISAs are present.
#[repr(u16)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
#[cfg_attr(
            any(feature = "enable-peepmatic", feature = "enable-serde"),
            derive(serde::Serialize, serde::Deserialize)
        )]
pub enum Opcode {
    /// `jump block, args`. (Jump)
    Jump = 1,
    /// `fallthrough block, args`. (Jump)
    Fallthrough,
    /// `brz c, block, args`. (Branch)
    /// Type inferred from `c`.
    Brz,
    /// `brnz c, block, args`. (Branch)
    /// Type inferred from `c`.
    Brnz,
    /// `br_icmp Cond, x, y, block, args`. (BranchIcmp)
    /// Type inferred from `x`.
    BrIcmp,
    /// `brif Cond, f, block, args`. (BranchInt)
    Brif,
    /// `brff Cond, f, block, args`. (BranchFloat)
    Brff,
    /// `br_table x, block, JT`. (BranchTable)
    /// Type inferred from `x`.
    BrTable,
    /// `entry = jump_table_entry x, addr, Size, JT`. (BranchTableEntry)
    /// Type inferred from `x`.
    JumpTableEntry,
    /// `addr = jump_table_base JT`. (BranchTableBase)
    JumpTableBase,
    /// `indirect_jump_table_br addr, JT`. (IndirectJump)
    /// Type inferred from `addr`.
    IndirectJumpTableBr,
    /// `debugtrap`. (NullAry)
    Debugtrap,
    /// `trap code`. (Trap)
    Trap,
    /// `trapz c, code`. (CondTrap)
    /// Type inferred from `c`.
    Trapz,
    /// `resumable_trap code`. (Trap)
    ResumableTrap,
    /// `trapnz c, code`. (CondTrap)
    /// Type inferred from `c`.
    Trapnz,
    /// `resumable_trapnz c, code`. (CondTrap)
    /// Type inferred from `c`.
    ResumableTrapnz,
    /// `trapif Cond, f, code`. (IntCondTrap)
    Trapif,
    /// `trapff Cond, f, code`. (FloatCondTrap)
    Trapff,
    /// `return rvals`. (MultiAry)
    Return,
    /// `fallthrough_return rvals`. (MultiAry)
    FallthroughReturn,
    /// `rvals = call FN, args`. (Call)
    Call,
    /// `rvals = call_indirect SIG, callee, args`. (CallIndirect)
    /// Type inferred from `callee`.
    CallIndirect,
    /// `addr = func_addr FN`. (FuncAddr)
    FuncAddr,
    /// `a = splat x`. (Unary)
    Splat,
    /// `a = swizzle x, y`. (Binary)
    Swizzle,
    /// `a = insertlane x, y, Idx`. (TernaryImm8)
    /// Type inferred from `x`.
    Insertlane,
    /// `a = extractlane x, Idx`. (BinaryImm8)
    /// Type inferred from `x`.
    Extractlane,
    /// `a = imin x, y`. (Binary)
    /// Type inferred from `x`.
    Imin,
    /// `a = umin x, y`. (Binary)
    /// Type inferred from `x`.
    Umin,
    /// `a = imax x, y`. (Binary)
    /// Type inferred from `x`.
    Imax,
    /// `a = umax x, y`. (Binary)
    /// Type inferred from `x`.
    Umax,
    /// `a = avg_round x, y`. (Binary)
    /// Type inferred from `x`.
    AvgRound,
    /// `a = uadd_sat x, y`. (Binary)
    /// Type inferred from `x`.
    UaddSat,
    /// `a = sadd_sat x, y`. (Binary)
    /// Type inferred from `x`.
    SaddSat,
    /// `a = usub_sat x, y`. (Binary)
    /// Type inferred from `x`.
    UsubSat,
    /// `a = ssub_sat x, y`. (Binary)
    /// Type inferred from `x`.
    SsubSat,
    /// `a = load MemFlags, p, Offset`. (Load)
    Load,
    /// `a = load_complex MemFlags, args, Offset`. (LoadComplex)
    LoadComplex,
    /// `store MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Store,
    /// `store_complex MemFlags, x, args, Offset`. (StoreComplex)
    /// Type inferred from `x`.
    StoreComplex,
    /// `a = uload8 MemFlags, p, Offset`. (Load)
    Uload8,
    /// `a = uload8_complex MemFlags, args, Offset`. (LoadComplex)
    Uload8Complex,
    /// `a = sload8 MemFlags, p, Offset`. (Load)
    Sload8,
    /// `a = sload8_complex MemFlags, args, Offset`. (LoadComplex)
    Sload8Complex,
    /// `istore8 MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Istore8,
    /// `istore8_complex MemFlags, x, args, Offset`. (StoreComplex)
    /// Type inferred from `x`.
    Istore8Complex,
    /// `a = uload16 MemFlags, p, Offset`. (Load)
    Uload16,
    /// `a = uload16_complex MemFlags, args, Offset`. (LoadComplex)
    Uload16Complex,
    /// `a = sload16 MemFlags, p, Offset`. (Load)
    Sload16,
    /// `a = sload16_complex MemFlags, args, Offset`. (LoadComplex)
    Sload16Complex,
    /// `istore16 MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Istore16,
    /// `istore16_complex MemFlags, x, args, Offset`. (StoreComplex)
    /// Type inferred from `x`.
    Istore16Complex,
    /// `a = uload32 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload32,
    /// `a = uload32_complex MemFlags, args, Offset`. (LoadComplex)
    Uload32Complex,
    /// `a = sload32 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload32,
    /// `a = sload32_complex MemFlags, args, Offset`. (LoadComplex)
    Sload32Complex,
    /// `istore32 MemFlags, x, p, Offset`. (Store)
    /// Type inferred from `x`.
    Istore32,
    /// `istore32_complex MemFlags, x, args, Offset`. (StoreComplex)
    Istore32Complex,
    /// `a = uload8x8 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload8x8,
    /// `a = uload8x8_complex MemFlags, args, Offset`. (LoadComplex)
    Uload8x8Complex,
    /// `a = sload8x8 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload8x8,
    /// `a = sload8x8_complex MemFlags, args, Offset`. (LoadComplex)
    Sload8x8Complex,
    /// `a = uload16x4 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload16x4,
    /// `a = uload16x4_complex MemFlags, args, Offset`. (LoadComplex)
    Uload16x4Complex,
    /// `a = sload16x4 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload16x4,
    /// `a = sload16x4_complex MemFlags, args, Offset`. (LoadComplex)
    Sload16x4Complex,
    /// `a = uload32x2 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Uload32x2,
    /// `a = uload32x2_complex MemFlags, args, Offset`. (LoadComplex)
    Uload32x2Complex,
    /// `a = sload32x2 MemFlags, p, Offset`. (Load)
    /// Type inferred from `p`.
    Sload32x2,
    /// `a = sload32x2_complex MemFlags, args, Offset`. (LoadComplex)
    Sload32x2Complex,
    /// `a = stack_load SS, Offset`. (StackLoad)
    StackLoad,
    /// `stack_store x, SS, Offset`. (StackStore)
    /// Type inferred from `x`.
    StackStore,
    /// `addr = stack_addr SS, Offset`. (StackLoad)
    StackAddr,
    /// `a = global_value GV`. (UnaryGlobalValue)
    GlobalValue,
    /// `a = symbol_value GV`. (UnaryGlobalValue)
    SymbolValue,
    /// `a = tls_value GV`. (UnaryGlobalValue)
    TlsValue,
    /// `addr = heap_addr H, p, Size`. (HeapAddr)
    HeapAddr,
    /// `addr = get_pinned_reg`. (NullAry)
    GetPinnedReg,
    /// `set_pinned_reg addr`. (Unary)
    /// Type inferred from `addr`.
    SetPinnedReg,
    /// `addr = table_addr T, p, Offset`. (TableAddr)
    TableAddr,
    /// `a = iconst N`. (UnaryImm)
    Iconst,
    /// `a = f32const N`. (UnaryIeee32)
    F32const,
    /// `a = f64const N`. (UnaryIeee64)
    F64const,
    /// `a = bconst N`. (UnaryBool)
    Bconst,
    /// `a = vconst N`. (UnaryConst)
    Vconst,
    /// `address = const_addr constant`. (UnaryConst)
    ConstAddr,
    /// `a = shuffle a, b, mask`. (Shuffle)
    /// Type inferred from `a`.
    Shuffle,
    /// `a = null`. (NullAry)
    Null,
    /// `nop`. (NullAry)
    Nop,
    /// `a = select c, x, y`. (Ternary)
    /// Type inferred from `x`.
    Select,
    /// `a = selectif cc, flags, x, y`. (IntSelect)
    Selectif,
    /// `a = selectif_spectre_guard cc, flags, x, y`. (IntSelect)
    SelectifSpectreGuard,
    /// `a = bitselect c, x, y`. (Ternary)
    /// Type inferred from `x`.
    Bitselect,
    /// `a = copy x`. (Unary)
    /// Type inferred from `x`.
    Copy,
    /// `a = spill x`. (Unary)
    /// Type inferred from `x`.
    Spill,
    /// `a = fill x`. (Unary)
    /// Type inferred from `x`.
    Fill,
    /// `a = fill_nop x`. (Unary)
    /// Type inferred from `x`.
    FillNop,
    /// `sarg_t = dummy_sarg_t`. (NullAry)
    DummySargT,
    /// `regmove x, src, dst`. (RegMove)
    /// Type inferred from `x`.
    Regmove,
    /// `copy_special src, dst`. (CopySpecial)
    CopySpecial,
    /// `a = copy_to_ssa src`. (CopyToSsa)
    CopyToSsa,
    /// `a = copy_nop x`. (Unary)
    /// Type inferred from `x`.
    CopyNop,
    /// `adjust_sp_down delta`. (Unary)
    /// Type inferred from `delta`.
    AdjustSpDown,
    /// `adjust_sp_up_imm Offset`. (UnaryImm)
    AdjustSpUpImm,
    /// `adjust_sp_down_imm Offset`. (UnaryImm)
    AdjustSpDownImm,
    /// `f = ifcmp_sp addr`. (Unary)
    /// Type inferred from `addr`.
    IfcmpSp,
    /// `regspill x, src, SS`. (RegSpill)
    /// Type inferred from `x`.
    Regspill,
    /// `regfill x, SS, dst`. (RegFill)
    /// Type inferred from `x`.
    Regfill,
    /// `safepoint args`. (MultiAry)
    Safepoint,
    /// `lo, hi = vsplit x`. (Unary)
    /// Type inferred from `x`.
    Vsplit,
    /// `a = vconcat x, y`. (Binary)
    /// Type inferred from `x`.
    Vconcat,
    /// `a = vselect c, x, y`. (Ternary)
    /// Type inferred from `x`.
    Vselect,
    /// `s = vany_true a`. (Unary)
    /// Type inferred from `a`.
    VanyTrue,
    /// `s = vall_true a`. (Unary)
    /// Type inferred from `a`.
    VallTrue,
    /// `x = vhigh_bits a`. (Unary)
    VhighBits,
    /// `a = icmp Cond, x, y`. (IntCompare)
    /// Type inferred from `x`.
    Icmp,
    /// `a = icmp_imm Cond, x, Y`. (IntCompareImm)
    /// Type inferred from `x`.
    IcmpImm,
    /// `f = ifcmp x, y`. (Binary)
    /// Type inferred from `x`.
    Ifcmp,
    /// `f = ifcmp_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IfcmpImm,
    /// `a = iadd x, y`. (Binary)
    /// Type inferred from `x`.
    Iadd,
    /// `a = isub x, y`. (Binary)
    /// Type inferred from `x`.
    Isub,
    /// `a = ineg x`. (Unary)
    /// Type inferred from `x`.
    Ineg,
    /// `a = iabs x`. (Unary)
    /// Type inferred from `x`.
    Iabs,
    /// `a = imul x, y`. (Binary)
    /// Type inferred from `x`.
    Imul,
    /// `a = umulhi x, y`. (Binary)
    /// Type inferred from `x`.
    Umulhi,
    /// `a = smulhi x, y`. (Binary)
    /// Type inferred from `x`.
    Smulhi,
    /// `a = udiv x, y`. (Binary)
    /// Type inferred from `x`.
    Udiv,
    /// `a = sdiv x, y`. (Binary)
    /// Type inferred from `x`.
    Sdiv,
    /// `a = urem x, y`. (Binary)
    /// Type inferred from `x`.
    Urem,
    /// `a = srem x, y`. (Binary)
    /// Type inferred from `x`.
    Srem,
    /// `a = iadd_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IaddImm,
    /// `a = imul_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    ImulImm,
    /// `a = udiv_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    UdivImm,
    /// `a = sdiv_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    SdivImm,
    /// `a = urem_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    UremImm,
    /// `a = srem_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    SremImm,
    /// `a = irsub_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IrsubImm,
    /// `a = iadd_cin x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddCin,
    /// `a = iadd_ifcin x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddIfcin,
    /// `a, c_out = iadd_cout x, y`. (Binary)
    /// Type inferred from `x`.
    IaddCout,
    /// `a, c_out = iadd_ifcout x, y`. (Binary)
    /// Type inferred from `x`.
    IaddIfcout,
    /// `a, c_out = iadd_carry x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddCarry,
    /// `a, c_out = iadd_ifcarry x, y, c_in`. (Ternary)
    /// Type inferred from `y`.
    IaddIfcarry,
    /// `a = isub_bin x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubBin,
    /// `a = isub_ifbin x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubIfbin,
    /// `a, b_out = isub_bout x, y`. (Binary)
    /// Type inferred from `x`.
    IsubBout,
    /// `a, b_out = isub_ifbout x, y`. (Binary)
    /// Type inferred from `x`.
    IsubIfbout,
    /// `a, b_out = isub_borrow x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubBorrow,
    /// `a, b_out = isub_ifborrow x, y, b_in`. (Ternary)
    /// Type inferred from `y`.
    IsubIfborrow,
    /// `a = band x, y`. (Binary)
    /// Type inferred from `x`.
    Band,
    /// `a = bor x, y`. (Binary)
    /// Type inferred from `x`.
    Bor,
    /// `a = bxor x, y`. (Binary)
    /// Type inferred from `x`.
    Bxor,
    /// `a = bnot x`. (Unary)
    /// Type inferred from `x`.
    Bnot,
    /// `a = band_not x, y`. (Binary)
    /// Type inferred from `x`.
    BandNot,
    /// `a = bor_not x, y`. (Binary)
    /// Type inferred from `x`.
    BorNot,
    /// `a = bxor_not x, y`. (Binary)
    /// Type inferred from `x`.
    BxorNot,
    /// `a = band_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    BandImm,
    /// `a = bor_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    BorImm,
    /// `a = bxor_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    BxorImm,
    /// `a = rotl x, y`. (Binary)
    /// Type inferred from `x`.
    Rotl,
    /// `a = rotr x, y`. (Binary)
    /// Type inferred from `x`.
    Rotr,
    /// `a = rotl_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    RotlImm,
    /// `a = rotr_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    RotrImm,
    /// `a = ishl x, y`. (Binary)
    /// Type inferred from `x`.
    Ishl,
    /// `a = ushr x, y`. (Binary)
    /// Type inferred from `x`.
    Ushr,
    /// `a = sshr x, y`. (Binary)
    /// Type inferred from `x`.
    Sshr,
    /// `a = ishl_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    IshlImm,
    /// `a = ushr_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    UshrImm,
    /// `a = sshr_imm x, Y`. (BinaryImm64)
    /// Type inferred from `x`.
    SshrImm,
    /// `a = bitrev x`. (Unary)
    /// Type inferred from `x`.
    Bitrev,
    /// `a = clz x`. (Unary)
    /// Type inferred from `x`.
    Clz,
    /// `a = cls x`. (Unary)
    /// Type inferred from `x`.
    Cls,
    /// `a = ctz x`. (Unary)
    /// Type inferred from `x`.
    Ctz,
    /// `a = popcnt x`. (Unary)
    /// Type inferred from `x`.
    Popcnt,
    /// `a = fcmp Cond, x, y`. (FloatCompare)
    /// Type inferred from `x`.
    Fcmp,
    /// `f = ffcmp x, y`. (Binary)
    /// Type inferred from `x`.
    Ffcmp,
    /// `a = fadd x, y`. (Binary)
    /// Type inferred from `x`.
    Fadd,
    /// `a = fsub x, y`. (Binary)
    /// Type inferred from `x`.
    Fsub,
    /// `a = fmul x, y`. (Binary)
    /// Type inferred from `x`.
    Fmul,
    /// `a = fdiv x, y`. (Binary)
    /// Type inferred from `x`.
    Fdiv,
    /// `a = sqrt x`. (Unary)
    /// Type inferred from `x`.
    Sqrt,
    /// `a = fma x, y, z`. (Ternary)
    /// Type inferred from `y`.
    Fma,
    /// `a = fneg x`. (Unary)
    /// Type inferred from `x`.
    Fneg,
    /// `a = fabs x`. (Unary)
    /// Type inferred from `x`.
    Fabs,
    /// `a = fcopysign x, y`. (Binary)
    /// Type inferred from `x`.
    Fcopysign,
    /// `a = fmin x, y`. (Binary)
    /// Type inferred from `x`.
    Fmin,
    /// `a = fmin_pseudo x, y`. (Binary)
    /// Type inferred from `x`.
    FminPseudo,
    /// `a = fmax x, y`. (Binary)
    /// Type inferred from `x`.
    Fmax,
    /// `a = fmax_pseudo x, y`. (Binary)
    /// Type inferred from `x`.
    FmaxPseudo,
    /// `a = ceil x`. (Unary)
    /// Type inferred from `x`.
    Ceil,
    /// `a = floor x`. (Unary)
    /// Type inferred from `x`.
    Floor,
    /// `a = trunc x`. (Unary)
    /// Type inferred from `x`.
    Trunc,
    /// `a = nearest x`. (Unary)
    /// Type inferred from `x`.
    Nearest,
    /// `a = is_null x`. (Unary)
    /// Type inferred from `x`.
    IsNull,
    /// `a = is_invalid x`. (Unary)
    /// Type inferred from `x`.
    IsInvalid,
    /// `a = trueif Cond, f`. (IntCond)
    Trueif,
    /// `a = trueff Cond, f`. (FloatCond)
    Trueff,
    /// `a = bitcast x`. (Unary)
    Bitcast,
    /// `a = raw_bitcast x`. (Unary)
    RawBitcast,
    /// `a = scalar_to_vector s`. (Unary)
    ScalarToVector,
    /// `a = breduce x`. (Unary)
    Breduce,
    /// `a = bextend x`. (Unary)
    Bextend,
    /// `a = bint x`. (Unary)
    Bint,
    /// `a = bmask x`. (Unary)
    Bmask,
    /// `a = ireduce x`. (Unary)
    Ireduce,
    /// `a = snarrow x, y`. (Binary)
    /// Type inferred from `x`.
    Snarrow,
    /// `a = unarrow x, y`. (Binary)
    /// Type inferred from `x`.
    Unarrow,
    /// `a = swiden_low x`. (Unary)
    /// Type inferred from `x`.
    SwidenLow,
    /// `a = swiden_high x`. (Unary)
    /// Type inferred from `x`.
    SwidenHigh,
    /// `a = uwiden_low x`. (Unary)
    /// Type inferred from `x`.
    UwidenLow,
    /// `a = uwiden_high x`. (Unary)
    /// Type inferred from `x`.
    UwidenHigh,
    /// `a = widening_pairwise_dot_product_s x, y`. (Binary)
    WideningPairwiseDotProductS,
    /// `a = uextend x`. (Unary)
    Uextend,
    /// `a = sextend x`. (Unary)
    Sextend,
    /// `a = fpromote x`. (Unary)
    Fpromote,
    /// `a = fdemote x`. (Unary)
    Fdemote,
    /// `a = fcvt_to_uint x`. (Unary)
    FcvtToUint,
    /// `a = fcvt_to_uint_sat x`. (Unary)
    FcvtToUintSat,
    /// `a = fcvt_to_sint x`. (Unary)
    FcvtToSint,
    /// `a = fcvt_to_sint_sat x`. (Unary)
    FcvtToSintSat,
    /// `a = fcvt_from_uint x`. (Unary)
    FcvtFromUint,
    /// `a = fcvt_from_sint x`. (Unary)
    FcvtFromSint,
    /// `a = fcvt_low_from_sint x`. (Unary)
    FcvtLowFromSint,
    /// `lo, hi = isplit x`. (Unary)
    /// Type inferred from `x`.
    Isplit,
    /// `a = iconcat lo, hi`. (Binary)
    /// Type inferred from `lo`.
    Iconcat,
    /// `a = atomic_rmw MemFlags, AtomicRmwOp, p, x`. (AtomicRmw)
    AtomicRmw,
    /// `a = atomic_cas MemFlags, p, e, x`. (AtomicCas)
    /// Type inferred from `x`.
    AtomicCas,
    /// `a = atomic_load MemFlags, p`. (LoadNoOffset)
    AtomicLoad,
    /// `atomic_store MemFlags, x, p`. (StoreNoOffset)
    /// Type inferred from `x`.
    AtomicStore,
    /// `fence`. (NullAry)
    Fence,
}

impl Opcode {
    /// True for instructions that terminate the block
    pub fn is_terminator(self) -> bool {
        match self {
            Self::BrTable |
            Self::Fallthrough |
            Self::FallthroughReturn |
            Self::IndirectJumpTableBr |
            Self::Jump |
            Self::Return |
            Self::Trap => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// True for all branch or jump instructions.
    pub fn is_branch(self) -> bool {
        match self {
            Self::BrIcmp |
            Self::BrTable |
            Self::Brff |
            Self::Brif |
            Self::Brnz |
            Self::Brz |
            Self::Fallthrough |
            Self::IndirectJumpTableBr |
            Self::Jump => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// True for all indirect branch or jump instructions.
    pub fn is_indirect_branch(self) -> bool {
        match self {
            Self::IndirectJumpTableBr => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Is this a call instruction?
    pub fn is_call(self) -> bool {
        match self {
            Self::Call |
            Self::CallIndirect => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Is this a return instruction?
    pub fn is_return(self) -> bool {
        match self {
            Self::FallthroughReturn |
            Self::Return => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Is this a ghost instruction?
    pub fn is_ghost(self) -> bool {
        match self {
            Self::Iconcat |
            Self::Isplit |
            Self::Vconcat |
            Self::Vsplit => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Can this instruction read from memory?
    pub fn can_load(self) -> bool {
        match self {
            Self::AtomicCas |
            Self::AtomicLoad |
            Self::AtomicRmw |
            Self::Debugtrap |
            Self::Fill |
            Self::FillNop |
            Self::JumpTableEntry |
            Self::Load |
            Self::LoadComplex |
            Self::Sload16 |
            Self::Sload16Complex |
            Self::Sload16x4 |
            Self::Sload16x4Complex |
            Self::Sload32 |
            Self::Sload32Complex |
            Self::Sload32x2 |
            Self::Sload32x2Complex |
            Self::Sload8 |
            Self::Sload8Complex |
            Self::Sload8x8 |
            Self::Sload8x8Complex |
            Self::StackLoad |
            Self::Uload16 |
            Self::Uload16Complex |
            Self::Uload16x4 |
            Self::Uload16x4Complex |
            Self::Uload32 |
            Self::Uload32Complex |
            Self::Uload32x2 |
            Self::Uload32x2Complex |
            Self::Uload8 |
            Self::Uload8Complex |
            Self::Uload8x8 |
            Self::Uload8x8Complex => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Can this instruction write to memory?
    pub fn can_store(self) -> bool {
        match self {
            Self::AtomicCas |
            Self::AtomicRmw |
            Self::AtomicStore |
            Self::Debugtrap |
            Self::Istore16 |
            Self::Istore16Complex |
            Self::Istore32 |
            Self::Istore32Complex |
            Self::Istore8 |
            Self::Istore8Complex |
            Self::Spill |
            Self::StackStore |
            Self::Store |
            Self::StoreComplex => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Can this instruction cause a trap?
    pub fn can_trap(self) -> bool {
        match self {
            Self::FcvtToSint |
            Self::FcvtToUint |
            Self::ResumableTrap |
            Self::ResumableTrapnz |
            Self::Sdiv |
            Self::Srem |
            Self::Trap |
            Self::Trapff |
            Self::Trapif |
            Self::Trapnz |
            Self::Trapz |
            Self::Udiv |
            Self::Urem => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Does this instruction have other side effects besides can_* flags?
    pub fn other_side_effects(self) -> bool {
        match self {
            Self::AdjustSpDown |
            Self::AdjustSpDownImm |
            Self::AdjustSpUpImm |
            Self::AtomicCas |
            Self::AtomicLoad |
            Self::AtomicRmw |
            Self::AtomicStore |
            Self::CopySpecial |
            Self::CopyToSsa |
            Self::Debugtrap |
            Self::Fence |
            Self::GetPinnedReg |
            Self::Regfill |
            Self::Regmove |
            Self::Regspill |
            Self::Safepoint |
            Self::SelectifSpectreGuard |
            Self::SetPinnedReg => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Does this instruction write to CPU flags?
    pub fn writes_cpu_flags(self) -> bool {
        match self {
            Self::Ffcmp |
            Self::IaddIfcarry |
            Self::IaddIfcout |
            Self::Ifcmp |
            Self::IfcmpImm |
            Self::IfcmpSp |
            Self::IsubIfborrow |
            Self::IsubIfbout => {
                true
            }
            _ => {
                false
            }
        }
    }

    /// Should this opcode be considered to clobber all the registers, during regalloc?
    pub fn clobbers_all_regs(self) -> bool {
        match self {
            _ => {
                false
            }
        }
    }

}

const OPCODE_FORMAT: [InstructionFormat; 231] = [
    InstructionFormat::Jump, // jump
    InstructionFormat::Jump, // fallthrough
    InstructionFormat::Branch, // brz
    InstructionFormat::Branch, // brnz
    InstructionFormat::BranchIcmp, // br_icmp
    InstructionFormat::BranchInt, // brif
    InstructionFormat::BranchFloat, // brff
    InstructionFormat::BranchTable, // br_table
    InstructionFormat::BranchTableEntry, // jump_table_entry
    InstructionFormat::BranchTableBase, // jump_table_base
    InstructionFormat::IndirectJump, // indirect_jump_table_br
    InstructionFormat::NullAry, // debugtrap
    InstructionFormat::Trap, // trap
    InstructionFormat::CondTrap, // trapz
    InstructionFormat::Trap, // resumable_trap
    InstructionFormat::CondTrap, // trapnz
    InstructionFormat::CondTrap, // resumable_trapnz
    InstructionFormat::IntCondTrap, // trapif
    InstructionFormat::FloatCondTrap, // trapff
    InstructionFormat::MultiAry, // return
    InstructionFormat::MultiAry, // fallthrough_return
    InstructionFormat::Call, // call
    InstructionFormat::CallIndirect, // call_indirect
    InstructionFormat::FuncAddr, // func_addr
    InstructionFormat::Unary, // splat
    InstructionFormat::Binary, // swizzle
    InstructionFormat::TernaryImm8, // insertlane
    InstructionFormat::BinaryImm8, // extractlane
    InstructionFormat::Binary, // imin
    InstructionFormat::Binary, // umin
    InstructionFormat::Binary, // imax
    InstructionFormat::Binary, // umax
    InstructionFormat::Binary, // avg_round
    InstructionFormat::Binary, // uadd_sat
    InstructionFormat::Binary, // sadd_sat
    InstructionFormat::Binary, // usub_sat
    InstructionFormat::Binary, // ssub_sat
    InstructionFormat::Load, // load
    InstructionFormat::LoadComplex, // load_complex
    InstructionFormat::Store, // store
    InstructionFormat::StoreComplex, // store_complex
    InstructionFormat::Load, // uload8
    InstructionFormat::LoadComplex, // uload8_complex
    InstructionFormat::Load, // sload8
    InstructionFormat::LoadComplex, // sload8_complex
    InstructionFormat::Store, // istore8
    InstructionFormat::StoreComplex, // istore8_complex
    InstructionFormat::Load, // uload16
    InstructionFormat::LoadComplex, // uload16_complex
    InstructionFormat::Load, // sload16
    InstructionFormat::LoadComplex, // sload16_complex
    InstructionFormat::Store, // istore16
    InstructionFormat::StoreComplex, // istore16_complex
    InstructionFormat::Load, // uload32
    InstructionFormat::LoadComplex, // uload32_complex
    InstructionFormat::Load, // sload32
    InstructionFormat::LoadComplex, // sload32_complex
    InstructionFormat::Store, // istore32
    InstructionFormat::StoreComplex, // istore32_complex
    InstructionFormat::Load, // uload8x8
    InstructionFormat::LoadComplex, // uload8x8_complex
    InstructionFormat::Load, // sload8x8
    InstructionFormat::LoadComplex, // sload8x8_complex
    InstructionFormat::Load, // uload16x4
    InstructionFormat::LoadComplex, // uload16x4_complex
    InstructionFormat::Load, // sload16x4
    InstructionFormat::LoadComplex, // sload16x4_complex
    InstructionFormat::Load, // uload32x2
    InstructionFormat::LoadComplex, // uload32x2_complex
    InstructionFormat::Load, // sload32x2
    InstructionFormat::LoadComplex, // sload32x2_complex
    InstructionFormat::StackLoad, // stack_load
    InstructionFormat::StackStore, // stack_store
    InstructionFormat::StackLoad, // stack_addr
    InstructionFormat::UnaryGlobalValue, // global_value
    InstructionFormat::UnaryGlobalValue, // symbol_value
    InstructionFormat::UnaryGlobalValue, // tls_value
    InstructionFormat::HeapAddr, // heap_addr
    InstructionFormat::NullAry, // get_pinned_reg
    InstructionFormat::Unary, // set_pinned_reg
    InstructionFormat::TableAddr, // table_addr
    InstructionFormat::UnaryImm, // iconst
    InstructionFormat::UnaryIeee32, // f32const
    InstructionFormat::UnaryIeee64, // f64const
    InstructionFormat::UnaryBool, // bconst
    InstructionFormat::UnaryConst, // vconst
    InstructionFormat::UnaryConst, // const_addr
    InstructionFormat::Shuffle, // shuffle
    InstructionFormat::NullAry, // null
    InstructionFormat::NullAry, // nop
    InstructionFormat::Ternary, // select
    InstructionFormat::IntSelect, // selectif
    InstructionFormat::IntSelect, // selectif_spectre_guard
    InstructionFormat::Ternary, // bitselect
    InstructionFormat::Unary, // copy
    InstructionFormat::Unary, // spill
    InstructionFormat::Unary, // fill
    InstructionFormat::Unary, // fill_nop
    InstructionFormat::NullAry, // dummy_sarg_t
    InstructionFormat::RegMove, // regmove
    InstructionFormat::CopySpecial, // copy_special
    InstructionFormat::CopyToSsa, // copy_to_ssa
    InstructionFormat::Unary, // copy_nop
    InstructionFormat::Unary, // adjust_sp_down
    InstructionFormat::UnaryImm, // adjust_sp_up_imm
    InstructionFormat::UnaryImm, // adjust_sp_down_imm
    InstructionFormat::Unary, // ifcmp_sp
    InstructionFormat::RegSpill, // regspill
    InstructionFormat::RegFill, // regfill
    InstructionFormat::MultiAry, // safepoint
    InstructionFormat::Unary, // vsplit
    InstructionFormat::Binary, // vconcat
    InstructionFormat::Ternary, // vselect
    InstructionFormat::Unary, // vany_true
    InstructionFormat::Unary, // vall_true
    InstructionFormat::Unary, // vhigh_bits
    InstructionFormat::IntCompare, // icmp
    InstructionFormat::IntCompareImm, // icmp_imm
    InstructionFormat::Binary, // ifcmp
    InstructionFormat::BinaryImm64, // ifcmp_imm
    InstructionFormat::Binary, // iadd
    InstructionFormat::Binary, // isub
    InstructionFormat::Unary, // ineg
    InstructionFormat::Unary, // iabs
    InstructionFormat::Binary, // imul
    InstructionFormat::Binary, // umulhi
    InstructionFormat::Binary, // smulhi
    InstructionFormat::Binary, // udiv
    InstructionFormat::Binary, // sdiv
    InstructionFormat::Binary, // urem
    InstructionFormat::Binary, // srem
    InstructionFormat::BinaryImm64, // iadd_imm
    InstructionFormat::BinaryImm64, // imul_imm
    InstructionFormat::BinaryImm64, // udiv_imm
    InstructionFormat::BinaryImm64, // sdiv_imm
    InstructionFormat::BinaryImm64, // urem_imm
    InstructionFormat::BinaryImm64, // srem_imm
    InstructionFormat::BinaryImm64, // irsub_imm
    InstructionFormat::Ternary, // iadd_cin
    InstructionFormat::Ternary, // iadd_ifcin
    InstructionFormat::Binary, // iadd_cout
    InstructionFormat::Binary, // iadd_ifcout
    InstructionFormat::Ternary, // iadd_carry
    InstructionFormat::Ternary, // iadd_ifcarry
    InstructionFormat::Ternary, // isub_bin
    InstructionFormat::Ternary, // isub_ifbin
    InstructionFormat::Binary, // isub_bout
    InstructionFormat::Binary, // isub_ifbout
    InstructionFormat::Ternary, // isub_borrow
    InstructionFormat::Ternary, // isub_ifborrow
    InstructionFormat::Binary, // band
    InstructionFormat::Binary, // bor
    InstructionFormat::Binary, // bxor
    InstructionFormat::Unary, // bnot
    InstructionFormat::Binary, // band_not
    InstructionFormat::Binary, // bor_not
    InstructionFormat::Binary, // bxor_not
    InstructionFormat::BinaryImm64, // band_imm
    InstructionFormat::BinaryImm64, // bor_imm
    InstructionFormat::BinaryImm64, // bxor_imm
    InstructionFormat::Binary, // rotl
    InstructionFormat::Binary, // rotr
    InstructionFormat::BinaryImm64, // rotl_imm
    InstructionFormat::BinaryImm64, // rotr_imm
    InstructionFormat::Binary, // ishl
    InstructionFormat::Binary, // ushr
    InstructionFormat::Binary, // sshr
    InstructionFormat::BinaryImm64, // ishl_imm
    InstructionFormat::BinaryImm64, // ushr_imm
    InstructionFormat::BinaryImm64, // sshr_imm
    InstructionFormat::Unary, // bitrev
    InstructionFormat::Unary, // clz
    InstructionFormat::Unary, // cls
    InstructionFormat::Unary, // ctz
    InstructionFormat::Unary, // popcnt
    InstructionFormat::FloatCompare, // fcmp
    InstructionFormat::Binary, // ffcmp
    InstructionFormat::Binary, // fadd
    InstructionFormat::Binary, // fsub
    InstructionFormat::Binary, // fmul
    InstructionFormat::Binary, // fdiv
    InstructionFormat::Unary, // sqrt
    InstructionFormat::Ternary, // fma
    InstructionFormat::Unary, // fneg
    InstructionFormat::Unary, // fabs
    InstructionFormat::Binary, // fcopysign
    InstructionFormat::Binary, // fmin
    InstructionFormat::Binary, // fmin_pseudo
    InstructionFormat::Binary, // fmax
    InstructionFormat::Binary, // fmax_pseudo
    InstructionFormat::Unary, // ceil
    InstructionFormat::Unary, // floor
    InstructionFormat::Unary, // trunc
    InstructionFormat::Unary, // nearest
    InstructionFormat::Unary, // is_null
    InstructionFormat::Unary, // is_invalid
    InstructionFormat::IntCond, // trueif
    InstructionFormat::FloatCond, // trueff
    InstructionFormat::Unary, // bitcast
    InstructionFormat::Unary, // raw_bitcast
    InstructionFormat::Unary, // scalar_to_vector
    InstructionFormat::Unary, // breduce
    InstructionFormat::Unary, // bextend
    InstructionFormat::Unary, // bint
    InstructionFormat::Unary, // bmask
    InstructionFormat::Unary, // ireduce
    InstructionFormat::Binary, // snarrow
    InstructionFormat::Binary, // unarrow
    InstructionFormat::Unary, // swiden_low
    InstructionFormat::Unary, // swiden_high
    InstructionFormat::Unary, // uwiden_low
    InstructionFormat::Unary, // uwiden_high
    InstructionFormat::Binary, // widening_pairwise_dot_product_s
    InstructionFormat::Unary, // uextend
    InstructionFormat::Unary, // sextend
    InstructionFormat::Unary, // fpromote
    InstructionFormat::Unary, // fdemote
    InstructionFormat::Unary, // fcvt_to_uint
    InstructionFormat::Unary, // fcvt_to_uint_sat
    InstructionFormat::Unary, // fcvt_to_sint
    InstructionFormat::Unary, // fcvt_to_sint_sat
    InstructionFormat::Unary, // fcvt_from_uint
    InstructionFormat::Unary, // fcvt_from_sint
    InstructionFormat::Unary, // fcvt_low_from_sint
    InstructionFormat::Unary, // isplit
    InstructionFormat::Binary, // iconcat
    InstructionFormat::AtomicRmw, // atomic_rmw
    InstructionFormat::AtomicCas, // atomic_cas
    InstructionFormat::LoadNoOffset, // atomic_load
    InstructionFormat::StoreNoOffset, // atomic_store
    InstructionFormat::NullAry, // fence
];

fn opcode_name(opc: Opcode) -> &'static str {
    match opc {
        Opcode::AdjustSpDown => {
            "adjust_sp_down"
        }
        Opcode::AdjustSpDownImm => {
            "adjust_sp_down_imm"
        }
        Opcode::AdjustSpUpImm => {
            "adjust_sp_up_imm"
        }
        Opcode::AtomicCas => {
            "atomic_cas"
        }
        Opcode::AtomicLoad => {
            "atomic_load"
        }
        Opcode::AtomicRmw => {
            "atomic_rmw"
        }
        Opcode::AtomicStore => {
            "atomic_store"
        }
        Opcode::AvgRound => {
            "avg_round"
        }
        Opcode::Band => {
            "band"
        }
        Opcode::BandImm => {
            "band_imm"
        }
        Opcode::BandNot => {
            "band_not"
        }
        Opcode::Bconst => {
            "bconst"
        }
        Opcode::Bextend => {
            "bextend"
        }
        Opcode::Bint => {
            "bint"
        }
        Opcode::Bitcast => {
            "bitcast"
        }
        Opcode::Bitrev => {
            "bitrev"
        }
        Opcode::Bitselect => {
            "bitselect"
        }
        Opcode::Bmask => {
            "bmask"
        }
        Opcode::Bnot => {
            "bnot"
        }
        Opcode::Bor => {
            "bor"
        }
        Opcode::BorImm => {
            "bor_imm"
        }
        Opcode::BorNot => {
            "bor_not"
        }
        Opcode::BrIcmp => {
            "br_icmp"
        }
        Opcode::BrTable => {
            "br_table"
        }
        Opcode::Breduce => {
            "breduce"
        }
        Opcode::Brff => {
            "brff"
        }
        Opcode::Brif => {
            "brif"
        }
        Opcode::Brnz => {
            "brnz"
        }
        Opcode::Brz => {
            "brz"
        }
        Opcode::Bxor => {
            "bxor"
        }
        Opcode::BxorImm => {
            "bxor_imm"
        }
        Opcode::BxorNot => {
            "bxor_not"
        }
        Opcode::Call => {
            "call"
        }
        Opcode::CallIndirect => {
            "call_indirect"
        }
        Opcode::Ceil => {
            "ceil"
        }
        Opcode::Cls => {
            "cls"
        }
        Opcode::Clz => {
            "clz"
        }
        Opcode::ConstAddr => {
            "const_addr"
        }
        Opcode::Copy => {
            "copy"
        }
        Opcode::CopyNop => {
            "copy_nop"
        }
        Opcode::CopySpecial => {
            "copy_special"
        }
        Opcode::CopyToSsa => {
            "copy_to_ssa"
        }
        Opcode::Ctz => {
            "ctz"
        }
        Opcode::Debugtrap => {
            "debugtrap"
        }
        Opcode::DummySargT => {
            "dummy_sarg_t"
        }
        Opcode::Extractlane => {
            "extractlane"
        }
        Opcode::F32const => {
            "f32const"
        }
        Opcode::F64const => {
            "f64const"
        }
        Opcode::Fabs => {
            "fabs"
        }
        Opcode::Fadd => {
            "fadd"
        }
        Opcode::Fallthrough => {
            "fallthrough"
        }
        Opcode::FallthroughReturn => {
            "fallthrough_return"
        }
        Opcode::Fcmp => {
            "fcmp"
        }
        Opcode::Fcopysign => {
            "fcopysign"
        }
        Opcode::FcvtFromSint => {
            "fcvt_from_sint"
        }
        Opcode::FcvtFromUint => {
            "fcvt_from_uint"
        }
        Opcode::FcvtLowFromSint => {
            "fcvt_low_from_sint"
        }
        Opcode::FcvtToSint => {
            "fcvt_to_sint"
        }
        Opcode::FcvtToSintSat => {
            "fcvt_to_sint_sat"
        }
        Opcode::FcvtToUint => {
            "fcvt_to_uint"
        }
        Opcode::FcvtToUintSat => {
            "fcvt_to_uint_sat"
        }
        Opcode::Fdemote => {
            "fdemote"
        }
        Opcode::Fdiv => {
            "fdiv"
        }
        Opcode::Fence => {
            "fence"
        }
        Opcode::Ffcmp => {
            "ffcmp"
        }
        Opcode::Fill => {
            "fill"
        }
        Opcode::FillNop => {
            "fill_nop"
        }
        Opcode::Floor => {
            "floor"
        }
        Opcode::Fma => {
            "fma"
        }
        Opcode::Fmax => {
            "fmax"
        }
        Opcode::FmaxPseudo => {
            "fmax_pseudo"
        }
        Opcode::Fmin => {
            "fmin"
        }
        Opcode::FminPseudo => {
            "fmin_pseudo"
        }
        Opcode::Fmul => {
            "fmul"
        }
        Opcode::Fneg => {
            "fneg"
        }
        Opcode::Fpromote => {
            "fpromote"
        }
        Opcode::Fsub => {
            "fsub"
        }
        Opcode::FuncAddr => {
            "func_addr"
        }
        Opcode::GetPinnedReg => {
            "get_pinned_reg"
        }
        Opcode::GlobalValue => {
            "global_value"
        }
        Opcode::HeapAddr => {
            "heap_addr"
        }
        Opcode::Iabs => {
            "iabs"
        }
        Opcode::Iadd => {
            "iadd"
        }
        Opcode::IaddCarry => {
            "iadd_carry"
        }
        Opcode::IaddCin => {
            "iadd_cin"
        }
        Opcode::IaddCout => {
            "iadd_cout"
        }
        Opcode::IaddIfcarry => {
            "iadd_ifcarry"
        }
        Opcode::IaddIfcin => {
            "iadd_ifcin"
        }
        Opcode::IaddIfcout => {
            "iadd_ifcout"
        }
        Opcode::IaddImm => {
            "iadd_imm"
        }
        Opcode::Icmp => {
            "icmp"
        }
        Opcode::IcmpImm => {
            "icmp_imm"
        }
        Opcode::Iconcat => {
            "iconcat"
        }
        Opcode::Iconst => {
            "iconst"
        }
        Opcode::Ifcmp => {
            "ifcmp"
        }
        Opcode::IfcmpImm => {
            "ifcmp_imm"
        }
        Opcode::IfcmpSp => {
            "ifcmp_sp"
        }
        Opcode::Imax => {
            "imax"
        }
        Opcode::Imin => {
            "imin"
        }
        Opcode::Imul => {
            "imul"
        }
        Opcode::ImulImm => {
            "imul_imm"
        }
        Opcode::IndirectJumpTableBr => {
            "indirect_jump_table_br"
        }
        Opcode::Ineg => {
            "ineg"
        }
        Opcode::Insertlane => {
            "insertlane"
        }
        Opcode::Ireduce => {
            "ireduce"
        }
        Opcode::IrsubImm => {
            "irsub_imm"
        }
        Opcode::IsInvalid => {
            "is_invalid"
        }
        Opcode::IsNull => {
            "is_null"
        }
        Opcode::Ishl => {
            "ishl"
        }
        Opcode::IshlImm => {
            "ishl_imm"
        }
        Opcode::Isplit => {
            "isplit"
        }
        Opcode::Istore16 => {
            "istore16"
        }
        Opcode::Istore16Complex => {
            "istore16_complex"
        }
        Opcode::Istore32 => {
            "istore32"
        }
        Opcode::Istore32Complex => {
            "istore32_complex"
        }
        Opcode::Istore8 => {
            "istore8"
        }
        Opcode::Istore8Complex => {
            "istore8_complex"
        }
        Opcode::Isub => {
            "isub"
        }
        Opcode::IsubBin => {
            "isub_bin"
        }
        Opcode::IsubBorrow => {
            "isub_borrow"
        }
        Opcode::IsubBout => {
            "isub_bout"
        }
        Opcode::IsubIfbin => {
            "isub_ifbin"
        }
        Opcode::IsubIfborrow => {
            "isub_ifborrow"
        }
        Opcode::IsubIfbout => {
            "isub_ifbout"
        }
        Opcode::Jump => {
            "jump"
        }
        Opcode::JumpTableBase => {
            "jump_table_base"
        }
        Opcode::JumpTableEntry => {
            "jump_table_entry"
        }
        Opcode::Load => {
            "load"
        }
        Opcode::LoadComplex => {
            "load_complex"
        }
        Opcode::Nearest => {
            "nearest"
        }
        Opcode::Nop => {
            "nop"
        }
        Opcode::Null => {
            "null"
        }
        Opcode::Popcnt => {
            "popcnt"
        }
        Opcode::RawBitcast => {
            "raw_bitcast"
        }
        Opcode::Regfill => {
            "regfill"
        }
        Opcode::Regmove => {
            "regmove"
        }
        Opcode::Regspill => {
            "regspill"
        }
        Opcode::ResumableTrap => {
            "resumable_trap"
        }
        Opcode::ResumableTrapnz => {
            "resumable_trapnz"
        }
        Opcode::Return => {
            "return"
        }
        Opcode::Rotl => {
            "rotl"
        }
        Opcode::RotlImm => {
            "rotl_imm"
        }
        Opcode::Rotr => {
            "rotr"
        }
        Opcode::RotrImm => {
            "rotr_imm"
        }
        Opcode::SaddSat => {
            "sadd_sat"
        }
        Opcode::Safepoint => {
            "safepoint"
        }
        Opcode::ScalarToVector => {
            "scalar_to_vector"
        }
        Opcode::Sdiv => {
            "sdiv"
        }
        Opcode::SdivImm => {
            "sdiv_imm"
        }
        Opcode::Select => {
            "select"
        }
        Opcode::Selectif => {
            "selectif"
        }
        Opcode::SelectifSpectreGuard => {
            "selectif_spectre_guard"
        }
        Opcode::SetPinnedReg => {
            "set_pinned_reg"
        }
        Opcode::Sextend => {
            "sextend"
        }
        Opcode::Shuffle => {
            "shuffle"
        }
        Opcode::Sload16 => {
            "sload16"
        }
        Opcode::Sload16Complex => {
            "sload16_complex"
        }
        Opcode::Sload16x4 => {
            "sload16x4"
        }
        Opcode::Sload16x4Complex => {
            "sload16x4_complex"
        }
        Opcode::Sload32 => {
            "sload32"
        }
        Opcode::Sload32Complex => {
            "sload32_complex"
        }
        Opcode::Sload32x2 => {
            "sload32x2"
        }
        Opcode::Sload32x2Complex => {
            "sload32x2_complex"
        }
        Opcode::Sload8 => {
            "sload8"
        }
        Opcode::Sload8Complex => {
            "sload8_complex"
        }
        Opcode::Sload8x8 => {
            "sload8x8"
        }
        Opcode::Sload8x8Complex => {
            "sload8x8_complex"
        }
        Opcode::Smulhi => {
            "smulhi"
        }
        Opcode::Snarrow => {
            "snarrow"
        }
        Opcode::Spill => {
            "spill"
        }
        Opcode::Splat => {
            "splat"
        }
        Opcode::Sqrt => {
            "sqrt"
        }
        Opcode::Srem => {
            "srem"
        }
        Opcode::SremImm => {
            "srem_imm"
        }
        Opcode::Sshr => {
            "sshr"
        }
        Opcode::SshrImm => {
            "sshr_imm"
        }
        Opcode::SsubSat => {
            "ssub_sat"
        }
        Opcode::StackAddr => {
            "stack_addr"
        }
        Opcode::StackLoad => {
            "stack_load"
        }
        Opcode::StackStore => {
            "stack_store"
        }
        Opcode::Store => {
            "store"
        }
        Opcode::StoreComplex => {
            "store_complex"
        }
        Opcode::SwidenHigh => {
            "swiden_high"
        }
        Opcode::SwidenLow => {
            "swiden_low"
        }
        Opcode::Swizzle => {
            "swizzle"
        }
        Opcode::SymbolValue => {
            "symbol_value"
        }
        Opcode::TableAddr => {
            "table_addr"
        }
        Opcode::TlsValue => {
            "tls_value"
        }
        Opcode::Trap => {
            "trap"
        }
        Opcode::Trapff => {
            "trapff"
        }
        Opcode::Trapif => {
            "trapif"
        }
        Opcode::Trapnz => {
            "trapnz"
        }
        Opcode::Trapz => {
            "trapz"
        }
        Opcode::Trueff => {
            "trueff"
        }
        Opcode::Trueif => {
            "trueif"
        }
        Opcode::Trunc => {
            "trunc"
        }
        Opcode::UaddSat => {
            "uadd_sat"
        }
        Opcode::Udiv => {
            "udiv"
        }
        Opcode::UdivImm => {
            "udiv_imm"
        }
        Opcode::Uextend => {
            "uextend"
        }
        Opcode::Uload16 => {
            "uload16"
        }
        Opcode::Uload16Complex => {
            "uload16_complex"
        }
        Opcode::Uload16x4 => {
            "uload16x4"
        }
        Opcode::Uload16x4Complex => {
            "uload16x4_complex"
        }
        Opcode::Uload32 => {
            "uload32"
        }
        Opcode::Uload32Complex => {
            "uload32_complex"
        }
        Opcode::Uload32x2 => {
            "uload32x2"
        }
        Opcode::Uload32x2Complex => {
            "uload32x2_complex"
        }
        Opcode::Uload8 => {
            "uload8"
        }
        Opcode::Uload8Complex => {
            "uload8_complex"
        }
        Opcode::Uload8x8 => {
            "uload8x8"
        }
        Opcode::Uload8x8Complex => {
            "uload8x8_complex"
        }
        Opcode::Umax => {
            "umax"
        }
        Opcode::Umin => {
            "umin"
        }
        Opcode::Umulhi => {
            "umulhi"
        }
        Opcode::Unarrow => {
            "unarrow"
        }
        Opcode::Urem => {
            "urem"
        }
        Opcode::UremImm => {
            "urem_imm"
        }
        Opcode::Ushr => {
            "ushr"
        }
        Opcode::UshrImm => {
            "ushr_imm"
        }
        Opcode::UsubSat => {
            "usub_sat"
        }
        Opcode::UwidenHigh => {
            "uwiden_high"
        }
        Opcode::UwidenLow => {
            "uwiden_low"
        }
        Opcode::VallTrue => {
            "vall_true"
        }
        Opcode::VanyTrue => {
            "vany_true"
        }
        Opcode::Vconcat => {
            "vconcat"
        }
        Opcode::Vconst => {
            "vconst"
        }
        Opcode::VhighBits => {
            "vhigh_bits"
        }
        Opcode::Vselect => {
            "vselect"
        }
        Opcode::Vsplit => {
            "vsplit"
        }
        Opcode::WideningPairwiseDotProductS => {
            "widening_pairwise_dot_product_s"
        }
    }
}

const OPCODE_HASH_TABLE: [Option<Opcode>; 512] = [
    Some(Opcode::Regfill),
    Some(Opcode::Sload32x2Complex),
    None,
    Some(Opcode::Brif),
    Some(Opcode::HeapAddr),
    Some(Opcode::FcvtToSintSat),
    Some(Opcode::Imin),
    None,
    Some(Opcode::TableAddr),
    Some(Opcode::Urem),
    Some(Opcode::Umin),
    Some(Opcode::Ifcmp),
    None,
    Some(Opcode::Sdiv),
    Some(Opcode::Brnz),
    Some(Opcode::Fallthrough),
    Some(Opcode::Isub),
    Some(Opcode::UshrImm),
    Some(Opcode::Brff),
    Some(Opcode::Trap),
    Some(Opcode::Srem),
    Some(Opcode::Imul),
    Some(Opcode::Uload16Complex),
    Some(Opcode::Bxor),
    Some(Opcode::JumpTableEntry),
    None,
    None,
    Some(Opcode::Umax),
    Some(Opcode::Sload8x8Complex),
    Some(Opcode::SremImm),
    Some(Opcode::BxorNot),
    Some(Opcode::Imax),
    Some(Opcode::Load),
    Some(Opcode::Fadd),
    Some(Opcode::Jump),
    Some(Opcode::Null),
    Some(Opcode::Shuffle),
    Some(Opcode::Copy),
    Some(Opcode::Umulhi),
    Some(Opcode::Ushr),
    None,
    Some(Opcode::BxorImm),
    None,
    Some(Opcode::VallTrue),
    Some(Opcode::Band),
    Some(Opcode::Fsub),
    Some(Opcode::CopyNop),
    Some(Opcode::Fabs),
    Some(Opcode::Ishl),
    Some(Opcode::Fmax),
    Some(Opcode::Vconst),
    Some(Opcode::Call),
    Some(Opcode::Fneg),
    Some(Opcode::Sqrt),
    Some(Opcode::Fill),
    None,
    Some(Opcode::Ceil),
    Some(Opcode::Ineg),
    Some(Opcode::SaddSat),
    Some(Opcode::Popcnt),
    Some(Opcode::Fmin),
    None,
    None,
    Some(Opcode::SwidenHigh),
    None,
    None,
    Some(Opcode::Bnot),
    Some(Opcode::FmaxPseudo),
    Some(Opcode::Trueff),
    Some(Opcode::Trapff),
    Some(Opcode::Isplit),
    Some(Opcode::Bint),
    Some(Opcode::Fdiv),
    Some(Opcode::Fcmp),
    None,
    Some(Opcode::IaddIfcin),
    Some(Opcode::Fmul),
    Some(Opcode::AdjustSpDownImm),
    Some(Opcode::IsubBin),
    None,
    Some(Opcode::Trapif),
    Some(Opcode::Trueif),
    None,
    None,
    Some(Opcode::BrIcmp),
    Some(Opcode::ConstAddr),
    Some(Opcode::UremImm),
    Some(Opcode::AtomicLoad),
    Some(Opcode::CopyToSsa),
    Some(Opcode::Trapnz),
    None,
    None,
    None,
    Some(Opcode::Bitrev),
    None,
    None,
    Some(Opcode::Smulhi),
    Some(Opcode::IsNull),
    None,
    None,
    None,
    None,
    Some(Opcode::Sextend),
    Some(Opcode::IfcmpSp),
    Some(Opcode::BorNot),
    None,
    Some(Opcode::Sload8x8),
    Some(Opcode::IsubIfbout),
    Some(Opcode::Istore8Complex),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::ImulImm),
    Some(Opcode::Ireduce),
    None,
    Some(Opcode::RotrImm),
    None,
    Some(Opcode::StackStore),
    Some(Opcode::UwidenLow),
    Some(Opcode::Select),
    Some(Opcode::BorImm),
    None,
    Some(Opcode::Selectif),
    None,
    Some(Opcode::Bconst),
    None,
    None,
    None,
    Some(Opcode::IcmpImm),
    None,
    None,
    Some(Opcode::FillNop),
    Some(Opcode::Fcopysign),
    None,
    Some(Opcode::SdivImm),
    None,
    Some(Opcode::AvgRound),
    Some(Opcode::Sload16x4Complex),
    None,
    None,
    None,
    Some(Opcode::StackAddr),
    None,
    Some(Opcode::BandImm),
    Some(Opcode::IsubBorrow),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::VanyTrue),
    Some(Opcode::IsubIfbin),
    Some(Opcode::UsubSat),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Iconcat),
    Some(Opcode::Fence),
    None,
    Some(Opcode::Regspill),
    None,
    None,
    Some(Opcode::Fma),
    None,
    None,
    Some(Opcode::BrTable),
    Some(Opcode::F64const),
    Some(Opcode::Nop),
    Some(Opcode::StackLoad),
    Some(Opcode::IrsubImm),
    Some(Opcode::Bor),
    Some(Opcode::StoreComplex),
    Some(Opcode::IsubBout),
    Some(Opcode::Debugtrap),
    Some(Opcode::Sload16x4),
    Some(Opcode::SelectifSpectreGuard),
    Some(Opcode::IshlImm),
    Some(Opcode::BandNot),
    Some(Opcode::Ctz),
    Some(Opcode::AdjustSpDown),
    Some(Opcode::Ffcmp),
    Some(Opcode::UwidenHigh),
    Some(Opcode::Brz),
    None,
    None,
    Some(Opcode::Floor),
    Some(Opcode::UaddSat),
    Some(Opcode::Sload32x2),
    Some(Opcode::Clz),
    None,
    None,
    Some(Opcode::Cls),
    None,
    None,
    None,
    Some(Opcode::SymbolValue),
    Some(Opcode::Bmask),
    Some(Opcode::FcvtToUintSat),
    Some(Opcode::AtomicRmw),
    Some(Opcode::SsubSat),
    None,
    None,
    Some(Opcode::ScalarToVector),
    None,
    None,
    None,
    Some(Opcode::FallthroughReturn),
    None,
    None,
    Some(Opcode::Trapz),
    Some(Opcode::Iabs),
    Some(Opcode::Udiv),
    None,
    Some(Opcode::AtomicCas),
    None,
    None,
    Some(Opcode::SwidenLow),
    None,
    Some(Opcode::Rotl),
    None,
    None,
    Some(Opcode::Regmove),
    None,
    None,
    None,
    Some(Opcode::UdivImm),
    None,
    None,
    Some(Opcode::IaddCout),
    Some(Opcode::Rotr),
    None,
    Some(Opcode::VhighBits),
    Some(Opcode::Iadd),
    None,
    Some(Opcode::Trunc),
    None,
    None,
    None,
    Some(Opcode::Icmp),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::AtomicStore),
    None,
    None,
    Some(Opcode::Uload16x4Complex),
    Some(Opcode::TlsValue),
    Some(Opcode::Nearest),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Iconst),
    Some(Opcode::Uload32Complex),
    Some(Opcode::IaddIfcarry),
    None,
    Some(Opcode::Store),
    None,
    Some(Opcode::SshrImm),
    None,
    Some(Opcode::FcvtFromSint),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::IsubIfborrow),
    None,
    None,
    None,
    Some(Opcode::FcvtFromUint),
    None,
    Some(Opcode::Insertlane),
    None,
    Some(Opcode::Swizzle),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Uload16x4),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::FuncAddr),
    Some(Opcode::FcvtToUint),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::GlobalValue),
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::RotlImm),
    Some(Opcode::FcvtToSint),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Uload8x8),
    None,
    Some(Opcode::ResumableTrapnz),
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::LoadComplex),
    None,
    Some(Opcode::Uload16),
    Some(Opcode::Safepoint),
    Some(Opcode::Uload32),
    Some(Opcode::IaddImm),
    None,
    Some(Opcode::IaddCarry),
    None,
    Some(Opcode::Uload8x8Complex),
    Some(Opcode::Vsplit),
    None,
    Some(Opcode::RawBitcast),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::AdjustSpUpImm),
    None,
    None,
    None,
    None,
    Some(Opcode::SetPinnedReg),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Fdemote),
    None,
    None,
    None,
    None,
    Some(Opcode::IaddCin),
    Some(Opcode::Istore32),
    None,
    Some(Opcode::Istore16),
    None,
    None,
    None,
    Some(Opcode::Uload8Complex),
    None,
    None,
    None,
    Some(Opcode::Sload16),
    None,
    None,
    None,
    Some(Opcode::ResumableTrap),
    Some(Opcode::DummySargT),
    Some(Opcode::Sload32),
    Some(Opcode::Breduce),
    None,
    Some(Opcode::Extractlane),
    Some(Opcode::Unarrow),
    None,
    None,
    None,
    Some(Opcode::Return),
    None,
    Some(Opcode::Uload32x2),
    None,
    None,
    None,
    Some(Opcode::Sload8Complex),
    None,
    Some(Opcode::JumpTableBase),
    Some(Opcode::Sload16Complex),
    None,
    Some(Opcode::Sload32Complex),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Bitselect),
    Some(Opcode::Istore8),
    Some(Opcode::IaddIfcout),
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Bitcast),
    None,
    None,
    None,
    None,
    Some(Opcode::Bextend),
    None,
    None,
    None,
    Some(Opcode::Uextend),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::IndirectJumpTableBr),
    None,
    Some(Opcode::Fpromote),
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::GetPinnedReg),
    None,
    Some(Opcode::Vselect),
    None,
    None,
    None,
    None,
    Some(Opcode::Uload8),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::CopySpecial),
    None,
    None,
    None,
    None,
    Some(Opcode::FminPseudo),
    None,
    Some(Opcode::F32const),
    Some(Opcode::IsInvalid),
    Some(Opcode::WideningPairwiseDotProductS),
    Some(Opcode::Splat),
    Some(Opcode::Uload32x2Complex),
    Some(Opcode::Snarrow),
    Some(Opcode::CallIndirect),
    Some(Opcode::Sload8),
    None,
    Some(Opcode::Istore16Complex),
    Some(Opcode::FcvtLowFromSint),
    None,
    Some(Opcode::IfcmpImm),
    Some(Opcode::Spill),
    None,
    None,
    None,
    None,
    None,
    Some(Opcode::Vconcat),
    None,
    Some(Opcode::Sshr),
    None,
    Some(Opcode::Istore32Complex),
];


// Table of opcode constraints.
const OPCODE_CONSTRAINTS: [OpcodeConstraints; 231] = [
    // Jump: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Fallthrough: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Brz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Brnz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // BrIcmp: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Brif: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::IFLAGS)']
    OpcodeConstraints {
        flags: 0x20,
        typeset_offset: 255,
        constraint_offset: 3,
    },
    // Brff: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::FFLAGS)']
    OpcodeConstraints {
        flags: 0x20,
        typeset_offset: 255,
        constraint_offset: 4,
    },
    // BrTable: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // JumpTableEntry: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // JumpTableBase: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // IndirectJumpTableBr: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // Debugtrap: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Trap: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Trapz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // ResumableTrap: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Trapnz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // ResumableTrapnz: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 0,
        constraint_offset: 0,
    },
    // Trapif: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::IFLAGS)']
    OpcodeConstraints {
        flags: 0x20,
        typeset_offset: 255,
        constraint_offset: 3,
    },
    // Trapff: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::FFLAGS)']
    OpcodeConstraints {
        flags: 0x20,
        typeset_offset: 255,
        constraint_offset: 4,
    },
    // Return: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // FallthroughReturn: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Call: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // CallIndirect: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // FuncAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // Splat: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'LaneOf']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 5,
    },
    // Swizzle: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::I8X16)', 'Concrete(ir::types::I8X16)']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x41,
        typeset_offset: 3,
        constraint_offset: 7,
    },
    // Insertlane: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'LaneOf']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 3,
        constraint_offset: 10,
    },
    // Extractlane: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['LaneOf', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 3,
        constraint_offset: 6,
    },
    // Imin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Umin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Imax: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Umax: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // AvgRound: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // UaddSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // SaddSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // UsubSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // SsubSat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 5,
        constraint_offset: 0,
    },
    // Load: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 6,
        constraint_offset: 13,
    },
    // LoadComplex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 6,
        constraint_offset: 0,
    },
    // Store: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 6,
        constraint_offset: 13,
    },
    // StoreComplex: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 6,
        constraint_offset: 0,
    },
    // Uload8: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 7,
        constraint_offset: 13,
    },
    // Uload8Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 7,
        constraint_offset: 0,
    },
    // Sload8: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 7,
        constraint_offset: 13,
    },
    // Sload8Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 7,
        constraint_offset: 0,
    },
    // Istore8: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 7,
        constraint_offset: 13,
    },
    // Istore8Complex: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={16, 32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 7,
        constraint_offset: 0,
    },
    // Uload16: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 8,
        constraint_offset: 13,
    },
    // Uload16Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 8,
        constraint_offset: 0,
    },
    // Sload16: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 8,
        constraint_offset: 13,
    },
    // Sload16Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 8,
        constraint_offset: 0,
    },
    // Istore16: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 8,
        constraint_offset: 13,
    },
    // Istore16Complex: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 8,
        constraint_offset: 0,
    },
    // Uload32: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 15,
    },
    // Uload32Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I64)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 15,
    },
    // Sload32: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 15,
    },
    // Sload32Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I64)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 15,
    },
    // Istore32: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Concrete(ir::types::I64)', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 9,
        constraint_offset: 17,
    },
    // Istore32Complex: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::I64)']
    OpcodeConstraints {
        flags: 0x20,
        typeset_offset: 255,
        constraint_offset: 15,
    },
    // Uload8x8: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I16X8)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 19,
    },
    // Uload8x8Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I16X8)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 19,
    },
    // Sload8x8: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I16X8)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 19,
    },
    // Sload8x8Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I16X8)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 19,
    },
    // Uload16x4: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I32X4)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 21,
    },
    // Uload16x4Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I32X4)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 21,
    },
    // Sload16x4: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I32X4)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 21,
    },
    // Sload16x4Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I32X4)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 21,
    },
    // Uload32x2: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64X2)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 23,
    },
    // Uload32x2Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I64X2)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 23,
    },
    // Sload32x2: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::I64X2)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 23,
    },
    // Sload32x2Complex: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::I64X2)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 23,
    },
    // StackLoad: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 6,
        constraint_offset: 0,
    },
    // StackStore: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 6,
        constraint_offset: 0,
    },
    // StackAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // GlobalValue: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 6,
        constraint_offset: 0,
    },
    // SymbolValue: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 6,
        constraint_offset: 0,
    },
    // TlsValue: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 6,
        constraint_offset: 0,
    },
    // HeapAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(8)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 2,
        constraint_offset: 24,
    },
    // GetPinnedReg: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // SetPinnedReg: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // TableAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(8)']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 2,
        constraint_offset: 24,
    },
    // Iconst: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // F32const: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::F32)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 26,
    },
    // F64const: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::F64)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 27,
    },
    // Bconst: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 10,
        constraint_offset: 0,
    },
    // Vconst: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 3,
        constraint_offset: 0,
    },
    // ConstAddr: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 2,
        constraint_offset: 0,
    },
    // Shuffle: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={16}, ints={8}, bools={8})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 11,
        constraint_offset: 0,
    },
    // Null: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 12,
        constraint_offset: 0,
    },
    // Nop: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Select: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Free(0)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 13,
        constraint_offset: 28,
    },
    // Selectif: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x61,
        typeset_offset: 13,
        constraint_offset: 31,
    },
    // SelectifSpectreGuard: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x61,
        typeset_offset: 13,
        constraint_offset: 31,
    },
    // Bitselect: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 13,
        constraint_offset: 33,
    },
    // Copy: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // Spill: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // Fill: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // FillNop: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // DummySargT: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Concrete(ir::types::SARG_T)']
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 255,
        constraint_offset: 37,
    },
    // Regmove: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // CopySpecial: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // CopyToSsa: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x01,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // CopyNop: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // AdjustSpDown: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // AdjustSpUpImm: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // AdjustSpDownImm: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // IfcmpSp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::IFLAGS)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 2,
        constraint_offset: 32,
    },
    // Regspill: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // Regfill: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x38,
        typeset_offset: 13,
        constraint_offset: 0,
    },
    // Safepoint: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
    // Vsplit: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['HalfVector', 'HalfVector', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x3a,
        typeset_offset: 3,
        constraint_offset: 38,
    },
    // Vconcat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['DoubleVector', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 14,
        constraint_offset: 41,
    },
    // Vselect: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'AsBool', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 3,
        constraint_offset: 43,
    },
    // VanyTrue: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::B1)', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 3,
        constraint_offset: 47,
    },
    // VallTrue: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::B1)', 'Same']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 3,
        constraint_offset: 47,
    },
    // VhighBits: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(3)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 48,
    },
    // Icmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['AsBool', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 4,
        constraint_offset: 44,
    },
    // IcmpImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::B1)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 47,
    },
    // Ifcmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 1,
        constraint_offset: 32,
    },
    // IfcmpImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::IFLAGS)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 1,
        constraint_offset: 32,
    },
    // Iadd: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Isub: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Ineg: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Iabs: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Imul: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Umulhi: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Smulhi: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Udiv: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Sdiv: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Urem: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Srem: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // IaddImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // ImulImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // UdivImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // SdivImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // UremImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // SremImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // IrsubImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // IaddCin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::B1)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 1,
        constraint_offset: 50,
    },
    // IaddIfcin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // IaddCout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::B1)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 1,
        constraint_offset: 52,
    },
    // IaddIfcout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 1,
        constraint_offset: 31,
    },
    // IaddCarry: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::B1)', 'Same', 'Same', 'Concrete(ir::types::B1)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 1,
        constraint_offset: 52,
    },
    // IaddIfcarry: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 1,
        constraint_offset: 57,
    },
    // IsubBin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::B1)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 1,
        constraint_offset: 50,
    },
    // IsubIfbin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // IsubBout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::B1)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 1,
        constraint_offset: 52,
    },
    // IsubIfbout: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x4a,
        typeset_offset: 1,
        constraint_offset: 31,
    },
    // IsubBorrow: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::B1)', 'Same', 'Same', 'Concrete(ir::types::B1)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 1,
        constraint_offset: 52,
    },
    // IsubIfborrow: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Concrete(ir::types::IFLAGS)', 'Same', 'Same', 'Concrete(ir::types::IFLAGS)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x6a,
        typeset_offset: 1,
        constraint_offset: 57,
    },
    // Band: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // Bor: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // Bxor: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // Bnot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // BandNot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // BorNot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // BxorNot: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 15,
        constraint_offset: 0,
    },
    // BandImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // BorImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // BxorImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Rotl: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 62,
    },
    // Rotr: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 62,
    },
    // RotlImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // RotrImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Ishl: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 62,
    },
    // Ushr: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 62,
    },
    // Sshr: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Free(1)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 4,
        constraint_offset: 62,
    },
    // IshlImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // UshrImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // SshrImm: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 4,
        constraint_offset: 0,
    },
    // Bitrev: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Clz: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Cls: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Ctz: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Popcnt: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 1,
        constraint_offset: 0,
    },
    // Fcmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['AsBool', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 16,
        constraint_offset: 44,
    },
    // Ffcmp: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Concrete(ir::types::FFLAGS)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 16,
        constraint_offset: 65,
    },
    // Fadd: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fsub: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fmul: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fdiv: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Sqrt: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fma: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 16,
        constraint_offset: 33,
    },
    // Fneg: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fabs: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fcopysign: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fmin: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // FminPseudo: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Fmax: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // FmaxPseudo: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x49,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Ceil: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Floor: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Trunc: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // Nearest: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x29,
        typeset_offset: 16,
        constraint_offset: 0,
    },
    // IsNull: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::B1)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 12,
        constraint_offset: 47,
    },
    // IsInvalid: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['Concrete(ir::types::B1)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 12,
        constraint_offset: 47,
    },
    // Trueif: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::B1)', 'Concrete(ir::types::IFLAGS)']
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 255,
        constraint_offset: 68,
    },
    // Trueff: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Concrete(ir::types::B1)', 'Concrete(ir::types::FFLAGS)']
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 255,
        constraint_offset: 70,
    },
    // Bitcast: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(6)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 6,
        constraint_offset: 72,
    },
    // RawBitcast: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(13)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 13,
        constraint_offset: 74,
    },
    // ScalarToVector: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'LaneOf']
    // Polymorphic over TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 3,
        constraint_offset: 5,
    },
    // Breduce: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(10)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 10,
        constraint_offset: 76,
    },
    // Bextend: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(10)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, bools={1, 8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 10,
        constraint_offset: 76,
    },
    // Bint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(10)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 76,
    },
    // Bmask: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(10)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 76,
    },
    // Ireduce: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(4)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 78,
    },
    // Snarrow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['SplitLanes', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={4, 8}, ints={16, 32})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 17,
        constraint_offset: 80,
    },
    // Unarrow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['SplitLanes', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={4, 8}, ints={16, 32})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 17,
        constraint_offset: 80,
    },
    // SwidenLow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={8, 16}, ints={8, 16})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 18,
        constraint_offset: 83,
    },
    // SwidenHigh: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={8, 16}, ints={8, 16})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 18,
        constraint_offset: 83,
    },
    // UwidenLow: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={8, 16}, ints={8, 16})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 18,
        constraint_offset: 83,
    },
    // UwidenHigh: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['MergeLanes', 'Same']
    // Polymorphic over TypeSet(lanes={8, 16}, ints={8, 16})
    OpcodeConstraints {
        flags: 0x39,
        typeset_offset: 18,
        constraint_offset: 83,
    },
    // WideningPairwiseDotProductS: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Concrete(ir::types::I32X4)', 'Concrete(ir::types::I16X8)', 'Concrete(ir::types::I16X8)']
    OpcodeConstraints {
        flags: 0x41,
        typeset_offset: 255,
        constraint_offset: 85,
    },
    // Uextend: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(4)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 78,
    },
    // Sextend: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(4)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 78,
    },
    // Fpromote: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(16)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 16,
        constraint_offset: 88,
    },
    // Fdemote: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(16)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 16,
        constraint_offset: 88,
    },
    // FcvtToUint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(16)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 88,
    },
    // FcvtToUintSat: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(16)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 88,
    },
    // FcvtToSint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(16)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 88,
    },
    // FcvtToSintSat: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(16)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 4,
        constraint_offset: 88,
    },
    // FcvtFromUint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(4)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 16,
        constraint_offset: 78,
    },
    // FcvtFromSint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(4)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 16,
        constraint_offset: 78,
    },
    // FcvtLowFromSint: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(4)']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 16,
        constraint_offset: 78,
    },
    // Isplit: fixed_results=2, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=1
    // Constraints=['HalfWidth', 'HalfWidth', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
    OpcodeConstraints {
        flags: 0x3a,
        typeset_offset: 19,
        constraint_offset: 90,
    },
    // Iconcat: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['DoubleWidth', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x59,
        typeset_offset: 20,
        constraint_offset: 93,
    },
    // AtomicRmw: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=2
    // Constraints=['Same', 'Free(2)', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x41,
        typeset_offset: 21,
        constraint_offset: 95,
    },
    // AtomicCas: fixed_results=1, use_typevar_operand=true, requires_typevar_operand=false, fixed_values=3
    // Constraints=['Same', 'Free(2)', 'Same', 'Same']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x69,
        typeset_offset: 21,
        constraint_offset: 95,
    },
    // AtomicLoad: fixed_results=1, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=1
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x21,
        typeset_offset: 21,
        constraint_offset: 13,
    },
    // AtomicStore: fixed_results=0, use_typevar_operand=true, requires_typevar_operand=true, fixed_values=2
    // Constraints=['Same', 'Free(2)']
    // Polymorphic over TypeSet(lanes={1}, ints={8, 16, 32, 64})
    OpcodeConstraints {
        flags: 0x58,
        typeset_offset: 21,
        constraint_offset: 13,
    },
    // Fence: fixed_results=0, use_typevar_operand=false, requires_typevar_operand=false, fixed_values=0
    // Constraints=[]
    OpcodeConstraints {
        flags: 0x00,
        typeset_offset: 255,
        constraint_offset: 0,
    },
];

// Table of value type sets.
const TYPE_SETS: [ir::instructions::ValueTypeSet; 22] = [
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={8, 16, 32, 64, 128}, bools={1, 8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(249),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={32, 64}, refs={32, 64})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(96),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        bools: BitSet::<u8>(249),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(510),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, refs={32, 64})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={16, 32, 64})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(112),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={32, 64})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(96),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={64})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(64),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, bools={1, 8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(0),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(249),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={16}, ints={8}, bools={8})
        lanes: BitSet::<u16>(16),
        ints: BitSet::<u8>(8),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(8),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, refs={32, 64})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(0),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128}, refs={32, 64})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        bools: BitSet::<u8>(249),
        refs: BitSet::<u8>(96),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(255),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        bools: BitSet::<u8>(249),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128}, floats={32, 64}, bools={1, 8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(96),
        bools: BitSet::<u8>(249),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, floats={32, 64})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(0),
        floats: BitSet::<u8>(96),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={4, 8}, ints={16, 32})
        lanes: BitSet::<u16>(12),
        ints: BitSet::<u8>(48),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={8, 16}, ints={8, 16})
        lanes: BitSet::<u16>(24),
        ints: BitSet::<u8>(24),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(240),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(120),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={8, 16, 32, 64})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(120),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
];

// Table of operand constraint sequences.
const OPERAND_CONSTRAINTS: [OperandConstraint; 99] = [
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Concrete(ir::types::FFLAGS),
    OperandConstraint::Same,
    OperandConstraint::LaneOf,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I8X16),
    OperandConstraint::Concrete(ir::types::I8X16),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::LaneOf,
    OperandConstraint::Same,
    OperandConstraint::Free(2),
    OperandConstraint::Concrete(ir::types::I64),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I64),
    OperandConstraint::Free(2),
    OperandConstraint::Concrete(ir::types::I16X8),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I32X4),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I64X2),
    OperandConstraint::Same,
    OperandConstraint::Free(8),
    OperandConstraint::Concrete(ir::types::F32),
    OperandConstraint::Concrete(ir::types::F64),
    OperandConstraint::Same,
    OperandConstraint::Free(0),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::SARG_T),
    OperandConstraint::HalfVector,
    OperandConstraint::HalfVector,
    OperandConstraint::Same,
    OperandConstraint::DoubleVector,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::AsBool,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::B1),
    OperandConstraint::Same,
    OperandConstraint::Free(3),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::B1),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::B1),
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Free(1),
    OperandConstraint::Concrete(ir::types::FFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::B1),
    OperandConstraint::Concrete(ir::types::IFLAGS),
    OperandConstraint::Concrete(ir::types::B1),
    OperandConstraint::Concrete(ir::types::FFLAGS),
    OperandConstraint::Same,
    OperandConstraint::Free(6),
    OperandConstraint::Same,
    OperandConstraint::Free(13),
    OperandConstraint::Same,
    OperandConstraint::Free(10),
    OperandConstraint::Same,
    OperandConstraint::Free(4),
    OperandConstraint::SplitLanes,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::MergeLanes,
    OperandConstraint::Same,
    OperandConstraint::Concrete(ir::types::I32X4),
    OperandConstraint::Concrete(ir::types::I16X8),
    OperandConstraint::Concrete(ir::types::I16X8),
    OperandConstraint::Same,
    OperandConstraint::Free(16),
    OperandConstraint::HalfWidth,
    OperandConstraint::HalfWidth,
    OperandConstraint::Same,
    OperandConstraint::DoubleWidth,
    OperandConstraint::Same,
    OperandConstraint::Same,
    OperandConstraint::Free(2),
    OperandConstraint::Same,
    OperandConstraint::Same,
];

impl core::convert::TryFrom<u16> for Opcode {
    type Error = ();
    #[inline]
    fn try_from(x: u16) -> Result<Self, ()> {
        if 0 < x && x <= 231 {
            Ok(unsafe { core::mem::transmute(x) })
        } else {
            Err(())
        }
    }
}
