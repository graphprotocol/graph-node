/// Convenience methods for building instructions.
///
/// The `InstBuilder` trait has one method per instruction opcode for
/// conveniently constructing the instruction with minimum arguments.
/// Polymorphic instructions infer their result types from the input
/// arguments when possible. In some cases, an explicit `ctrl_typevar`
/// argument is required.
///
/// The opcode methods return the new instruction's result values, or
/// the `Inst` itself for instructions that don't have any results.
///
/// There is also a method per instruction format. These methods all
/// return an `Inst`.
pub trait InstBuilder<'f>: InstBuilderBase<'f> {
    /// Jump.
    ///
    /// Unconditionally jump to a basic block, passing the specified
    /// block arguments. The number and types of arguments must match the
    /// destination block.
    ///
    /// Inputs:
    ///
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn jump(mut self, block: ir::Block, args: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Jump(Opcode::Jump, types::INVALID, block, vlist).0
    }

    /// Fall through to the next block.
    ///
    /// This is the same as `jump`, except the destination block must be
    /// the next one in the layout.
    ///
    /// Jumps are turned into fall-through instructions by the branch
    /// relaxation pass. There is no reason to use this instruction outside
    /// that pass.
    ///
    /// Inputs:
    ///
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn fallthrough(mut self, block: ir::Block, args: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Jump(Opcode::Fallthrough, types::INVALID, block, vlist).0
    }

    /// Branch when zero.
    ///
    /// If ``c`` is a `b1` value, take the branch when ``c`` is false. If
    /// ``c`` is an integer value, take the branch when ``c = 0``.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn brz(mut self, c: ir::Value, block: ir::Block, args: &[Value]) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(c, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Branch(Opcode::Brz, ctrl_typevar, block, vlist).0
    }

    /// Branch when non-zero.
    ///
    /// If ``c`` is a `b1` value, take the branch when ``c`` is true. If
    /// ``c`` is an integer value, take the branch when ``c != 0``.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn brnz(mut self, c: ir::Value, block: ir::Block, args: &[Value]) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(c, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Branch(Opcode::Brnz, ctrl_typevar, block, vlist).0
    }

    /// Compare scalar integers and branch.
    ///
    /// Compare ``x`` and ``y`` in the same way as the `icmp` instruction
    /// and take the branch if the condition is true:
    ///
    /// ```text
    ///     br_icmp ugt v1, v2, block4(v5, v6)
    /// ```
    ///
    /// is semantically equivalent to:
    ///
    /// ```text
    ///     v10 = icmp ugt, v1, v2
    ///     brnz v10, block4(v5, v6)
    /// ```
    ///
    /// Some RISC architectures like MIPS and RISC-V provide instructions that
    /// implement all or some of the condition codes. The instruction can also
    /// be used to represent *macro-op fusion* on architectures like Intel's.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn br_icmp<T1: Into<ir::condcodes::IntCC>>(mut self, Cond: T1, x: ir::Value, y: ir::Value, block: ir::Block, args: &[Value]) -> Inst {
        let Cond = Cond.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(x, pool);
            vlist.push(y, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.BranchIcmp(Opcode::BrIcmp, ctrl_typevar, Cond, block, vlist).0
    }

    /// Branch when condition is true in integer CPU flags.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn brif<T1: Into<ir::condcodes::IntCC>>(mut self, Cond: T1, f: ir::Value, block: ir::Block, args: &[Value]) -> Inst {
        let Cond = Cond.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(f, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.BranchInt(Opcode::Brif, types::INVALID, Cond, block, vlist).0
    }

    /// Branch when condition is true in floating point CPU flags.
    ///
    /// Inputs:
    ///
    /// - Cond: A floating point comparison condition code
    /// - f: CPU flags representing the result of a floating point comparison. These
    /// flags can be tested with a :type:`floatcc` condition code.
    /// - block: Destination basic block
    /// - args: block arguments
    #[allow(non_snake_case)]
    fn brff<T1: Into<ir::condcodes::FloatCC>>(mut self, Cond: T1, f: ir::Value, block: ir::Block, args: &[Value]) -> Inst {
        let Cond = Cond.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(f, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.BranchFloat(Opcode::Brff, types::INVALID, Cond, block, vlist).0
    }

    /// Indirect branch via jump table.
    ///
    /// Use ``x`` as an unsigned index into the jump table ``JT``. If a jump
    /// table entry is found, branch to the corresponding block. If no entry was
    /// found or the index is out-of-bounds, branch to the given default block.
    ///
    /// Note that this branch instruction can't pass arguments to the targeted
    /// blocks. Split critical edges as needed to work around this.
    ///
    /// Do not confuse this with "tables" in WebAssembly. ``br_table`` is for
    /// jump tables with destinations within the current function only -- think
    /// of a ``match`` in Rust or a ``switch`` in C.  If you want to call a
    /// function in a dynamic library, that will typically use
    /// ``call_indirect``.
    ///
    /// Inputs:
    ///
    /// - x: index into jump table
    /// - block: Destination basic block
    /// - JT: A jump table.
    #[allow(non_snake_case)]
    fn br_table(self, x: ir::Value, block: ir::Block, JT: ir::JumpTable) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.BranchTable(Opcode::BrTable, ctrl_typevar, block, JT, x).0
    }

    /// Get an entry from a jump table.
    ///
    /// Load a serialized ``entry`` from a jump table ``JT`` at a given index
    /// ``addr`` with a specific ``Size``. The retrieved entry may need to be
    /// decoded after loading, depending upon the jump table type used.
    ///
    /// Currently, the only type supported is entries which are relative to the
    /// base of the jump table.
    ///
    /// Inputs:
    ///
    /// - x: index into jump table
    /// - addr: An integer address type
    /// - Size: Size in bytes
    /// - JT: A jump table.
    ///
    /// Outputs:
    ///
    /// - entry: entry of jump table
    #[allow(non_snake_case)]
    fn jump_table_entry<T1: Into<ir::immediates::Uimm8>>(self, x: ir::Value, addr: ir::Value, Size: T1, JT: ir::JumpTable) -> Value {
        let Size = Size.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BranchTableEntry(Opcode::JumpTableEntry, ctrl_typevar, Size, JT, x, addr);
        dfg.first_result(inst)
    }

    /// Get the absolute base address of a jump table.
    ///
    /// This is used for jump tables wherein the entries are stored relative to
    /// the base of jump table. In order to use these, generated code should first
    /// load an entry using ``jump_table_entry``, then use this instruction to add
    /// the relative base back to it.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - JT: A jump table.
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn jump_table_base(self, iAddr: crate::ir::Type, JT: ir::JumpTable) -> Value {
        let (inst, dfg) = self.BranchTableBase(Opcode::JumpTableBase, iAddr, JT);
        dfg.first_result(inst)
    }

    /// Branch indirectly via a jump table entry.
    ///
    /// Unconditionally jump via a jump table entry that was previously loaded
    /// with the ``jump_table_entry`` instruction.
    ///
    /// Inputs:
    ///
    /// - addr: An integer address type
    /// - JT: A jump table.
    #[allow(non_snake_case)]
    fn indirect_jump_table_br(self, addr: ir::Value, JT: ir::JumpTable) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(addr);
        self.IndirectJump(Opcode::IndirectJumpTableBr, ctrl_typevar, JT, addr).0
    }

    /// Encodes an assembly debug trap.
    #[allow(non_snake_case)]
    fn debugtrap(self) -> Inst {
        self.NullAry(Opcode::Debugtrap, types::INVALID).0
    }

    /// Terminate execution unconditionally.
    ///
    /// Inputs:
    ///
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trap<T1: Into<ir::TrapCode>>(self, code: T1) -> Inst {
        let code = code.into();
        self.Trap(Opcode::Trap, types::INVALID, code).0
    }

    /// Trap when zero.
    ///
    /// if ``c`` is non-zero, execution continues at the following instruction.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trapz<T1: Into<ir::TrapCode>>(self, c: ir::Value, code: T1) -> Inst {
        let code = code.into();
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        self.CondTrap(Opcode::Trapz, ctrl_typevar, code, c).0
    }

    /// A resumable trap.
    ///
    /// This instruction allows non-conditional traps to be used as non-terminal instructions.
    ///
    /// Inputs:
    ///
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn resumable_trap<T1: Into<ir::TrapCode>>(self, code: T1) -> Inst {
        let code = code.into();
        self.Trap(Opcode::ResumableTrap, types::INVALID, code).0
    }

    /// Trap when non-zero.
    ///
    /// If ``c`` is zero, execution continues at the following instruction.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trapnz<T1: Into<ir::TrapCode>>(self, c: ir::Value, code: T1) -> Inst {
        let code = code.into();
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        self.CondTrap(Opcode::Trapnz, ctrl_typevar, code, c).0
    }

    /// A resumable trap to be called when the passed condition is non-zero.
    ///
    /// If ``c`` is zero, execution continues at the following instruction.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn resumable_trapnz<T1: Into<ir::TrapCode>>(self, c: ir::Value, code: T1) -> Inst {
        let code = code.into();
        let ctrl_typevar = self.data_flow_graph().value_type(c);
        self.CondTrap(Opcode::ResumableTrapnz, ctrl_typevar, code, c).0
    }

    /// Trap when condition is true in integer CPU flags.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trapif<T1: Into<ir::condcodes::IntCC>, T2: Into<ir::TrapCode>>(self, Cond: T1, f: ir::Value, code: T2) -> Inst {
        let Cond = Cond.into();
        let code = code.into();
        self.IntCondTrap(Opcode::Trapif, types::INVALID, Cond, code, f).0
    }

    /// Trap when condition is true in floating point CPU flags.
    ///
    /// Inputs:
    ///
    /// - Cond: A floating point comparison condition code
    /// - f: CPU flags representing the result of a floating point comparison. These
    /// flags can be tested with a :type:`floatcc` condition code.
    /// - code: A trap reason code.
    #[allow(non_snake_case)]
    fn trapff<T1: Into<ir::condcodes::FloatCC>, T2: Into<ir::TrapCode>>(self, Cond: T1, f: ir::Value, code: T2) -> Inst {
        let Cond = Cond.into();
        let code = code.into();
        self.FloatCondTrap(Opcode::Trapff, types::INVALID, Cond, code, f).0
    }

    /// Return from the function.
    ///
    /// Unconditionally transfer control to the calling function, passing the
    /// provided return values. The list of return values must match the
    /// function signature's return types.
    ///
    /// Inputs:
    ///
    /// - rvals: return values
    #[allow(non_snake_case)]
    fn return_(mut self, rvals: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(rvals.iter().cloned(), pool);
        }
        self.MultiAry(Opcode::Return, types::INVALID, vlist).0
    }

    /// Return from the function by fallthrough.
    ///
    /// This is a specialized instruction for use where one wants to append
    /// a custom epilogue, which will then perform the real return. This
    /// instruction has no encoding.
    ///
    /// Inputs:
    ///
    /// - rvals: return values
    #[allow(non_snake_case)]
    fn fallthrough_return(mut self, rvals: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(rvals.iter().cloned(), pool);
        }
        self.MultiAry(Opcode::FallthroughReturn, types::INVALID, vlist).0
    }

    /// Direct function call.
    ///
    /// Call a function which has been declared in the preamble. The argument
    /// types must match the function's signature.
    ///
    /// Inputs:
    ///
    /// - FN: function to call, declared by `function`
    /// - args: call arguments
    ///
    /// Outputs:
    ///
    /// - rvals: return values
    #[allow(non_snake_case)]
    fn call(mut self, FN: ir::FuncRef, args: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        self.Call(Opcode::Call, types::INVALID, FN, vlist).0
    }

    /// Indirect function call.
    ///
    /// Call the function pointed to by `callee` with the given arguments. The
    /// called function must match the specified signature.
    ///
    /// Note that this is different from WebAssembly's ``call_indirect``; the
    /// callee is a native address, rather than a table index. For WebAssembly,
    /// `table_addr` and `load` are used to obtain a native address
    /// from a table.
    ///
    /// Inputs:
    ///
    /// - SIG: function signature
    /// - callee: address of function to call
    /// - args: call arguments
    ///
    /// Outputs:
    ///
    /// - rvals: return values
    #[allow(non_snake_case)]
    fn call_indirect(mut self, SIG: ir::SigRef, callee: ir::Value, args: &[Value]) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(callee);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(callee, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.CallIndirect(Opcode::CallIndirect, ctrl_typevar, SIG, vlist).0
    }

    /// Get the address of a function.
    ///
    /// Compute the absolute address of a function declared in the preamble.
    /// The returned address can be used as a ``callee`` argument to
    /// `call_indirect`. This is also a method for calling functions that
    /// are too far away to be addressable by a direct `call`
    /// instruction.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - FN: function to call, declared by `function`
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn func_addr(self, iAddr: crate::ir::Type, FN: ir::FuncRef) -> Value {
        let (inst, dfg) = self.FuncAddr(Opcode::FuncAddr, iAddr, FN);
        dfg.first_result(inst)
    }

    /// Vector splat.
    ///
    /// Return a vector whose lanes are all ``x``.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - x: Value to splat to all lanes
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn splat(self, TxN: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Splat, TxN, x);
        dfg.first_result(inst)
    }

    /// Vector swizzle.
    ///
    /// Returns a new vector with byte-width lanes selected from the lanes of the first input
    /// vector ``x`` specified in the second input vector ``s``. The indices ``i`` in range
    /// ``[0, 15]`` select the ``i``-th element of ``x``. For indices outside of the range the
    /// resulting lane is 0. Note that this operates on byte-width lanes.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - x: Vector to modify by re-arranging lanes
    /// - y: Mask for re-arranging lanes
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn swizzle(self, TxN: crate::ir::Type, x: ir::Value, y: ir::Value) -> Value {
        let (inst, dfg) = self.Binary(Opcode::Swizzle, TxN, x, y);
        dfg.first_result(inst)
    }

    /// Insert ``y`` as lane ``Idx`` in x.
    ///
    /// The lane index, ``Idx``, is an immediate value, not an SSA value. It
    /// must indicate a valid lane index for the type of ``x``.
    ///
    /// Inputs:
    ///
    /// - x: The vector to modify
    /// - y: New lane value
    /// - Idx: Lane index
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn insertlane<T1: Into<ir::immediates::Uimm8>>(self, x: ir::Value, y: ir::Value, Idx: T1) -> Value {
        let Idx = Idx.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.TernaryImm8(Opcode::Insertlane, ctrl_typevar, Idx, x, y);
        dfg.first_result(inst)
    }

    /// Extract lane ``Idx`` from ``x``.
    ///
    /// The lane index, ``Idx``, is an immediate value, not an SSA value. It
    /// must indicate a valid lane index for the type of ``x``. Note that the upper bits of ``a``
    /// may or may not be zeroed depending on the ISA but the type system should prevent using
    /// ``a`` as anything other than the extracted value.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type
    /// - Idx: Lane index
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn extractlane<T1: Into<ir::immediates::Uimm8>>(self, x: ir::Value, Idx: T1) -> Value {
        let Idx = Idx.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm8(Opcode::Extractlane, ctrl_typevar, Idx, x);
        dfg.first_result(inst)
    }

    /// Signed integer minimum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn imin(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Imin, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer minimum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn umin(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Umin, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer maximum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn imax(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Imax, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer maximum.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn umax(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Umax, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned average with rounding: `a := (x + y + 1) // 2`
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn avg_round(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::AvgRound, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Add with unsigned saturation.
    ///
    /// This is similar to `iadd` but the operands are interpreted as unsigned integers and their
    /// summed result, instead of wrapping, will be saturated to the highest unsigned integer for
    /// the controlling type (e.g. `0xFF` for i8).
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn uadd_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::UaddSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Add with signed saturation.
    ///
    /// This is similar to `iadd` but the operands are interpreted as signed integers and their
    /// summed result, instead of wrapping, will be saturated to the lowest or highest
    /// signed integer for the controlling type (e.g. `0x80` or `0x7F` for i8). For example,
    /// since an `sadd_sat.i8` of `0x70` and `0x70` is greater than `0x7F`, the result will be
    /// clamped to `0x7F`.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn sadd_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::SaddSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Subtract with unsigned saturation.
    ///
    /// This is similar to `isub` but the operands are interpreted as unsigned integers and their
    /// difference, instead of wrapping, will be saturated to the lowest unsigned integer for
    /// the controlling type (e.g. `0x00` for i8).
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn usub_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::UsubSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Subtract with signed saturation.
    ///
    /// This is similar to `isub` but the operands are interpreted as signed integers and their
    /// difference, instead of wrapping, will be saturated to the lowest or highest
    /// signed integer for the controlling type (e.g. `0x80` or `0x7F` for i8).
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integers
    /// - y: A SIMD vector type containing integers
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type containing integers
    #[allow(non_snake_case)]
    fn ssub_sat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::SsubSat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Load from memory at ``p + Offset``.
    ///
    /// This is a polymorphic instruction that can load any value type which
    /// has a memory representation.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn load<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, Mem: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Load, Mem, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load from memory at ``sum(args) + Offset``.
    ///
    /// This is a polymorphic instruction that can load any value type which
    /// has a memory representation.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn load_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, Mem: crate::ir::Type, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::LoadComplex, Mem, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Store ``x`` to memory at ``p + Offset``.
    ///
    /// This is a polymorphic instruction that can store any value type with a
    /// memory representation.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: Value to be stored
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn store<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Store, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Store ``x`` to memory at ``sum(args) + Offset``.
    ///
    /// This is a polymorphic instruction that can store any value type with a
    /// memory representation.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: Value to be stored
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn store_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, x: ir::Value, args: &[Value], Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(x, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.StoreComplex(Opcode::StoreComplex, ctrl_typevar, MemFlags, Offset, vlist).0
    }

    /// Load 8 bits from memory at ``p + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i8`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - iExt8 (controlling type variable): An integer type with more than 8 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 8 bits
    #[allow(non_snake_case)]
    fn uload8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt8: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Uload8, iExt8, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 8 bits from memory at ``sum(args) + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i8`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - iExt8 (controlling type variable): An integer type with more than 8 bits
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 8 bits
    #[allow(non_snake_case)]
    fn uload8_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, iExt8: crate::ir::Type, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Uload8Complex, iExt8, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load 8 bits from memory at ``p + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i8`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - iExt8 (controlling type variable): An integer type with more than 8 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 8 bits
    #[allow(non_snake_case)]
    fn sload8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt8: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Sload8, iExt8, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 8 bits from memory at ``sum(args) + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i8`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - iExt8 (controlling type variable): An integer type with more than 8 bits
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 8 bits
    #[allow(non_snake_case)]
    fn sload8_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, iExt8: crate::ir::Type, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Sload8Complex, iExt8, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Store the low 8 bits of ``x`` to memory at ``p + Offset``.
    ///
    /// This is equivalent to ``ireduce.i8`` followed by ``store.i8``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 8 bits
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Istore8, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Store the low 8 bits of ``x`` to memory at ``sum(args) + Offset``.
    ///
    /// This is equivalent to ``ireduce.i8`` followed by ``store.i8``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 8 bits
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore8_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, x: ir::Value, args: &[Value], Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(x, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.StoreComplex(Opcode::Istore8Complex, ctrl_typevar, MemFlags, Offset, vlist).0
    }

    /// Load 16 bits from memory at ``p + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i16`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - iExt16 (controlling type variable): An integer type with more than 16 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 16 bits
    #[allow(non_snake_case)]
    fn uload16<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt16: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Uload16, iExt16, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 16 bits from memory at ``sum(args) + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i16`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - iExt16 (controlling type variable): An integer type with more than 16 bits
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 16 bits
    #[allow(non_snake_case)]
    fn uload16_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, iExt16: crate::ir::Type, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Uload16Complex, iExt16, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load 16 bits from memory at ``p + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i16`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - iExt16 (controlling type variable): An integer type with more than 16 bits
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 16 bits
    #[allow(non_snake_case)]
    fn sload16<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, iExt16: crate::ir::Type, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let (inst, dfg) = self.Load(Opcode::Sload16, iExt16, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 16 bits from memory at ``sum(args) + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i16`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - iExt16 (controlling type variable): An integer type with more than 16 bits
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 16 bits
    #[allow(non_snake_case)]
    fn sload16_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, iExt16: crate::ir::Type, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Sload16Complex, iExt16, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Store the low 16 bits of ``x`` to memory at ``p + Offset``.
    ///
    /// This is equivalent to ``ireduce.i16`` followed by ``store.i16``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 16 bits
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore16<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Istore16, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Store the low 16 bits of ``x`` to memory at ``sum(args) + Offset``.
    ///
    /// This is equivalent to ``ireduce.i16`` followed by ``store.i16``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 16 bits
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore16_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, x: ir::Value, args: &[Value], Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(x, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.StoreComplex(Opcode::Istore16Complex, ctrl_typevar, MemFlags, Offset, vlist).0
    }

    /// Load 32 bits from memory at ``p + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i32`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 32 bits
    #[allow(non_snake_case)]
    fn uload32<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload32, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 32 bits from memory at ``sum(args) + Offset`` and zero-extend.
    ///
    /// This is equivalent to ``load.i32`` followed by ``uextend``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 32 bits
    #[allow(non_snake_case)]
    fn uload32_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Uload32Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load 32 bits from memory at ``p + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i32`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 32 bits
    #[allow(non_snake_case)]
    fn sload32<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload32, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load 32 bits from memory at ``sum(args) + Offset`` and sign-extend.
    ///
    /// This is equivalent to ``load.i32`` followed by ``sextend``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: An integer type with more than 32 bits
    #[allow(non_snake_case)]
    fn sload32_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Sload32Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Store the low 32 bits of ``x`` to memory at ``p + Offset``.
    ///
    /// This is equivalent to ``ireduce.i32`` followed by ``store.i32``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 32 bits
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore32<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, x: ir::Value, p: ir::Value, Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.Store(Opcode::Istore32, ctrl_typevar, MemFlags, Offset, x, p).0
    }

    /// Store the low 32 bits of ``x`` to memory at ``sum(args) + Offset``.
    ///
    /// This is equivalent to ``ireduce.i32`` followed by ``store.i32``.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: An integer type with more than 32 bits
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    #[allow(non_snake_case)]
    fn istore32_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, x: ir::Value, args: &[Value], Offset: T2) -> Inst {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.push(x, pool);
            vlist.extend(args.iter().cloned(), pool);
        }
        self.StoreComplex(Opcode::Istore32Complex, types::INVALID, MemFlags, Offset, vlist).0
    }

    /// Load an 8x8 vector (64 bits) from memory at ``p + Offset`` and zero-extend into an i16x8
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload8x8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload8x8, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load an 8x8 vector (64 bits) from memory at ``sum(args) + Offset`` and zero-extend into an
    /// i16x8 vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload8x8_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Uload8x8Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load an 8x8 vector (64 bits) from memory at ``p + Offset`` and sign-extend into an i16x8
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload8x8<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload8x8, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load an 8x8 vector (64 bits) from memory at ``sum(args) + Offset`` and sign-extend into an
    /// i16x8 vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload8x8_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Sload8x8Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load a 16x4 vector (64 bits) from memory at ``p + Offset`` and zero-extend into an i32x4
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload16x4<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload16x4, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a 16x4 vector (64 bits) from memory at ``sum(args) + Offset`` and zero-extend into an
    /// i32x4 vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload16x4_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Uload16x4Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load a 16x4 vector (64 bits) from memory at ``p + Offset`` and sign-extend into an i32x4
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload16x4<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload16x4, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a 16x4 vector (64 bits) from memory at ``sum(args) + Offset`` and sign-extend into an
    /// i32x4 vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload16x4_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Sload16x4Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load an 32x2 vector (64 bits) from memory at ``p + Offset`` and zero-extend into an i64x2
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload32x2<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Uload32x2, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a 32x2 vector (64 bits) from memory at ``sum(args) + Offset`` and zero-extend into an
    /// i64x2 vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn uload32x2_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Uload32x2Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load a 32x2 vector (64 bits) from memory at ``p + Offset`` and sign-extend into an i64x2
    /// vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload32x2<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(self, MemFlags: T1, p: ir::Value, Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(p);
        let (inst, dfg) = self.Load(Opcode::Sload32x2, ctrl_typevar, MemFlags, Offset, p);
        dfg.first_result(inst)
    }

    /// Load a 32x2 vector (64 bits) from memory at ``sum(args) + Offset`` and sign-extend into an
    /// i64x2 vector.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - args: Address arguments
    /// - Offset: Byte offset from base address
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn sload32x2_complex<T1: Into<ir::MemFlags>, T2: Into<ir::immediates::Offset32>>(mut self, MemFlags: T1, args: &[Value], Offset: T2) -> Value {
        let MemFlags = MemFlags.into();
        let Offset = Offset.into();
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        let (inst, dfg) = self.LoadComplex(Opcode::Sload32x2Complex, types::INVALID, MemFlags, Offset, vlist);
        dfg.first_result(inst)
    }

    /// Load a value from a stack slot at the constant offset.
    ///
    /// This is a polymorphic instruction that can load any value type which
    /// has a memory representation.
    ///
    /// The offset is an immediate constant, not an SSA value. The memory
    /// access cannot go out of bounds, i.e.
    /// `sizeof(a) + Offset <= sizeof(SS)`.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - SS: A stack slot
    /// - Offset: In-bounds offset into stack slot
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn stack_load<T1: Into<ir::immediates::Offset32>>(self, Mem: crate::ir::Type, SS: ir::StackSlot, Offset: T1) -> Value {
        let Offset = Offset.into();
        let (inst, dfg) = self.StackLoad(Opcode::StackLoad, Mem, SS, Offset);
        dfg.first_result(inst)
    }

    /// Store a value to a stack slot at a constant offset.
    ///
    /// This is a polymorphic instruction that can store any value type with a
    /// memory representation.
    ///
    /// The offset is an immediate constant, not an SSA value. The memory
    /// access cannot go out of bounds, i.e.
    /// `sizeof(a) + Offset <= sizeof(SS)`.
    ///
    /// Inputs:
    ///
    /// - x: Value to be stored
    /// - SS: A stack slot
    /// - Offset: In-bounds offset into stack slot
    #[allow(non_snake_case)]
    fn stack_store<T1: Into<ir::immediates::Offset32>>(self, x: ir::Value, SS: ir::StackSlot, Offset: T1) -> Inst {
        let Offset = Offset.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.StackStore(Opcode::StackStore, ctrl_typevar, SS, Offset, x).0
    }

    /// Get the address of a stack slot.
    ///
    /// Compute the absolute address of a byte in a stack slot. The offset must
    /// refer to a byte inside the stack slot:
    /// `0 <= Offset < sizeof(SS)`.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - SS: A stack slot
    /// - Offset: In-bounds offset into stack slot
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn stack_addr<T1: Into<ir::immediates::Offset32>>(self, iAddr: crate::ir::Type, SS: ir::StackSlot, Offset: T1) -> Value {
        let Offset = Offset.into();
        let (inst, dfg) = self.StackLoad(Opcode::StackAddr, iAddr, SS, Offset);
        dfg.first_result(inst)
    }

    /// Compute the value of global GV.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - GV: A global value.
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn global_value(self, Mem: crate::ir::Type, GV: ir::GlobalValue) -> Value {
        let (inst, dfg) = self.UnaryGlobalValue(Opcode::GlobalValue, Mem, GV);
        dfg.first_result(inst)
    }

    /// Compute the value of global GV, which is a symbolic value.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - GV: A global value.
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn symbol_value(self, Mem: crate::ir::Type, GV: ir::GlobalValue) -> Value {
        let (inst, dfg) = self.UnaryGlobalValue(Opcode::SymbolValue, Mem, GV);
        dfg.first_result(inst)
    }

    /// Compute the value of global GV, which is a TLS (thread local storage) value.
    ///
    /// Inputs:
    ///
    /// - Mem (controlling type variable): Any type that can be stored in memory
    /// - GV: A global value.
    ///
    /// Outputs:
    ///
    /// - a: Value loaded
    #[allow(non_snake_case)]
    fn tls_value(self, Mem: crate::ir::Type, GV: ir::GlobalValue) -> Value {
        let (inst, dfg) = self.UnaryGlobalValue(Opcode::TlsValue, Mem, GV);
        dfg.first_result(inst)
    }

    /// Bounds check and compute absolute address of heap memory.
    ///
    /// Verify that the offset range ``p .. p + Size - 1`` is in bounds for the
    /// heap H, and generate an absolute address that is safe to dereference.
    ///
    /// 1. If ``p + Size`` is not greater than the heap bound, return an
    ///    absolute address corresponding to a byte offset of ``p`` from the
    ///    heap's base address.
    /// 2. If ``p + Size`` is greater than the heap bound, generate a trap.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - H: A heap.
    /// - p: An unsigned heap offset
    /// - Size: Size in bytes
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn heap_addr<T1: Into<ir::immediates::Uimm32>>(self, iAddr: crate::ir::Type, H: ir::Heap, p: ir::Value, Size: T1) -> Value {
        let Size = Size.into();
        let (inst, dfg) = self.HeapAddr(Opcode::HeapAddr, iAddr, H, Size, p);
        dfg.first_result(inst)
    }

    /// Gets the content of the pinned register, when it's enabled.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn get_pinned_reg(self, iAddr: crate::ir::Type) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::GetPinnedReg, iAddr);
        dfg.first_result(inst)
    }

    /// Sets the content of the pinned register, when it's enabled.
    ///
    /// Inputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn set_pinned_reg(self, addr: ir::Value) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(addr);
        self.Unary(Opcode::SetPinnedReg, ctrl_typevar, addr).0
    }

    /// Bounds check and compute absolute address of a table entry.
    ///
    /// Verify that the offset ``p`` is in bounds for the table T, and generate
    /// an absolute address that is safe to dereference.
    ///
    /// ``Offset`` must be less than the size of a table element.
    ///
    /// 1. If ``p`` is not greater than the table bound, return an absolute
    ///    address corresponding to a byte offset of ``p`` from the table's
    ///    base address.
    /// 2. If ``p`` is greater than the table bound, generate a trap.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - T: A table.
    /// - p: An unsigned table offset
    /// - Offset: Byte offset from element address
    ///
    /// Outputs:
    ///
    /// - addr: An integer address type
    #[allow(non_snake_case)]
    fn table_addr<T1: Into<ir::immediates::Offset32>>(self, iAddr: crate::ir::Type, T: ir::Table, p: ir::Value, Offset: T1) -> Value {
        let Offset = Offset.into();
        let (inst, dfg) = self.TableAddr(Opcode::TableAddr, iAddr, T, Offset, p);
        dfg.first_result(inst)
    }

    /// Integer constant.
    ///
    /// Create a scalar integer SSA value with an immediate constant value, or
    /// an integer vector where all the lanes have the same value.
    ///
    /// Inputs:
    ///
    /// - Int (controlling type variable): A scalar or vector integer type
    /// - N: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A constant integer scalar or vector value
    #[allow(non_snake_case)]
    fn iconst<T1: Into<ir::immediates::Imm64>>(self, Int: crate::ir::Type, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryImm(Opcode::Iconst, Int, N);
        dfg.first_result(inst)
    }

    /// Floating point constant.
    ///
    /// Create a `f32` SSA value with an immediate constant value.
    ///
    /// Inputs:
    ///
    /// - N: A 32-bit immediate floating point number.
    ///
    /// Outputs:
    ///
    /// - a: A constant f32 scalar value
    #[allow(non_snake_case)]
    fn f32const<T1: Into<ir::immediates::Ieee32>>(self, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryIeee32(Opcode::F32const, types::INVALID, N);
        dfg.first_result(inst)
    }

    /// Floating point constant.
    ///
    /// Create a `f64` SSA value with an immediate constant value.
    ///
    /// Inputs:
    ///
    /// - N: A 64-bit immediate floating point number.
    ///
    /// Outputs:
    ///
    /// - a: A constant f64 scalar value
    #[allow(non_snake_case)]
    fn f64const<T1: Into<ir::immediates::Ieee64>>(self, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryIeee64(Opcode::F64const, types::INVALID, N);
        dfg.first_result(inst)
    }

    /// Boolean constant.
    ///
    /// Create a scalar boolean SSA value with an immediate constant value, or
    /// a boolean vector where all the lanes have the same value.
    ///
    /// Inputs:
    ///
    /// - Bool (controlling type variable): A scalar or vector boolean type
    /// - N: An immediate boolean.
    ///
    /// Outputs:
    ///
    /// - a: A constant boolean scalar or vector value
    #[allow(non_snake_case)]
    fn bconst<T1: Into<bool>>(self, Bool: crate::ir::Type, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryBool(Opcode::Bconst, Bool, N);
        dfg.first_result(inst)
    }

    /// SIMD vector constant.
    ///
    /// Construct a vector with the given immediate bytes.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - N: The 16 immediate bytes of a 128-bit vector
    ///
    /// Outputs:
    ///
    /// - a: A constant vector value
    #[allow(non_snake_case)]
    fn vconst<T1: Into<ir::Constant>>(self, TxN: crate::ir::Type, N: T1) -> Value {
        let N = N.into();
        let (inst, dfg) = self.UnaryConst(Opcode::Vconst, TxN, N);
        dfg.first_result(inst)
    }

    /// Calculate the base address of a value in the constant pool.
    ///
    /// Inputs:
    ///
    /// - iAddr (controlling type variable): An integer address type
    /// - constant: A constant in the constant pool
    ///
    /// Outputs:
    ///
    /// - address: An integer address type
    #[allow(non_snake_case)]
    fn const_addr<T1: Into<ir::Constant>>(self, iAddr: crate::ir::Type, constant: T1) -> Value {
        let constant = constant.into();
        let (inst, dfg) = self.UnaryConst(Opcode::ConstAddr, iAddr, constant);
        dfg.first_result(inst)
    }

    /// SIMD vector shuffle.
    ///
    /// Shuffle two vectors using the given immediate bytes. For each of the 16 bytes of the
    /// immediate, a value i of 0-15 selects the i-th element of the first vector and a value i of
    /// 16-31 selects the (i-16)th element of the second vector. Immediate values outside of the
    /// 0-31 range place a 0 in the resulting vector lane.
    ///
    /// Inputs:
    ///
    /// - a: A vector value
    /// - b: A vector value
    /// - mask: The 16 immediate bytes used for selecting the elements to shuffle
    ///
    /// Outputs:
    ///
    /// - a: A vector value
    #[allow(non_snake_case)]
    fn shuffle<T1: Into<ir::Immediate>>(self, a: ir::Value, b: ir::Value, mask: T1) -> Value {
        let mask = mask.into();
        let ctrl_typevar = self.data_flow_graph().value_type(a);
        let (inst, dfg) = self.Shuffle(Opcode::Shuffle, ctrl_typevar, mask, a, b);
        dfg.first_result(inst)
    }

    /// Null constant value for reference types.
    ///
    /// Create a scalar reference SSA value with a constant null value.
    ///
    /// Inputs:
    ///
    /// - Ref (controlling type variable): A scalar reference type
    ///
    /// Outputs:
    ///
    /// - a: A constant reference null value
    #[allow(non_snake_case)]
    fn null(self, Ref: crate::ir::Type) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::Null, Ref);
        dfg.first_result(inst)
    }

    /// Just a dummy instruction.
    ///
    /// Note: this doesn't compile to a machine code nop.
    #[allow(non_snake_case)]
    fn nop(self) -> Inst {
        self.NullAry(Opcode::Nop, types::INVALID).0
    }

    /// Conditional select.
    ///
    /// This instruction selects whole values. Use `vselect` for
    /// lane-wise selection.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - x: Value to use when `c` is true
    /// - y: Value to use when `c` is false
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn select(self, c: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Ternary(Opcode::Select, ctrl_typevar, c, x, y);
        dfg.first_result(inst)
    }

    /// Conditional select, dependent on integer condition codes.
    ///
    /// Inputs:
    ///
    /// - Any (controlling type variable): Any integer, float, boolean, or reference scalar or vector type
    /// - cc: Controlling condition code
    /// - flags: The machine's flag register
    /// - x: Value to use when `c` is true
    /// - y: Value to use when `c` is false
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn selectif<T1: Into<ir::condcodes::IntCC>>(self, Any: crate::ir::Type, cc: T1, flags: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let cc = cc.into();
        let (inst, dfg) = self.IntSelect(Opcode::Selectif, Any, cc, flags, x, y);
        dfg.first_result(inst)
    }

    /// Conditional select intended for Spectre guards.
    ///
    /// This operation is semantically equivalent to a selectif instruction.
    /// However, it is guaranteed to not be removed or otherwise altered by any
    /// optimization pass, and is guaranteed to result in a conditional-move
    /// instruction, not a branch-based lowering.  As such, it is suitable
    /// for use when producing Spectre guards. For example, a bounds-check
    /// may guard against unsafe speculation past a bounds-check conditional
    /// branch by passing the address or index to be accessed through a
    /// conditional move, also gated on the same condition. Because no
    /// Spectre-vulnerable processors are known to perform speculation on
    /// conditional move instructions, this is guaranteed to pick the
    /// correct input. If the selected input in case of overflow is a "safe"
    /// value, for example a null pointer that causes an exception in the
    /// speculative path, this ensures that no Spectre vulnerability will
    /// exist.
    ///
    /// Inputs:
    ///
    /// - Any (controlling type variable): Any integer, float, boolean, or reference scalar or vector type
    /// - cc: Controlling condition code
    /// - flags: The machine's flag register
    /// - x: Value to use when `c` is true
    /// - y: Value to use when `c` is false
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn selectif_spectre_guard<T1: Into<ir::condcodes::IntCC>>(self, Any: crate::ir::Type, cc: T1, flags: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let cc = cc.into();
        let (inst, dfg) = self.IntSelect(Opcode::SelectifSpectreGuard, Any, cc, flags, x, y);
        dfg.first_result(inst)
    }

    /// Conditional select of bits.
    ///
    /// For each bit in `c`, this instruction selects the corresponding bit from `x` if the bit
    /// in `c` is 1 and the corresponding bit from `y` if the bit in `c` is 0. See also:
    /// `select`, `vselect`.
    ///
    /// Inputs:
    ///
    /// - c: Controlling value to test
    /// - x: Value to use when `c` is true
    /// - y: Value to use when `c` is false
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn bitselect(self, c: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Ternary(Opcode::Bitselect, ctrl_typevar, c, x, y);
        dfg.first_result(inst)
    }

    /// Register-register copy.
    ///
    /// This instruction copies its input, preserving the value type.
    ///
    /// A pure SSA-form program does not need to copy values, but this
    /// instruction is useful for representing intermediate stages during
    /// instruction transformations, and the register allocator needs a way of
    /// representing register copies.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn copy(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Copy, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Spill a register value to a stack slot.
    ///
    /// This instruction behaves exactly like `copy`, but the result
    /// value is assigned to a spill slot.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn spill(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Spill, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Load a register value from a stack slot.
    ///
    /// This instruction behaves exactly like `copy`, but creates a new
    /// SSA value for the spilled input value.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn fill(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Fill, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// This is identical to `fill`, except it has no encoding, since it is a no-op.
    ///
    /// This instruction is created only during late-stage redundant-reload removal, after all
    /// registers and stack slots have been assigned.  It is used to replace `fill`s that have
    /// been identified as redundant.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn fill_nop(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::FillNop, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// This creates a sarg_t
    ///
    /// This instruction is internal and should not be created by
    /// Cranelift users.
    ///
    /// Outputs:
    ///
    /// - sarg_t: Any scalar or vector type with at most 128 lanes
    #[allow(non_snake_case)]
    fn dummy_sarg_t(self) -> Value {
        let (inst, dfg) = self.NullAry(Opcode::DummySargT, types::INVALID);
        dfg.first_result(inst)
    }

    /// Temporarily divert ``x`` from ``src`` to ``dst``.
    ///
    /// This instruction moves the location of a value from one register to
    /// another without creating a new SSA value. It is used by the register
    /// allocator to temporarily rearrange register assignments in order to
    /// satisfy instruction constraints.
    ///
    /// The register diversions created by this instruction must be undone
    /// before the value leaves the block. At the entry to a new block, all live
    /// values must be in their originally assigned registers.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    /// - src: A register unit in the target ISA
    /// - dst: A register unit in the target ISA
    #[allow(non_snake_case)]
    fn regmove<T1: Into<isa::RegUnit>, T2: Into<isa::RegUnit>>(self, x: ir::Value, src: T1, dst: T2) -> Inst {
        let src = src.into();
        let dst = dst.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.RegMove(Opcode::Regmove, ctrl_typevar, src, dst, x).0
    }

    /// Copies the contents of ''src'' register to ''dst'' register.
    ///
    /// This instructions copies the contents of one register to another
    /// register without involving any SSA values. This is used for copying
    /// special registers, e.g. copying the stack register to the frame
    /// register in a function prologue.
    ///
    /// Inputs:
    ///
    /// - src: A register unit in the target ISA
    /// - dst: A register unit in the target ISA
    #[allow(non_snake_case)]
    fn copy_special<T1: Into<isa::RegUnit>, T2: Into<isa::RegUnit>>(self, src: T1, dst: T2) -> Inst {
        let src = src.into();
        let dst = dst.into();
        self.CopySpecial(Opcode::CopySpecial, types::INVALID, src, dst).0
    }

    /// Copies the contents of ''src'' register to ''a'' SSA name.
    ///
    /// This instruction copies the contents of one register, regardless of its SSA name, to
    /// another register, creating a new SSA name.  In that sense it is a one-sided version
    /// of ''copy_special''.  This instruction is internal and should not be created by
    /// Cranelift users.
    ///
    /// Inputs:
    ///
    /// - Any (controlling type variable): Any integer, float, boolean, or reference scalar or vector type
    /// - src: A register unit in the target ISA
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn copy_to_ssa<T1: Into<isa::RegUnit>>(self, Any: crate::ir::Type, src: T1) -> Value {
        let src = src.into();
        let (inst, dfg) = self.CopyToSsa(Opcode::CopyToSsa, Any, src);
        dfg.first_result(inst)
    }

    /// Stack-slot-to-the-same-stack-slot copy, which is guaranteed to turn
    /// into a no-op.  This instruction is for use only within Cranelift itself.
    ///
    /// This instruction copies its input, preserving the value type.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, boolean, or reference scalar or vector type
    #[allow(non_snake_case)]
    fn copy_nop(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::CopyNop, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Subtracts ``delta`` offset value from the stack pointer register.
    ///
    /// This instruction is used to adjust the stack pointer by a dynamic amount.
    ///
    /// Inputs:
    ///
    /// - delta: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn adjust_sp_down(self, delta: ir::Value) -> Inst {
        let ctrl_typevar = self.data_flow_graph().value_type(delta);
        self.Unary(Opcode::AdjustSpDown, ctrl_typevar, delta).0
    }

    /// Adds ``Offset`` immediate offset value to the stack pointer register.
    ///
    /// This instruction is used to adjust the stack pointer, primarily in function
    /// prologues and epilogues. ``Offset`` is constrained to the size of a signed
    /// 32-bit integer.
    ///
    /// Inputs:
    ///
    /// - Offset: Offset from current stack pointer
    #[allow(non_snake_case)]
    fn adjust_sp_up_imm<T1: Into<ir::immediates::Imm64>>(self, Offset: T1) -> Inst {
        let Offset = Offset.into();
        self.UnaryImm(Opcode::AdjustSpUpImm, types::INVALID, Offset).0
    }

    /// Subtracts ``Offset`` immediate offset value from the stack pointer
    /// register.
    ///
    /// This instruction is used to adjust the stack pointer, primarily in function
    /// prologues and epilogues. ``Offset`` is constrained to the size of a signed
    /// 32-bit integer.
    ///
    /// Inputs:
    ///
    /// - Offset: Offset from current stack pointer
    #[allow(non_snake_case)]
    fn adjust_sp_down_imm<T1: Into<ir::immediates::Imm64>>(self, Offset: T1) -> Inst {
        let Offset = Offset.into();
        self.UnaryImm(Opcode::AdjustSpDownImm, types::INVALID, Offset).0
    }

    /// Compare ``addr`` with the stack pointer and set the CPU flags.
    ///
    /// This is like `ifcmp` where ``addr`` is the LHS operand and the stack
    /// pointer is the RHS.
    ///
    /// Inputs:
    ///
    /// - addr: An integer address type
    ///
    /// Outputs:
    ///
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn ifcmp_sp(self, addr: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(addr);
        let (inst, dfg) = self.Unary(Opcode::IfcmpSp, ctrl_typevar, addr);
        dfg.first_result(inst)
    }

    /// Temporarily divert ``x`` from ``src`` to ``SS``.
    ///
    /// This instruction moves the location of a value from a register to a
    /// stack slot without creating a new SSA value. It is used by the register
    /// allocator to temporarily rearrange register assignments in order to
    /// satisfy instruction constraints.
    ///
    /// See also `regmove`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    /// - src: A register unit in the target ISA
    /// - SS: A stack slot
    #[allow(non_snake_case)]
    fn regspill<T1: Into<isa::RegUnit>>(self, x: ir::Value, src: T1, SS: ir::StackSlot) -> Inst {
        let src = src.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.RegSpill(Opcode::Regspill, ctrl_typevar, src, SS, x).0
    }

    /// Temporarily divert ``x`` from ``SS`` to ``dst``.
    ///
    /// This instruction moves the location of a value from a stack slot to a
    /// register without creating a new SSA value. It is used by the register
    /// allocator to temporarily rearrange register assignments in order to
    /// satisfy instruction constraints.
    ///
    /// See also `regmove`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    /// - SS: A stack slot
    /// - dst: A register unit in the target ISA
    #[allow(non_snake_case)]
    fn regfill<T1: Into<isa::RegUnit>>(self, x: ir::Value, SS: ir::StackSlot, dst: T1) -> Inst {
        let dst = dst.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.RegFill(Opcode::Regfill, ctrl_typevar, SS, dst, x).0
    }

    /// This instruction will provide live reference values at a point in
    /// the function. It can only be used by the compiler.
    ///
    /// Inputs:
    ///
    /// - args: Variable number of args for StackMap
    #[allow(non_snake_case)]
    fn safepoint(mut self, args: &[Value]) -> Inst {
        let mut vlist = ir::ValueList::default();
        {
            let pool = &mut self.data_flow_graph_mut().value_lists;
            vlist.extend(args.iter().cloned(), pool);
        }
        self.MultiAry(Opcode::Safepoint, types::INVALID, vlist).0
    }

    /// Split a vector into two halves.
    ///
    /// Split the vector `x` into two separate values, each containing half of
    /// the lanes from ``x``. The result may be two scalars if ``x`` only had
    /// two lanes.
    ///
    /// Inputs:
    ///
    /// - x: Vector to split
    ///
    /// Outputs:
    ///
    /// - lo: Low-numbered lanes of `x`
    /// - hi: High-numbered lanes of `x`
    #[allow(non_snake_case)]
    fn vsplit(self, x: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Vsplit, ctrl_typevar, x);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Vector concatenation.
    ///
    /// Return a vector formed by concatenating ``x`` and ``y``. The resulting
    /// vector type has twice as many lanes as each of the inputs. The lanes of
    /// ``x`` appear as the low-numbered lanes, and the lanes of ``y`` become
    /// the high-numbered lanes of ``a``.
    ///
    /// It is possible to form a vector by concatenating two scalars.
    ///
    /// Inputs:
    ///
    /// - x: Low-numbered lanes
    /// - y: High-numbered lanes
    ///
    /// Outputs:
    ///
    /// - a: Concatenation of `x` and `y`
    #[allow(non_snake_case)]
    fn vconcat(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Vconcat, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Vector lane select.
    ///
    /// Select lanes from ``x`` or ``y`` controlled by the lanes of the boolean
    /// vector ``c``.
    ///
    /// Inputs:
    ///
    /// - c: Controlling vector
    /// - x: Value to use where `c` is true
    /// - y: Value to use where `c` is false
    ///
    /// Outputs:
    ///
    /// - a: A SIMD vector type
    #[allow(non_snake_case)]
    fn vselect(self, c: ir::Value, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Ternary(Opcode::Vselect, ctrl_typevar, c, x, y);
        dfg.first_result(inst)
    }

    /// Reduce a vector to a scalar boolean.
    ///
    /// Return a scalar boolean true if any lane in ``a`` is non-zero, false otherwise.
    ///
    /// Inputs:
    ///
    /// - a: A SIMD vector type
    ///
    /// Outputs:
    ///
    /// - s: A boolean type with 1 bits.
    #[allow(non_snake_case)]
    fn vany_true(self, a: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(a);
        let (inst, dfg) = self.Unary(Opcode::VanyTrue, ctrl_typevar, a);
        dfg.first_result(inst)
    }

    /// Reduce a vector to a scalar boolean.
    ///
    /// Return a scalar boolean true if all lanes in ``i`` are non-zero, false otherwise.
    ///
    /// Inputs:
    ///
    /// - a: A SIMD vector type
    ///
    /// Outputs:
    ///
    /// - s: A boolean type with 1 bits.
    #[allow(non_snake_case)]
    fn vall_true(self, a: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(a);
        let (inst, dfg) = self.Unary(Opcode::VallTrue, ctrl_typevar, a);
        dfg.first_result(inst)
    }

    /// Reduce a vector to a scalar integer.
    ///
    /// Return a scalar integer, consisting of the concatenation of the most significant bit
    /// of each lane of ``a``.
    ///
    /// Inputs:
    ///
    /// - Int (controlling type variable): A scalar or vector integer type
    /// - a: A SIMD vector type
    ///
    /// Outputs:
    ///
    /// - x: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn vhigh_bits(self, Int: crate::ir::Type, a: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::VhighBits, Int, a);
        dfg.first_result(inst)
    }

    /// Integer comparison.
    ///
    /// The condition code determines if the operands are interpreted as signed
    /// or unsigned integers.
    ///
    /// | Signed | Unsigned | Condition             |
    /// |--------|----------|-----------------------|
    /// | eq     | eq       | Equal                 |
    /// | ne     | ne       | Not equal             |
    /// | slt    | ult      | Less than             |
    /// | sge    | uge      | Greater than or equal |
    /// | sgt    | ugt      | Greater than          |
    /// | sle    | ule      | Less than or equal    |
    /// | of     | *        | Overflow              |
    /// | nof    | *        | No Overflow           |
    ///
    /// \* The unsigned version of overflow conditions have ISA-specific
    /// semantics and thus have been kept as methods on the TargetIsa trait as
    /// [unsigned_add_overflow_condition][isa::TargetIsa::unsigned_add_overflow_condition] and
    /// [unsigned_sub_overflow_condition][isa::TargetIsa::unsigned_sub_overflow_condition].
    ///
    /// When this instruction compares integer vectors, it returns a boolean
    /// vector of lane-wise comparisons.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn icmp<T1: Into<ir::condcodes::IntCC>>(self, Cond: T1, x: ir::Value, y: ir::Value) -> Value {
        let Cond = Cond.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.IntCompare(Opcode::Icmp, ctrl_typevar, Cond, x, y);
        dfg.first_result(inst)
    }

    /// Compare scalar integer to a constant.
    ///
    /// This is the same as the `icmp` instruction, except one operand is
    /// an immediate constant.
    ///
    /// This instruction can only compare scalars. Use `icmp` for
    /// lane-wise vector comparisons.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A boolean type with 1 bits.
    #[allow(non_snake_case)]
    fn icmp_imm<T1: Into<ir::condcodes::IntCC>, T2: Into<ir::immediates::Imm64>>(self, Cond: T1, x: ir::Value, Y: T2) -> Value {
        let Cond = Cond.into();
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.IntCompareImm(Opcode::IcmpImm, ctrl_typevar, Cond, Y, x);
        dfg.first_result(inst)
    }

    /// Compare scalar integers and return flags.
    ///
    /// Compare two scalar integer values and return integer CPU flags
    /// representing the result.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn ifcmp(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ifcmp, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Compare scalar integer to a constant and return flags.
    ///
    /// Like `icmp_imm`, but returns integer CPU flags instead of testing
    /// a specific condition code.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn ifcmp_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IfcmpImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Wrapping integer addition: `a := x + y \pmod{2^B}`.
    ///
    /// This instruction does not depend on the signed/unsigned interpretation
    /// of the operands.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn iadd(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Iadd, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Wrapping integer subtraction: `a := x - y \pmod{2^B}`.
    ///
    /// This instruction does not depend on the signed/unsigned interpretation
    /// of the operands.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn isub(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Isub, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Integer negation: `a := -x \pmod{2^B}`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ineg(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Ineg, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Integer absolute value with wrapping: `a := |x|`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn iabs(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Iabs, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Wrapping integer multiplication: `a := x y \pmod{2^B}`.
    ///
    /// This instruction does not depend on the signed/unsigned interpretation
    /// of the operands.
    ///
    /// Polymorphic over all integer types (vector and scalar).
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn imul(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Imul, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer multiplication, producing the high half of a
    /// double-length result.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn umulhi(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Umulhi, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer multiplication, producing the high half of a
    /// double-length result.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn smulhi(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Smulhi, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer division: `a := \lfloor {x \over y} \rfloor`.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn udiv(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Udiv, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer division rounded toward zero: `a := sign(xy)
    /// \lfloor {|x| \over |y|}\rfloor`.
    ///
    /// This operation traps if the divisor is zero, or if the result is not
    /// representable in `B` bits two's complement. This only happens
    /// when `x = -2^{B-1}, y = -1`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn sdiv(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Sdiv, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned integer remainder.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn urem(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Urem, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed integer remainder. The result has the sign of the dividend.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector integer type
    /// - y: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn srem(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Srem, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Add immediate integer.
    ///
    /// Same as `iadd`, but one operand is an immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn iadd_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IaddImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Integer multiplication by immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn imul_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::ImulImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Unsigned integer division by an immediate constant.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn udiv_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::UdivImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Signed integer division by an immediate constant.
    ///
    /// This operation traps if the divisor is zero, or if the result is not
    /// representable in `B` bits two's complement. This only happens
    /// when `x = -2^{B-1}, Y = -1`.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn sdiv_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::SdivImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Unsigned integer remainder with immediate divisor.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn urem_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::UremImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Signed integer remainder with immediate divisor.
    ///
    /// This operation traps if the divisor is zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn srem_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::SremImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Immediate reverse wrapping subtraction: `a := Y - x \pmod{2^B}`.
    ///
    /// Also works as integer negation when `Y = 0`. Use `iadd_imm`
    /// with a negative immediate operand for the reverse immediate
    /// subtraction.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn irsub_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IrsubImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Add integers with carry in.
    ///
    /// Same as `iadd` with an additional carry input. Computes:
    ///
    /// ```text
    ///     a = x + y + c_{in} \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: Input carry flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn iadd_cin(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddCin, ctrl_typevar, x, y, c_in);
        dfg.first_result(inst)
    }

    /// Add integers with carry in.
    ///
    /// Same as `iadd` with an additional carry flag input. Computes:
    ///
    /// ```text
    ///     a = x + y + c_{in} \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn iadd_ifcin(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddIfcin, ctrl_typevar, x, y, c_in);
        dfg.first_result(inst)
    }

    /// Add integers with carry out.
    ///
    /// Same as `iadd` with an additional carry output.
    ///
    /// ```text
    ///     a &= x + y \pmod 2^B \\
    ///     c_{out} &= x+y >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: Output carry flag
    #[allow(non_snake_case)]
    fn iadd_cout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IaddCout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Add integers with carry out.
    ///
    /// Same as `iadd` with an additional carry flag output.
    ///
    /// ```text
    ///     a &= x + y \pmod 2^B \\
    ///     c_{out} &= x+y >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn iadd_ifcout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IaddIfcout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Add integers with carry in and out.
    ///
    /// Same as `iadd` with an additional carry input and output.
    ///
    /// ```text
    ///     a &= x + y + c_{in} \pmod 2^B \\
    ///     c_{out} &= x + y + c_{in} >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: Input carry flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: Output carry flag
    #[allow(non_snake_case)]
    fn iadd_carry(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddCarry, ctrl_typevar, x, y, c_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Add integers with carry in and out.
    ///
    /// Same as `iadd` with an additional carry flag input and output.
    ///
    /// ```text
    ///     a &= x + y + c_{in} \pmod 2^B \\
    ///     c_{out} &= x + y + c_{in} >= 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - c_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - c_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn iadd_ifcarry(self, x: ir::Value, y: ir::Value, c_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IaddIfcarry, ctrl_typevar, x, y, c_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Subtract integers with borrow in.
    ///
    /// Same as `isub` with an additional borrow flag input. Computes:
    ///
    /// ```text
    ///     a = x - (y + b_{in}) \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: Input borrow flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn isub_bin(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubBin, ctrl_typevar, x, y, b_in);
        dfg.first_result(inst)
    }

    /// Subtract integers with borrow in.
    ///
    /// Same as `isub` with an additional borrow flag input. Computes:
    ///
    /// ```text
    ///     a = x - (y + b_{in}) \pmod 2^B
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn isub_ifbin(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubIfbin, ctrl_typevar, x, y, b_in);
        dfg.first_result(inst)
    }

    /// Subtract integers with borrow out.
    ///
    /// Same as `isub` with an additional borrow flag output.
    ///
    /// ```text
    ///     a &= x - y \pmod 2^B \\
    ///     b_{out} &= x < y
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: Output borrow flag
    #[allow(non_snake_case)]
    fn isub_bout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IsubBout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Subtract integers with borrow out.
    ///
    /// Same as `isub` with an additional borrow flag output.
    ///
    /// ```text
    ///     a &= x - y \pmod 2^B \\
    ///     b_{out} &= x < y
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn isub_ifbout(self, x: ir::Value, y: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::IsubIfbout, ctrl_typevar, x, y);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Subtract integers with borrow in and out.
    ///
    /// Same as `isub` with an additional borrow flag input and output.
    ///
    /// ```text
    ///     a &= x - (y + b_{in}) \pmod 2^B \\
    ///     b_{out} &= x < y + b_{in}
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: Input borrow flag
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: Output borrow flag
    #[allow(non_snake_case)]
    fn isub_borrow(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubBorrow, ctrl_typevar, x, y, b_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Subtract integers with borrow in and out.
    ///
    /// Same as `isub` with an additional borrow flag input and output.
    ///
    /// ```text
    ///     a &= x - (y + b_{in}) \pmod 2^B \\
    ///     b_{out} &= x < y + b_{in}
    /// ```
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - y: A scalar integer type
    /// - b_in: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    /// - b_out: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    #[allow(non_snake_case)]
    fn isub_ifborrow(self, x: ir::Value, y: ir::Value, b_in: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::IsubIfborrow, ctrl_typevar, x, y, b_in);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Bitwise and.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or boolean scalar or vector type
    /// - y: Any integer, float, or boolean scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or boolean scalar or vector type
    #[allow(non_snake_case)]
    fn band(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Band, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise or.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or boolean scalar or vector type
    /// - y: Any integer, float, or boolean scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or boolean scalar or vector type
    #[allow(non_snake_case)]
    fn bor(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Bor, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise xor.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or boolean scalar or vector type
    /// - y: Any integer, float, or boolean scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or boolean scalar or vector type
    #[allow(non_snake_case)]
    fn bxor(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Bxor, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise not.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or boolean scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or boolean scalar or vector type
    #[allow(non_snake_case)]
    fn bnot(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Bnot, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Bitwise and not.
    ///
    /// Computes `x & ~y`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or boolean scalar or vector type
    /// - y: Any integer, float, or boolean scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or boolean scalar or vector type
    #[allow(non_snake_case)]
    fn band_not(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::BandNot, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise or not.
    ///
    /// Computes `x | ~y`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or boolean scalar or vector type
    /// - y: Any integer, float, or boolean scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or boolean scalar or vector type
    #[allow(non_snake_case)]
    fn bor_not(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::BorNot, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise xor not.
    ///
    /// Computes `x ^ ~y`.
    ///
    /// Inputs:
    ///
    /// - x: Any integer, float, or boolean scalar or vector type
    /// - y: Any integer, float, or boolean scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Any integer, float, or boolean scalar or vector type
    #[allow(non_snake_case)]
    fn bxor_not(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::BxorNot, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Bitwise and with immediate.
    ///
    /// Same as `band`, but one operand is an immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn band_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::BandImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Bitwise or with immediate.
    ///
    /// Same as `bor`, but one operand is an immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn bor_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::BorImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Bitwise xor with immediate.
    ///
    /// Same as `bxor`, but one operand is an immediate constant.
    ///
    /// Polymorphic over all scalar integer types, but does not support vector
    /// types.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn bxor_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::BxorImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Rotate left.
    ///
    /// Rotate the bits in ``x`` by ``y`` places.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotl(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Rotl, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Rotate right.
    ///
    /// Rotate the bits in ``x`` by ``y`` places.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotr(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Rotr, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Rotate left by immediate.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotl_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::RotlImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Rotate right by immediate.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn rotr_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::RotrImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Integer shift left. Shift the bits in ``x`` towards the MSB by ``y``
    /// places. Shift in zero bits to the LSB.
    ///
    /// The shift amount is masked to the size of ``x``.
    ///
    /// When shifting a B-bits integer type, this instruction computes:
    ///
    /// ```text
    ///     s &:= y \pmod B,
    ///     a &:= x \cdot 2^s \pmod{2^B}.
    /// ```
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ishl(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ishl, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Unsigned shift right. Shift bits in ``x`` towards the LSB by ``y``
    /// places, shifting in zero bits to the MSB. Also called a *logical
    /// shift*.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// When shifting a B-bits integer type, this instruction computes:
    ///
    /// ```text
    ///     s &:= y \pmod B,
    ///     a &:= \lfloor x \cdot 2^{-s} \rfloor.
    /// ```
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ushr(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ushr, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Signed shift right. Shift bits in ``x`` towards the LSB by ``y``
    /// places, shifting in sign bits to the MSB. Also called an *arithmetic
    /// shift*.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - y: Number of bits to shift
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn sshr(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Sshr, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Integer shift left by immediate.
    ///
    /// The shift amount is masked to the size of ``x``.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ishl_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::IshlImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Unsigned shift right by immediate.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn ushr_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::UshrImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Signed shift right by immediate.
    ///
    /// The shift amount is masked to the size of the register.
    ///
    /// Inputs:
    ///
    /// - x: Scalar or vector value to shift
    /// - Y: A 64-bit immediate integer.
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector integer type
    #[allow(non_snake_case)]
    fn sshr_imm<T1: Into<ir::immediates::Imm64>>(self, x: ir::Value, Y: T1) -> Value {
        let Y = Y.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.BinaryImm64(Opcode::SshrImm, ctrl_typevar, Y, x);
        dfg.first_result(inst)
    }

    /// Reverse the bits of a integer.
    ///
    /// Reverses the bits in ``x``.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn bitrev(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Bitrev, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Count leading zero bits.
    ///
    /// Starting from the MSB in ``x``, count the number of zero bits before
    /// reaching the first one bit. When ``x`` is zero, returns the size of x
    /// in bits.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn clz(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Clz, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Count leading sign bits.
    ///
    /// Starting from the MSB after the sign bit in ``x``, count the number of
    /// consecutive bits identical to the sign bit. When ``x`` is 0 or -1,
    /// returns one less than the size of x in bits.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn cls(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Cls, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Count trailing zeros.
    ///
    /// Starting from the LSB in ``x``, count the number of zero bits before
    /// reaching the first one bit. When ``x`` is zero, returns the size of x
    /// in bits.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn ctz(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Ctz, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Population count
    ///
    /// Count the number of one bits in ``x``.
    ///
    /// Inputs:
    ///
    /// - x: A scalar integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar integer type
    #[allow(non_snake_case)]
    fn popcnt(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Popcnt, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point comparison.
    ///
    /// Two IEEE 754-2008 floating point numbers, `x` and `y`, relate to each
    /// other in exactly one of four ways:
    ///
    /// == ==========================================
    /// UN Unordered when one or both numbers is NaN.
    /// EQ When `x = y`. (And `0.0 = -0.0`).
    /// LT When `x < y`.
    /// GT When `x > y`.
    /// == ==========================================
    ///
    /// The 14 `floatcc` condition codes each correspond to a subset of
    /// the four relations, except for the empty set which would always be
    /// false, and the full set which would always be true.
    ///
    /// The condition codes are divided into 7 'ordered' conditions which don't
    /// include UN, and 7 unordered conditions which all include UN.
    ///
    /// +-------+------------+---------+------------+-------------------------+
    /// |Ordered             |Unordered             |Condition                |
    /// +=======+============+=========+============+=========================+
    /// |ord    |EQ | LT | GT|uno      |UN          |NaNs absent / present.   |
    /// +-------+------------+---------+------------+-------------------------+
    /// |eq     |EQ          |ueq      |UN | EQ     |Equal                    |
    /// +-------+------------+---------+------------+-------------------------+
    /// |one    |LT | GT     |ne       |UN | LT | GT|Not equal                |
    /// +-------+------------+---------+------------+-------------------------+
    /// |lt     |LT          |ult      |UN | LT     |Less than                |
    /// +-------+------------+---------+------------+-------------------------+
    /// |le     |LT | EQ     |ule      |UN | LT | EQ|Less than or equal       |
    /// +-------+------------+---------+------------+-------------------------+
    /// |gt     |GT          |ugt      |UN | GT     |Greater than             |
    /// +-------+------------+---------+------------+-------------------------+
    /// |ge     |GT | EQ     |uge      |UN | GT | EQ|Greater than or equal    |
    /// +-------+------------+---------+------------+-------------------------+
    ///
    /// The standard C comparison operators, `<, <=, >, >=`, are all ordered,
    /// so they are false if either operand is NaN. The C equality operator,
    /// `==`, is ordered, and since inequality is defined as the logical
    /// inverse it is *unordered*. They map to the `floatcc` condition
    /// codes as follows:
    ///
    /// ==== ====== ============
    /// C    `Cond` Subset
    /// ==== ====== ============
    /// `==` eq     EQ
    /// `!=` ne     UN | LT | GT
    /// `<`  lt     LT
    /// `<=` le     LT | EQ
    /// `>`  gt     GT
    /// `>=` ge     GT | EQ
    /// ==== ====== ============
    ///
    /// This subset of condition codes also corresponds to the WebAssembly
    /// floating point comparisons of the same name.
    ///
    /// When this instruction compares floating point vectors, it returns a
    /// boolean vector with the results of lane-wise comparisons.
    ///
    /// Inputs:
    ///
    /// - Cond: A floating point comparison condition code
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn fcmp<T1: Into<ir::condcodes::FloatCC>>(self, Cond: T1, x: ir::Value, y: ir::Value) -> Value {
        let Cond = Cond.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.FloatCompare(Opcode::Fcmp, ctrl_typevar, Cond, x, y);
        dfg.first_result(inst)
    }

    /// Floating point comparison returning flags.
    ///
    /// Compares two numbers like `fcmp`, but returns floating point CPU
    /// flags instead of testing a specific condition.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - f: CPU flags representing the result of a floating point comparison. These
    /// flags can be tested with a :type:`floatcc` condition code.
    #[allow(non_snake_case)]
    fn ffcmp(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Ffcmp, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point addition.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fadd(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fadd, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point subtraction.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fsub(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fsub, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point multiplication.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fmul(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fmul, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point division.
    ///
    /// Unlike the integer division instructions ` and
    /// `udiv`, this can't trap. Division by zero is infinity or
    /// NaN, depending on the dividend.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fdiv(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fdiv, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point square root.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn sqrt(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Sqrt, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point fused multiply-and-add.
    ///
    /// Computes `a := xy+z` without any intermediate rounding of the
    /// product.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    /// - z: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: Result of applying operator to each lane
    #[allow(non_snake_case)]
    fn fma(self, x: ir::Value, y: ir::Value, z: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(y);
        let (inst, dfg) = self.Ternary(Opcode::Fma, ctrl_typevar, x, y, z);
        dfg.first_result(inst)
    }

    /// Floating point negation.
    ///
    /// Note that this is a pure bitwise operation.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` with its sign bit inverted
    #[allow(non_snake_case)]
    fn fneg(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Fneg, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point absolute value.
    ///
    /// Note that this is a pure bitwise operation.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` with its sign bit cleared
    #[allow(non_snake_case)]
    fn fabs(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Fabs, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Floating point copy sign.
    ///
    /// Note that this is a pure bitwise operation. The sign bit from ``y`` is
    /// copied to the sign bit of ``x``.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` with its sign bit changed to that of ``y``
    #[allow(non_snake_case)]
    fn fcopysign(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fcopysign, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point minimum, propagating NaNs.
    ///
    /// If either operand is NaN, this returns a NaN.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The smaller of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmin(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fmin, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point pseudo-minimum, propagating NaNs.  This behaves differently from ``fmin``.
    /// See <https://github.com/WebAssembly/simd/pull/122> for background.
    ///
    /// The behaviour is defined as ``fmin_pseudo(a, b) = (b < a) ? b : a``, and the behaviour
    /// for zero or NaN inputs follows from the behaviour of ``<`` with such inputs.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The smaller of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmin_pseudo(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::FminPseudo, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point maximum, propagating NaNs.
    ///
    /// If either operand is NaN, this returns a NaN.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The larger of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmax(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Fmax, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Floating point pseudo-maximum, propagating NaNs.  This behaves differently from ``fmax``.
    /// See <https://github.com/WebAssembly/simd/pull/122> for background.
    ///
    /// The behaviour is defined as ``fmax_pseudo(a, b) = (a < b) ? b : a``, and the behaviour
    /// for zero or NaN inputs follows from the behaviour of ``<`` with such inputs.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    /// - y: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: The larger of ``x`` and ``y``
    #[allow(non_snake_case)]
    fn fmax_pseudo(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::FmaxPseudo, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards positive infinity.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn ceil(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Ceil, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards negative infinity.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn floor(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Floor, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards zero.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn trunc(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Trunc, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Round floating point round to integral, towards nearest with ties to
    /// even.
    ///
    /// Inputs:
    ///
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: ``x`` rounded to integral value
    #[allow(non_snake_case)]
    fn nearest(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Nearest, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Reference verification.
    ///
    /// The condition code determines if the reference type in question is
    /// null or not.
    ///
    /// Inputs:
    ///
    /// - x: A scalar reference type
    ///
    /// Outputs:
    ///
    /// - a: A boolean type with 1 bits.
    #[allow(non_snake_case)]
    fn is_null(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::IsNull, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Reference verification.
    ///
    /// The condition code determines if the reference type in question is
    /// invalid or not.
    ///
    /// Inputs:
    ///
    /// - x: A scalar reference type
    ///
    /// Outputs:
    ///
    /// - a: A boolean type with 1 bits.
    #[allow(non_snake_case)]
    fn is_invalid(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::IsInvalid, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Test integer CPU flags for a specific condition.
    ///
    /// Check the CPU flags in ``f`` against the ``Cond`` condition code and
    /// return true when the condition code is satisfied.
    ///
    /// Inputs:
    ///
    /// - Cond: An integer comparison condition code.
    /// - f: CPU flags representing the result of an integer comparison. These flags
    /// can be tested with an :type:`intcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A boolean type with 1 bits.
    #[allow(non_snake_case)]
    fn trueif<T1: Into<ir::condcodes::IntCC>>(self, Cond: T1, f: ir::Value) -> Value {
        let Cond = Cond.into();
        let (inst, dfg) = self.IntCond(Opcode::Trueif, types::INVALID, Cond, f);
        dfg.first_result(inst)
    }

    /// Test floating point CPU flags for a specific condition.
    ///
    /// Check the CPU flags in ``f`` against the ``Cond`` condition code and
    /// return true when the condition code is satisfied.
    ///
    /// Inputs:
    ///
    /// - Cond: A floating point comparison condition code
    /// - f: CPU flags representing the result of a floating point comparison. These
    /// flags can be tested with a :type:`floatcc` condition code.
    ///
    /// Outputs:
    ///
    /// - a: A boolean type with 1 bits.
    #[allow(non_snake_case)]
    fn trueff<T1: Into<ir::condcodes::FloatCC>>(self, Cond: T1, f: ir::Value) -> Value {
        let Cond = Cond.into();
        let (inst, dfg) = self.FloatCond(Opcode::Trueff, types::INVALID, Cond, f);
        dfg.first_result(inst)
    }

    /// Reinterpret the bits in `x` as a different type.
    ///
    /// The input and output types must be storable to memory and of the same
    /// size. A bitcast is equivalent to storing one type and loading the other
    /// type from the same address.
    ///
    /// Inputs:
    ///
    /// - MemTo (controlling type variable):
    /// - x: Any type that can be stored in memory
    ///
    /// Outputs:
    ///
    /// - a: Bits of `x` reinterpreted
    #[allow(non_snake_case)]
    fn bitcast(self, MemTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Bitcast, MemTo, x);
        dfg.first_result(inst)
    }

    /// Cast the bits in `x` as a different type of the same bit width.
    ///
    /// This instruction does not change the data's representation but allows
    /// data in registers to be used as different types, e.g. an i32x4 as a
    /// b8x16. The only constraint on the result `a` is that it can be
    /// `raw_bitcast` back to the original type. Also, in a raw_bitcast between
    /// vector types with the same number of lanes, the value of each result
    /// lane is a raw_bitcast of the corresponding operand lane. TODO there is
    /// currently no mechanism for enforcing the bit width constraint.
    ///
    /// Inputs:
    ///
    /// - AnyTo (controlling type variable):
    /// - x: Any integer, float, boolean, or reference scalar or vector type
    ///
    /// Outputs:
    ///
    /// - a: Bits of `x` reinterpreted
    #[allow(non_snake_case)]
    fn raw_bitcast(self, AnyTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::RawBitcast, AnyTo, x);
        dfg.first_result(inst)
    }

    /// Copies a scalar value to a vector value.  The scalar is copied into the
    /// least significant lane of the vector, and all other lanes will be zero.
    ///
    /// Inputs:
    ///
    /// - TxN (controlling type variable): A SIMD vector type
    /// - s: A scalar value
    ///
    /// Outputs:
    ///
    /// - a: A vector value
    #[allow(non_snake_case)]
    fn scalar_to_vector(self, TxN: crate::ir::Type, s: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::ScalarToVector, TxN, s);
        dfg.first_result(inst)
    }

    /// Convert `x` to a smaller boolean type in the platform-defined way.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and each lane must not have more bits that the input lanes. If the
    /// input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - BoolTo (controlling type variable): A smaller boolean type with the same number of lanes
    /// - x: A scalar or vector boolean type
    ///
    /// Outputs:
    ///
    /// - a: A smaller boolean type with the same number of lanes
    #[allow(non_snake_case)]
    fn breduce(self, BoolTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Breduce, BoolTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a larger boolean type in the platform-defined way.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and each lane must not have fewer bits that the input lanes. If the
    /// input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - BoolTo (controlling type variable): A larger boolean type with the same number of lanes
    /// - x: A scalar or vector boolean type
    ///
    /// Outputs:
    ///
    /// - a: A larger boolean type with the same number of lanes
    #[allow(non_snake_case)]
    fn bextend(self, BoolTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Bextend, BoolTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to an integer.
    ///
    /// True maps to 1 and false maps to 0. The result type must have the same
    /// number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): An integer type with the same number of lanes
    /// - x: A scalar or vector boolean type
    ///
    /// Outputs:
    ///
    /// - a: An integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn bint(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Bint, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to an integer mask.
    ///
    /// True maps to all 1s and false maps to all 0s. The result type must have
    /// the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): An integer type with the same number of lanes
    /// - x: A scalar or vector boolean type
    ///
    /// Outputs:
    ///
    /// - a: An integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn bmask(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Bmask, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a smaller integer type by dropping high bits.
    ///
    /// Each lane in `x` is converted to a smaller integer type by discarding
    /// the most significant bits. This is the same as reducing modulo
    /// `2^n`.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and each lane must not have more bits that the input lanes. If the
    /// input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A smaller integer type with the same number of lanes
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A smaller integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn ireduce(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Ireduce, IntTo, x);
        dfg.first_result(inst)
    }

    /// Combine `x` and `y` into a vector with twice the lanes but half the integer width while
    /// saturating overflowing values to the signed maximum and minimum.
    ///
    /// The lanes will be concatenated after narrowing. For example, when `x` and `y` are `i32x4`
    /// and `x = [x3, x2, x1, x0]` and `y = [y3, y2, y1, y0]`, then after narrowing the value
    /// returned is an `i16x8`: `a = [y3', y2', y1', y0', x3', x2', x1', x0']`.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 16 or 32 bits wide
    /// - y: A SIMD vector type containing integer lanes 16 or 32 bits wide
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn snarrow(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Snarrow, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Combine `x` and `y` into a vector with twice the lanes but half the integer width while
    /// saturating overflowing values to the unsigned maximum and minimum.
    ///
    /// Note that all input lanes are considered signed: any negative lanes will overflow and be
    /// replaced with the unsigned minimum, `0x00`.
    ///
    /// The lanes will be concatenated after narrowing. For example, when `x` and `y` are `i32x4`
    /// and `x = [x3, x2, x1, x0]` and `y = [y3, y2, y1, y0]`, then after narrowing the value
    /// returned is an `i16x8`: `a = [y3', y2', y1', y0', x3', x2', x1', x0']`.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 16 or 32 bits wide
    /// - y: A SIMD vector type containing integer lanes 16 or 32 bits wide
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn unarrow(self, x: ir::Value, y: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Binary(Opcode::Unarrow, ctrl_typevar, x, y);
        dfg.first_result(inst)
    }

    /// Widen the low lanes of `x` using signed extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8 or 16 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn swiden_low(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::SwidenLow, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Widen the high lanes of `x` using signed extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8 or 16 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn swiden_high(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::SwidenHigh, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Widen the low lanes of `x` using unsigned extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8 or 16 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn uwiden_low(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::UwidenLow, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Widen the high lanes of `x` using unsigned extension.
    ///
    /// This will double the lane width and halve the number of lanes.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing integer lanes 8 or 16 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn uwiden_high(self, x: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::UwidenHigh, ctrl_typevar, x);
        dfg.first_result(inst)
    }

    /// Takes corresponding elements in `x` and `y`, performs a sign-extending length-doubling
    /// multiplication on them, then adds adjacent pairs of elements to form the result.  For
    /// example, if the input vectors are `[x3, x2, x1, x0]` and `[y3, y2, y1, y0]`, it produces
    /// the vector `[r1, r0]`, where `r1 = sx(x3) * sx(y3) + sx(x2) * sx(y2)` and
    /// `r0 = sx(x1) * sx(y1) + sx(x0) * sx(y0)`, and `sx(n)` sign-extends `n` to twice its width.
    ///
    /// This will double the lane width and halve the number of lanes.  So the resulting
    /// vector has the same number of bits as `x` and `y` do (individually).
    ///
    /// See <https://github.com/WebAssembly/simd/pull/127> for background info.
    ///
    /// Inputs:
    ///
    /// - x: A SIMD vector type containing 8 integer lanes each 16 bits wide.
    /// - y: A SIMD vector type containing 8 integer lanes each 16 bits wide.
    ///
    /// Outputs:
    ///
    /// - a:
    #[allow(non_snake_case)]
    fn widening_pairwise_dot_product_s(self, x: ir::Value, y: ir::Value) -> Value {
        let (inst, dfg) = self.Binary(Opcode::WideningPairwiseDotProductS, types::INVALID, x, y);
        dfg.first_result(inst)
    }

    /// Convert `x` to a larger integer type by zero-extending.
    ///
    /// Each lane in `x` is converted to a larger integer type by adding
    /// zeroes. The result has the same numerical value as `x` when both are
    /// interpreted as unsigned integers.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and each lane must not have fewer bits that the input lanes. If the
    /// input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn uextend(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Uextend, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a larger integer type by sign-extending.
    ///
    /// Each lane in `x` is converted to a larger integer type by replicating
    /// the sign bit. The result has the same numerical value as `x` when both
    /// are interpreted as signed integers.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and each lane must not have fewer bits that the input lanes. If the
    /// input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn sextend(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Sextend, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a larger floating point format.
    ///
    /// Each lane in `x` is converted to the destination floating point format.
    /// This is an exact operation.
    ///
    /// Cranelift currently only supports two floating point formats
    /// - `f32` and `f64`. This may change in the future.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and the result lanes must not have fewer bits than the input lanes. If
    /// the input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fpromote(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Fpromote, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Convert `x` to a smaller floating point format.
    ///
    /// Each lane in `x` is converted to the destination floating point format
    /// by rounding to nearest, ties to even.
    ///
    /// Cranelift currently only supports two floating point formats
    /// - `f32` and `f64`. This may change in the future.
    ///
    /// The result type must have the same number of vector lanes as the input,
    /// and the result lanes must not have more bits than the input lanes. If
    /// the input and output types are the same, this is a no-op.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fdemote(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::Fdemote, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Convert floating point to unsigned integer.
    ///
    /// Each lane in `x` is converted to an unsigned integer by rounding
    /// towards zero. If `x` is NaN or if the unsigned integral value cannot be
    /// represented in the result type, this instruction traps.
    ///
    /// The result type must have the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_uint(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToUint, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert floating point to unsigned integer as fcvt_to_uint does, but
    /// saturates the input instead of trapping. NaN and negative values are
    /// converted to 0.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_uint_sat(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToUintSat, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert floating point to signed integer.
    ///
    /// Each lane in `x` is converted to a signed integer by rounding towards
    /// zero. If `x` is NaN or if the signed integral value cannot be
    /// represented in the result type, this instruction traps.
    ///
    /// The result type must have the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_sint(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToSint, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert floating point to signed integer as fcvt_to_sint does, but
    /// saturates the input instead of trapping. NaN values are converted to 0.
    ///
    /// Inputs:
    ///
    /// - IntTo (controlling type variable): A larger integer type with the same number of lanes
    /// - x: A scalar or vector floating point number
    ///
    /// Outputs:
    ///
    /// - a: A larger integer type with the same number of lanes
    #[allow(non_snake_case)]
    fn fcvt_to_sint_sat(self, IntTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtToSintSat, IntTo, x);
        dfg.first_result(inst)
    }

    /// Convert unsigned integer to floating point.
    ///
    /// Each lane in `x` is interpreted as an unsigned integer and converted to
    /// floating point using round to nearest, ties to even.
    ///
    /// The result type must have the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fcvt_from_uint(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtFromUint, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Convert signed integer to floating point.
    ///
    /// Each lane in `x` is interpreted as a signed integer and converted to
    /// floating point using round to nearest, ties to even.
    ///
    /// The result type must have the same number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fcvt_from_sint(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtFromSint, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Converts packed signed doubleword integers to packed double precision floating point.
    ///
    /// Considering only the low half of the register, each lane in `x` is interpreted as a
    /// signed doubleword integer that is then converted to a double precision float. This
    /// instruction differs from fcvt_from_sint in that it converts half the number of lanes
    /// which are converted to occupy twice the number of bits. No rounding should be needed
    /// for the resulting float.
    ///
    /// The result type will have half the number of vector lanes as the input.
    ///
    /// Inputs:
    ///
    /// - FloatTo (controlling type variable): A scalar or vector floating point number
    /// - x: A scalar or vector integer type
    ///
    /// Outputs:
    ///
    /// - a: A scalar or vector floating point number
    #[allow(non_snake_case)]
    fn fcvt_low_from_sint(self, FloatTo: crate::ir::Type, x: ir::Value) -> Value {
        let (inst, dfg) = self.Unary(Opcode::FcvtLowFromSint, FloatTo, x);
        dfg.first_result(inst)
    }

    /// Split an integer into low and high parts.
    ///
    /// Vectors of integers are split lane-wise, so the results have the same
    /// number of lanes as the input, but the lanes are half the size.
    ///
    /// Returns the low half of `x` and the high half of `x` as two independent
    /// values.
    ///
    /// Inputs:
    ///
    /// - x: An integer type with lanes from `i16` upwards
    ///
    /// Outputs:
    ///
    /// - lo: The low bits of `x`
    /// - hi: The high bits of `x`
    #[allow(non_snake_case)]
    fn isplit(self, x: ir::Value) -> (Value, Value) {
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.Unary(Opcode::Isplit, ctrl_typevar, x);
        let results = &dfg.inst_results(inst)[0..2];
        (results[0], results[1])
    }

    /// Concatenate low and high bits to form a larger integer type.
    ///
    /// Vectors of integers are concatenated lane-wise such that the result has
    /// the same number of lanes as the inputs, but the lanes are twice the
    /// size.
    ///
    /// Inputs:
    ///
    /// - lo: An integer type with lanes type to `i64`
    /// - hi: An integer type with lanes type to `i64`
    ///
    /// Outputs:
    ///
    /// - a: The concatenation of `lo` and `hi`
    #[allow(non_snake_case)]
    fn iconcat(self, lo: ir::Value, hi: ir::Value) -> Value {
        let ctrl_typevar = self.data_flow_graph().value_type(lo);
        let (inst, dfg) = self.Binary(Opcode::Iconcat, ctrl_typevar, lo, hi);
        dfg.first_result(inst)
    }

    /// Atomically read-modify-write memory at `p`, with second operand `x`.  The old value is
    /// returned.  `p` has the type of the target word size, and `x` may be an integer type of
    /// 8, 16, 32 or 64 bits, even on a 32-bit target.  The type of the returned value is the
    /// same as the type of `x`.  This operation is sequentially consistent and creates
    /// happens-before edges that order normal (non-atomic) loads and stores.
    ///
    /// Inputs:
    ///
    /// - AtomicMem (controlling type variable): Any type that can be stored in memory, which can be used in an atomic operation
    /// - MemFlags: Memory operation flags
    /// - AtomicRmwOp: Atomic Read-Modify-Write Ops
    /// - p: An integer address type
    /// - x: Value to be atomically stored
    ///
    /// Outputs:
    ///
    /// - a: Value atomically loaded
    #[allow(non_snake_case)]
    fn atomic_rmw<T1: Into<ir::MemFlags>, T2: Into<ir::AtomicRmwOp>>(self, AtomicMem: crate::ir::Type, MemFlags: T1, AtomicRmwOp: T2, p: ir::Value, x: ir::Value) -> Value {
        let MemFlags = MemFlags.into();
        let AtomicRmwOp = AtomicRmwOp.into();
        let (inst, dfg) = self.AtomicRmw(Opcode::AtomicRmw, AtomicMem, MemFlags, AtomicRmwOp, p, x);
        dfg.first_result(inst)
    }

    /// Perform an atomic compare-and-swap operation on memory at `p`, with expected value `e`,
    /// storing `x` if the value at `p` equals `e`.  The old value at `p` is returned,
    /// regardless of whether the operation succeeds or fails.  `p` has the type of the target
    /// word size, and `x` and `e` must have the same type and the same size, which may be an
    /// integer type of 8, 16, 32 or 64 bits, even on a 32-bit target.  The type of the returned
    /// value is the same as the type of `x` and `e`.  This operation is sequentially
    /// consistent and creates happens-before edges that order normal (non-atomic) loads and
    /// stores.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    /// - e: Expected value in CAS
    /// - x: Value to be atomically stored
    ///
    /// Outputs:
    ///
    /// - a: Value atomically loaded
    #[allow(non_snake_case)]
    fn atomic_cas<T1: Into<ir::MemFlags>>(self, MemFlags: T1, p: ir::Value, e: ir::Value, x: ir::Value) -> Value {
        let MemFlags = MemFlags.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        let (inst, dfg) = self.AtomicCas(Opcode::AtomicCas, ctrl_typevar, MemFlags, p, e, x);
        dfg.first_result(inst)
    }

    /// Atomically load from memory at `p`.
    ///
    /// This is a polymorphic instruction that can load any value type which has a memory
    /// representation.  It should only be used for integer types with 8, 16, 32 or 64 bits.
    /// This operation is sequentially consistent and creates happens-before edges that order
    /// normal (non-atomic) loads and stores.
    ///
    /// Inputs:
    ///
    /// - AtomicMem (controlling type variable): Any type that can be stored in memory, which can be used in an atomic operation
    /// - MemFlags: Memory operation flags
    /// - p: An integer address type
    ///
    /// Outputs:
    ///
    /// - a: Value atomically loaded
    #[allow(non_snake_case)]
    fn atomic_load<T1: Into<ir::MemFlags>>(self, AtomicMem: crate::ir::Type, MemFlags: T1, p: ir::Value) -> Value {
        let MemFlags = MemFlags.into();
        let (inst, dfg) = self.LoadNoOffset(Opcode::AtomicLoad, AtomicMem, MemFlags, p);
        dfg.first_result(inst)
    }

    /// Atomically store `x` to memory at `p`.
    ///
    /// This is a polymorphic instruction that can store any value type with a memory
    /// representation.  It should only be used for integer types with 8, 16, 32 or 64 bits.
    /// This operation is sequentially consistent and creates happens-before edges that order
    /// normal (non-atomic) loads and stores.
    ///
    /// Inputs:
    ///
    /// - MemFlags: Memory operation flags
    /// - x: Value to be atomically stored
    /// - p: An integer address type
    #[allow(non_snake_case)]
    fn atomic_store<T1: Into<ir::MemFlags>>(self, MemFlags: T1, x: ir::Value, p: ir::Value) -> Inst {
        let MemFlags = MemFlags.into();
        let ctrl_typevar = self.data_flow_graph().value_type(x);
        self.StoreNoOffset(Opcode::AtomicStore, ctrl_typevar, MemFlags, x, p).0
    }

    /// A memory fence.  This must provide ordering to ensure that, at a minimum, neither loads
    /// nor stores of any kind may move forwards or backwards across the fence.  This operation
    /// is sequentially consistent.
    #[allow(non_snake_case)]
    fn fence(self) -> Inst {
        self.NullAry(Opcode::Fence, types::INVALID).0
    }

    /// AtomicCas(imms=(flags: ir::MemFlags), vals=3)
    #[allow(non_snake_case)]
    fn AtomicCas(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, arg0: Value, arg1: Value, arg2: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::AtomicCas {
            opcode,
            flags,
            args: [arg0, arg1, arg2],
        };
        self.build(data, ctrl_typevar)
    }

    /// AtomicRmw(imms=(flags: ir::MemFlags, op: ir::AtomicRmwOp), vals=2)
    #[allow(non_snake_case)]
    fn AtomicRmw(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, op: ir::AtomicRmwOp, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::AtomicRmw {
            opcode,
            flags,
            op,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// Binary(imms=(), vals=2)
    #[allow(non_snake_case)]
    fn Binary(self, opcode: Opcode, ctrl_typevar: Type, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Binary {
            opcode,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// BinaryImm64(imms=(imm: ir::immediates::Imm64), vals=1)
    #[allow(non_snake_case)]
    fn BinaryImm64(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Imm64, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let mut data = ir::InstructionData::BinaryImm64 {
            opcode,
            imm,
            arg: arg0,
        };
        data.sign_extend_immediates(ctrl_typevar);
        self.build(data, ctrl_typevar)
    }

    /// BinaryImm8(imms=(imm: ir::immediates::Uimm8), vals=1)
    #[allow(non_snake_case)]
    fn BinaryImm8(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Uimm8, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BinaryImm8 {
            opcode,
            imm,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// Branch(imms=(destination: ir::Block), vals=1)
    #[allow(non_snake_case)]
    fn Branch(self, opcode: Opcode, ctrl_typevar: Type, destination: ir::Block, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Branch {
            opcode,
            destination,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// BranchFloat(imms=(cond: ir::condcodes::FloatCC, destination: ir::Block), vals=1)
    #[allow(non_snake_case)]
    fn BranchFloat(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::FloatCC, destination: ir::Block, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BranchFloat {
            opcode,
            cond,
            destination,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// BranchIcmp(imms=(cond: ir::condcodes::IntCC, destination: ir::Block), vals=2)
    #[allow(non_snake_case)]
    fn BranchIcmp(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, destination: ir::Block, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BranchIcmp {
            opcode,
            cond,
            destination,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// BranchInt(imms=(cond: ir::condcodes::IntCC, destination: ir::Block), vals=1)
    #[allow(non_snake_case)]
    fn BranchInt(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, destination: ir::Block, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BranchInt {
            opcode,
            cond,
            destination,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// BranchTable(imms=(destination: ir::Block, table: ir::JumpTable), vals=1)
    #[allow(non_snake_case)]
    fn BranchTable(self, opcode: Opcode, ctrl_typevar: Type, destination: ir::Block, table: ir::JumpTable, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BranchTable {
            opcode,
            destination,
            table,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// BranchTableBase(imms=(table: ir::JumpTable), vals=0)
    #[allow(non_snake_case)]
    fn BranchTableBase(self, opcode: Opcode, ctrl_typevar: Type, table: ir::JumpTable) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BranchTableBase {
            opcode,
            table,
        };
        self.build(data, ctrl_typevar)
    }

    /// BranchTableEntry(imms=(imm: ir::immediates::Uimm8, table: ir::JumpTable), vals=2)
    #[allow(non_snake_case)]
    fn BranchTableEntry(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Uimm8, table: ir::JumpTable, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::BranchTableEntry {
            opcode,
            imm,
            table,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// Call(imms=(func_ref: ir::FuncRef), vals=0)
    #[allow(non_snake_case)]
    fn Call(self, opcode: Opcode, ctrl_typevar: Type, func_ref: ir::FuncRef, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Call {
            opcode,
            func_ref,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// CallIndirect(imms=(sig_ref: ir::SigRef), vals=1)
    #[allow(non_snake_case)]
    fn CallIndirect(self, opcode: Opcode, ctrl_typevar: Type, sig_ref: ir::SigRef, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::CallIndirect {
            opcode,
            sig_ref,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// CondTrap(imms=(code: ir::TrapCode), vals=1)
    #[allow(non_snake_case)]
    fn CondTrap(self, opcode: Opcode, ctrl_typevar: Type, code: ir::TrapCode, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::CondTrap {
            opcode,
            code,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// CopySpecial(imms=(src: isa::RegUnit, dst: isa::RegUnit), vals=0)
    #[allow(non_snake_case)]
    fn CopySpecial(self, opcode: Opcode, ctrl_typevar: Type, src: isa::RegUnit, dst: isa::RegUnit) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::CopySpecial {
            opcode,
            src,
            dst,
        };
        self.build(data, ctrl_typevar)
    }

    /// CopyToSsa(imms=(src: isa::RegUnit), vals=0)
    #[allow(non_snake_case)]
    fn CopyToSsa(self, opcode: Opcode, ctrl_typevar: Type, src: isa::RegUnit) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::CopyToSsa {
            opcode,
            src,
        };
        self.build(data, ctrl_typevar)
    }

    /// FloatCompare(imms=(cond: ir::condcodes::FloatCC), vals=2)
    #[allow(non_snake_case)]
    fn FloatCompare(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::FloatCC, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::FloatCompare {
            opcode,
            cond,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// FloatCond(imms=(cond: ir::condcodes::FloatCC), vals=1)
    #[allow(non_snake_case)]
    fn FloatCond(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::FloatCC, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::FloatCond {
            opcode,
            cond,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// FloatCondTrap(imms=(cond: ir::condcodes::FloatCC, code: ir::TrapCode), vals=1)
    #[allow(non_snake_case)]
    fn FloatCondTrap(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::FloatCC, code: ir::TrapCode, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::FloatCondTrap {
            opcode,
            cond,
            code,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// FuncAddr(imms=(func_ref: ir::FuncRef), vals=0)
    #[allow(non_snake_case)]
    fn FuncAddr(self, opcode: Opcode, ctrl_typevar: Type, func_ref: ir::FuncRef) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::FuncAddr {
            opcode,
            func_ref,
        };
        self.build(data, ctrl_typevar)
    }

    /// HeapAddr(imms=(heap: ir::Heap, imm: ir::immediates::Uimm32), vals=1)
    #[allow(non_snake_case)]
    fn HeapAddr(self, opcode: Opcode, ctrl_typevar: Type, heap: ir::Heap, imm: ir::immediates::Uimm32, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::HeapAddr {
            opcode,
            heap,
            imm,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// IndirectJump(imms=(table: ir::JumpTable), vals=1)
    #[allow(non_snake_case)]
    fn IndirectJump(self, opcode: Opcode, ctrl_typevar: Type, table: ir::JumpTable, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::IndirectJump {
            opcode,
            table,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// IntCompare(imms=(cond: ir::condcodes::IntCC), vals=2)
    #[allow(non_snake_case)]
    fn IntCompare(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::IntCompare {
            opcode,
            cond,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// IntCompareImm(imms=(cond: ir::condcodes::IntCC, imm: ir::immediates::Imm64), vals=1)
    #[allow(non_snake_case)]
    fn IntCompareImm(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, imm: ir::immediates::Imm64, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let mut data = ir::InstructionData::IntCompareImm {
            opcode,
            cond,
            imm,
            arg: arg0,
        };
        data.sign_extend_immediates(ctrl_typevar);
        self.build(data, ctrl_typevar)
    }

    /// IntCond(imms=(cond: ir::condcodes::IntCC), vals=1)
    #[allow(non_snake_case)]
    fn IntCond(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::IntCond {
            opcode,
            cond,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// IntCondTrap(imms=(cond: ir::condcodes::IntCC, code: ir::TrapCode), vals=1)
    #[allow(non_snake_case)]
    fn IntCondTrap(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, code: ir::TrapCode, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::IntCondTrap {
            opcode,
            cond,
            code,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// IntSelect(imms=(cond: ir::condcodes::IntCC), vals=3)
    #[allow(non_snake_case)]
    fn IntSelect(self, opcode: Opcode, ctrl_typevar: Type, cond: ir::condcodes::IntCC, arg0: Value, arg1: Value, arg2: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::IntSelect {
            opcode,
            cond,
            args: [arg0, arg1, arg2],
        };
        self.build(data, ctrl_typevar)
    }

    /// Jump(imms=(destination: ir::Block), vals=0)
    #[allow(non_snake_case)]
    fn Jump(self, opcode: Opcode, ctrl_typevar: Type, destination: ir::Block, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Jump {
            opcode,
            destination,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// Load(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=1)
    #[allow(non_snake_case)]
    fn Load(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, offset: ir::immediates::Offset32, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Load {
            opcode,
            flags,
            offset,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// LoadComplex(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=0)
    #[allow(non_snake_case)]
    fn LoadComplex(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, offset: ir::immediates::Offset32, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::LoadComplex {
            opcode,
            flags,
            offset,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// LoadNoOffset(imms=(flags: ir::MemFlags), vals=1)
    #[allow(non_snake_case)]
    fn LoadNoOffset(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::LoadNoOffset {
            opcode,
            flags,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// MultiAry(imms=(), vals=0)
    #[allow(non_snake_case)]
    fn MultiAry(self, opcode: Opcode, ctrl_typevar: Type, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::MultiAry {
            opcode,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// NullAry(imms=(), vals=0)
    #[allow(non_snake_case)]
    fn NullAry(self, opcode: Opcode, ctrl_typevar: Type) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::NullAry {
            opcode,
        };
        self.build(data, ctrl_typevar)
    }

    /// RegFill(imms=(src: ir::StackSlot, dst: isa::RegUnit), vals=1)
    #[allow(non_snake_case)]
    fn RegFill(self, opcode: Opcode, ctrl_typevar: Type, src: ir::StackSlot, dst: isa::RegUnit, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::RegFill {
            opcode,
            src,
            dst,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// RegMove(imms=(src: isa::RegUnit, dst: isa::RegUnit), vals=1)
    #[allow(non_snake_case)]
    fn RegMove(self, opcode: Opcode, ctrl_typevar: Type, src: isa::RegUnit, dst: isa::RegUnit, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::RegMove {
            opcode,
            src,
            dst,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// RegSpill(imms=(src: isa::RegUnit, dst: ir::StackSlot), vals=1)
    #[allow(non_snake_case)]
    fn RegSpill(self, opcode: Opcode, ctrl_typevar: Type, src: isa::RegUnit, dst: ir::StackSlot, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::RegSpill {
            opcode,
            src,
            dst,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// Shuffle(imms=(mask: ir::Immediate), vals=2)
    #[allow(non_snake_case)]
    fn Shuffle(self, opcode: Opcode, ctrl_typevar: Type, mask: ir::Immediate, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Shuffle {
            opcode,
            mask,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// StackLoad(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=0)
    #[allow(non_snake_case)]
    fn StackLoad(self, opcode: Opcode, ctrl_typevar: Type, stack_slot: ir::StackSlot, offset: ir::immediates::Offset32) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::StackLoad {
            opcode,
            stack_slot,
            offset,
        };
        self.build(data, ctrl_typevar)
    }

    /// StackStore(imms=(stack_slot: ir::StackSlot, offset: ir::immediates::Offset32), vals=1)
    #[allow(non_snake_case)]
    fn StackStore(self, opcode: Opcode, ctrl_typevar: Type, stack_slot: ir::StackSlot, offset: ir::immediates::Offset32, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::StackStore {
            opcode,
            stack_slot,
            offset,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// Store(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=2)
    #[allow(non_snake_case)]
    fn Store(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, offset: ir::immediates::Offset32, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Store {
            opcode,
            flags,
            offset,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// StoreComplex(imms=(flags: ir::MemFlags, offset: ir::immediates::Offset32), vals=1)
    #[allow(non_snake_case)]
    fn StoreComplex(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, offset: ir::immediates::Offset32, args: ir::ValueList) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::StoreComplex {
            opcode,
            flags,
            offset,
            args,
        };
        self.build(data, ctrl_typevar)
    }

    /// StoreNoOffset(imms=(flags: ir::MemFlags), vals=2)
    #[allow(non_snake_case)]
    fn StoreNoOffset(self, opcode: Opcode, ctrl_typevar: Type, flags: ir::MemFlags, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::StoreNoOffset {
            opcode,
            flags,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// TableAddr(imms=(table: ir::Table, offset: ir::immediates::Offset32), vals=1)
    #[allow(non_snake_case)]
    fn TableAddr(self, opcode: Opcode, ctrl_typevar: Type, table: ir::Table, offset: ir::immediates::Offset32, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::TableAddr {
            opcode,
            table,
            offset,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// Ternary(imms=(), vals=3)
    #[allow(non_snake_case)]
    fn Ternary(self, opcode: Opcode, ctrl_typevar: Type, arg0: Value, arg1: Value, arg2: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Ternary {
            opcode,
            args: [arg0, arg1, arg2],
        };
        self.build(data, ctrl_typevar)
    }

    /// TernaryImm8(imms=(imm: ir::immediates::Uimm8), vals=2)
    #[allow(non_snake_case)]
    fn TernaryImm8(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Uimm8, arg0: Value, arg1: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::TernaryImm8 {
            opcode,
            imm,
            args: [arg0, arg1],
        };
        self.build(data, ctrl_typevar)
    }

    /// Trap(imms=(code: ir::TrapCode), vals=0)
    #[allow(non_snake_case)]
    fn Trap(self, opcode: Opcode, ctrl_typevar: Type, code: ir::TrapCode) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Trap {
            opcode,
            code,
        };
        self.build(data, ctrl_typevar)
    }

    /// Unary(imms=(), vals=1)
    #[allow(non_snake_case)]
    fn Unary(self, opcode: Opcode, ctrl_typevar: Type, arg0: Value) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::Unary {
            opcode,
            arg: arg0,
        };
        self.build(data, ctrl_typevar)
    }

    /// UnaryBool(imms=(imm: bool), vals=0)
    #[allow(non_snake_case)]
    fn UnaryBool(self, opcode: Opcode, ctrl_typevar: Type, imm: bool) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryBool {
            opcode,
            imm,
        };
        self.build(data, ctrl_typevar)
    }

    /// UnaryConst(imms=(constant_handle: ir::Constant), vals=0)
    #[allow(non_snake_case)]
    fn UnaryConst(self, opcode: Opcode, ctrl_typevar: Type, constant_handle: ir::Constant) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryConst {
            opcode,
            constant_handle,
        };
        self.build(data, ctrl_typevar)
    }

    /// UnaryGlobalValue(imms=(global_value: ir::GlobalValue), vals=0)
    #[allow(non_snake_case)]
    fn UnaryGlobalValue(self, opcode: Opcode, ctrl_typevar: Type, global_value: ir::GlobalValue) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryGlobalValue {
            opcode,
            global_value,
        };
        self.build(data, ctrl_typevar)
    }

    /// UnaryIeee32(imms=(imm: ir::immediates::Ieee32), vals=0)
    #[allow(non_snake_case)]
    fn UnaryIeee32(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Ieee32) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryIeee32 {
            opcode,
            imm,
        };
        self.build(data, ctrl_typevar)
    }

    /// UnaryIeee64(imms=(imm: ir::immediates::Ieee64), vals=0)
    #[allow(non_snake_case)]
    fn UnaryIeee64(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Ieee64) -> (Inst, &'f mut ir::DataFlowGraph) {
        let data = ir::InstructionData::UnaryIeee64 {
            opcode,
            imm,
        };
        self.build(data, ctrl_typevar)
    }

    /// UnaryImm(imms=(imm: ir::immediates::Imm64), vals=0)
    #[allow(non_snake_case)]
    fn UnaryImm(self, opcode: Opcode, ctrl_typevar: Type, imm: ir::immediates::Imm64) -> (Inst, &'f mut ir::DataFlowGraph) {
        let mut data = ir::InstructionData::UnaryImm {
            opcode,
            imm,
        };
        data.sign_extend_immediates(ctrl_typevar);
        self.build(data, ctrl_typevar)
    }
}
