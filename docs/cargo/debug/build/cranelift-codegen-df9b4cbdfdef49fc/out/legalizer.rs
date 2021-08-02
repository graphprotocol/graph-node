/// Legalize instructions by expansion.
///
/// Rewrite instructions in terms of other instructions, generally
/// operating on the same types as the original instructions.
#[allow(unused_variables,unused_assignments,unused_imports,non_snake_case)]
pub fn expand(
    inst: crate::ir::Inst,
    func: &mut crate::ir::Function,
    cfg: &mut crate::flowgraph::ControlFlowGraph,
    isa: &dyn crate::isa::TargetIsa,
) -> bool {
    use crate::ir::InstBuilder;
    use crate::cursor::{Cursor, FuncCursor};
    let mut pos = FuncCursor::new(func).at_inst(inst);
    pos.use_srcloc(inst);
    {
        match pos.func.dfg[inst].opcode() {
            ir::Opcode::BandImm => {
                // Unwrap fields from instruction format a := band_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := band(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).band(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::BandNot => {
                // Unwrap fields from instruction format a := band_not(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := band(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().bnot(y);
                let a = pos.func.dfg.replace(inst).band(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::Bitrev => {
                // Unwrap fields from instruction format a := bitrev.i32(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := bor(e1, e2).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I32 {
                    let a1 = pos.ins().band_imm(x, 2863311530);
                    let a2 = pos.ins().ushr_imm(a1, 1);
                    let a3 = pos.ins().band_imm(x, 1431655765);
                    let a4 = pos.ins().ishl_imm(a3, 1);
                    let b = pos.ins().bor(a2, a4);
                    let b1 = pos.ins().band_imm(b, 3435973836);
                    let b2 = pos.ins().ushr_imm(b1, 2);
                    let b3 = pos.ins().band_imm(b, 858993459);
                    let b4 = pos.ins().ishl_imm(b3, 2);
                    let c = pos.ins().bor(b2, b4);
                    let c1 = pos.ins().band_imm(c, 4042322160);
                    let c2 = pos.ins().ushr_imm(c1, 4);
                    let c3 = pos.ins().band_imm(c, 252645135);
                    let c4 = pos.ins().ishl_imm(c3, 4);
                    let d = pos.ins().bor(c2, c4);
                    let d1 = pos.ins().band_imm(d, 4278255360);
                    let d2 = pos.ins().ushr_imm(d1, 8);
                    let d3 = pos.ins().band_imm(d, 16711935);
                    let d4 = pos.ins().ishl_imm(d3, 8);
                    let e = pos.ins().bor(d2, d4);
                    let e1 = pos.ins().ushr_imm(e, 16);
                    let e2 = pos.ins().ishl_imm(e, 16);
                    let a = pos.func.dfg.replace(inst).bor(e1, e2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let a1 = pos.ins().band_imm(x, -6148914691236517206);
                    let a2 = pos.ins().ushr_imm(a1, 1);
                    let a3 = pos.ins().band_imm(x, 6148914691236517205);
                    let a4 = pos.ins().ishl_imm(a3, 1);
                    let b = pos.ins().bor(a2, a4);
                    let b1 = pos.ins().band_imm(b, -3689348814741910324);
                    let b2 = pos.ins().ushr_imm(b1, 2);
                    let b3 = pos.ins().band_imm(b, 3689348814741910323);
                    let b4 = pos.ins().ishl_imm(b3, 2);
                    let c = pos.ins().bor(b2, b4);
                    let c1 = pos.ins().band_imm(c, -1085102592571150096);
                    let c2 = pos.ins().ushr_imm(c1, 4);
                    let c3 = pos.ins().band_imm(c, 1085102592571150095);
                    let c4 = pos.ins().ishl_imm(c3, 4);
                    let d = pos.ins().bor(c2, c4);
                    let d1 = pos.ins().band_imm(d, -71777214294589696);
                    let d2 = pos.ins().ushr_imm(d1, 8);
                    let d3 = pos.ins().band_imm(d, 71777214294589695);
                    let d4 = pos.ins().ishl_imm(d3, 8);
                    let e = pos.ins().bor(d2, d4);
                    let e1 = pos.ins().band_imm(e, -281470681808896);
                    let e2 = pos.ins().ushr_imm(e1, 16);
                    let e3 = pos.ins().band_imm(e, 281470681808895);
                    let e4 = pos.ins().ishl_imm(e3, 16);
                    let f = pos.ins().bor(e2, e4);
                    let f1 = pos.ins().ushr_imm(f, 32);
                    let f2 = pos.ins().ishl_imm(f, 32);
                    let a = pos.func.dfg.replace(inst).bor(f1, f2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Bnot => {
                // Unwrap fields from instruction format a := bnot(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := bxor(x, y).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[0].contains(typeof_x);
                if predicate {
                    let y = pos.ins().iconst(typeof_x, -1);
                    let a = pos.func.dfg.replace(inst).bxor(x, y);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::BorImm => {
                // Unwrap fields from instruction format a := bor_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := bor(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).bor(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::BorNot => {
                // Unwrap fields from instruction format a := bor_not(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := bor(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().bnot(y);
                let a = pos.func.dfg.replace(inst).bor(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::BxorImm => {
                // Unwrap fields from instruction format a := bxor_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := bxor(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).bxor(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::BxorNot => {
                // Unwrap fields from instruction format a := bxor_not(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := bxor(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().bnot(y);
                let a = pos.func.dfg.replace(inst).bxor(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::Fabs => {
                // Unwrap fields from instruction format a := fabs.f32(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := band_not(x, b).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::F32 {
                    let b = pos.ins().f32const(ir::immediates::Ieee32::with_bits(0x80000000));
                    let a = pos.func.dfg.replace(inst).band_not(x, b);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::F64 {
                    let b = pos.ins().f64const(ir::immediates::Ieee64::with_bits(0x8000000000000000));
                    let a = pos.func.dfg.replace(inst).band_not(x, b);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Fcopysign => {
                // Unwrap fields from instruction format a := fcopysign.f32(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := bor(a1, a2).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::F32 {
                    let b = pos.ins().f32const(ir::immediates::Ieee32::with_bits(0x80000000));
                    let a1 = pos.ins().band_not(x, b);
                    let a2 = pos.ins().band(y, b);
                    let a = pos.func.dfg.replace(inst).bor(a1, a2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::F64 {
                    let b = pos.ins().f64const(ir::immediates::Ieee64::with_bits(0x8000000000000000));
                    let a1 = pos.ins().band_not(x, b);
                    let a2 = pos.ins().band(y, b);
                    let a = pos.func.dfg.replace(inst).bor(a1, a2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::FcvtFromSint => {
                // Unwrap fields from instruction format a := fcvt_from_sint.f32.i8(b)
                let (b, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := fcvt_from_sint.f32(x).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I8 && pos.func.dfg.ctrl_typevar(inst) == ir::types::F32 {
                    let x = pos.ins().sextend(ir::types::I32, b);
                    let a = pos.func.dfg.replace(inst).fcvt_from_sint(ir::types::F32, x);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I16 && pos.func.dfg.ctrl_typevar(inst) == ir::types::F32 {
                    let x = pos.ins().sextend(ir::types::I32, b);
                    let a = pos.func.dfg.replace(inst).fcvt_from_sint(ir::types::F32, x);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I8 && pos.func.dfg.ctrl_typevar(inst) == ir::types::F64 {
                    let x = pos.ins().sextend(ir::types::I32, b);
                    let a = pos.func.dfg.replace(inst).fcvt_from_sint(ir::types::F64, x);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I16 && pos.func.dfg.ctrl_typevar(inst) == ir::types::F64 {
                    let x = pos.ins().sextend(ir::types::I32, b);
                    let a = pos.func.dfg.replace(inst).fcvt_from_sint(ir::types::F64, x);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Fneg => {
                // Unwrap fields from instruction format a := fneg.f32(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := bxor(x, b).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::F32 {
                    let b = pos.ins().f32const(ir::immediates::Ieee32::with_bits(0x80000000));
                    let a = pos.func.dfg.replace(inst).bxor(x, b);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::F64 {
                    let b = pos.ins().f64const(ir::immediates::Ieee64::with_bits(0x8000000000000000));
                    let a = pos.func.dfg.replace(inst).bxor(x, b);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::IaddCarry => {
                // Unwrap fields from instruction format (a, c) := iadd_carry(x, y, c_in)
                let (x, y, c_in, args) = if let ir::InstructionData::Ternary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        pos.func.dfg.resolve_aliases(args[2]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                let a;
                let c;
                {
                    let r = pos.func.dfg.inst_results(inst);
                    a = r[0];
                    c = r[1];
                }

                pos.func.dfg.clear_results(inst);
                let (a1, c1) = pos.ins().iadd_cout(x, y);
                let c_int = pos.ins().bint(typeof_x, c_in);
                let (a, c2) = pos.ins().with_results([Some(a), None]).iadd_cout(a1, c_int);
                let c = pos.ins().with_result(c).bor(c1, c2);
                let removed = pos.remove_inst();
                debug_assert_eq!(removed, inst);
                return true;
            }

            ir::Opcode::IaddCin => {
                // Unwrap fields from instruction format a := iadd_cin(x, y, c)
                let (x, y, c, args) = if let ir::InstructionData::Ternary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        pos.func.dfg.resolve_aliases(args[2]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iadd(a1, c_int).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iadd(x, y);
                let c_int = pos.ins().bint(typeof_x, c);
                let a = pos.func.dfg.replace(inst).iadd(a1, c_int);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::IaddCout => {
                // Unwrap fields from instruction format (a, c) := iadd_cout(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                let a;
                let c;
                {
                    let r = pos.func.dfg.inst_results(inst);
                    a = r[0];
                    c = r[1];
                }

                pos.func.dfg.clear_results(inst);
                let a = pos.ins().with_result(a).iadd(x, y);
                let c = pos.ins().with_result(c).icmp(ir::condcodes::IntCC::UnsignedLessThan, a, x);
                let removed = pos.remove_inst();
                debug_assert_eq!(removed, inst);
                return true;
            }

            ir::Opcode::IaddImm => {
                // Unwrap fields from instruction format a := iadd_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iadd(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).iadd(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::IcmpImm => {
                // Unwrap fields from instruction format a := icmp_imm(cc, x, y)
                let (cc, x, y, args) = if let ir::InstructionData::IntCompareImm {
                    cond,
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        cond,
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := icmp(cc, x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).icmp(cc, x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::IfcmpImm => {
                // Unwrap fields from instruction format a := ifcmp_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := ifcmp(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).ifcmp(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::ImulImm => {
                // Unwrap fields from instruction format a := imul_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := imul(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).imul(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::Ireduce => {
                // Unwrap fields from instruction format a := ireduce.i64.i128(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := copy(b).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I128 && pos.func.dfg.ctrl_typevar(inst) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (b, c) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let a = pos.func.dfg.replace(inst).copy(b);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I64 && pos.func.dfg.ctrl_typevar(inst) == ir::types::I32 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (b, c) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let a = pos.func.dfg.replace(inst).copy(b);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::IrsubImm => {
                // Unwrap fields from instruction format a := irsub_imm(y, x)
                let (y, x, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_y = pos.func.dfg.value_type(y);
                // Results handled by a := isub(a1, y).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_y, x);
                let a = pos.func.dfg.replace(inst).isub(a1, y);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::IshlImm => {
                // Unwrap fields from instruction format a := ishl_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := ishl(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(ir::types::I32, y);
                let a = pos.func.dfg.replace(inst).ishl(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::IsubBin => {
                // Unwrap fields from instruction format a := isub_bin(x, y, b)
                let (x, y, b, args) = if let ir::InstructionData::Ternary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        pos.func.dfg.resolve_aliases(args[2]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := isub(a1, b_int).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().isub(x, y);
                let b_int = pos.ins().bint(typeof_x, b);
                let a = pos.func.dfg.replace(inst).isub(a1, b_int);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::IsubBorrow => {
                // Unwrap fields from instruction format (a, b) := isub_borrow(x, y, b_in)
                let (x, y, b_in, args) = if let ir::InstructionData::Ternary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        pos.func.dfg.resolve_aliases(args[2]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                let a;
                let b;
                {
                    let r = pos.func.dfg.inst_results(inst);
                    a = r[0];
                    b = r[1];
                }

                pos.func.dfg.clear_results(inst);
                let (a1, b1) = pos.ins().isub_bout(x, y);
                let b_int = pos.ins().bint(typeof_x, b_in);
                let (a, b2) = pos.ins().with_results([Some(a), None]).isub_bout(a1, b_int);
                let b = pos.ins().with_result(b).bor(b1, b2);
                let removed = pos.remove_inst();
                debug_assert_eq!(removed, inst);
                return true;
            }

            ir::Opcode::IsubBout => {
                // Unwrap fields from instruction format (a, b) := isub_bout(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                let a;
                let b;
                {
                    let r = pos.func.dfg.inst_results(inst);
                    a = r[0];
                    b = r[1];
                }

                pos.func.dfg.clear_results(inst);
                let a = pos.ins().with_result(a).isub(x, y);
                let b = pos.ins().with_result(b).icmp(ir::condcodes::IntCC::UnsignedGreaterThan, a, x);
                let removed = pos.remove_inst();
                debug_assert_eq!(removed, inst);
                return true;
            }

            ir::Opcode::RotlImm => {
                // Unwrap fields from instruction format a := rotl_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := rotl(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(ir::types::I32, y);
                let a = pos.func.dfg.replace(inst).rotl(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::RotrImm => {
                // Unwrap fields from instruction format a := rotr_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := rotr(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(ir::types::I32, y);
                let a = pos.func.dfg.replace(inst).rotr(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::SdivImm => {
                // Unwrap fields from instruction format a := sdiv_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := sdiv(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).sdiv(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::SremImm => {
                // Unwrap fields from instruction format a := srem_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := srem(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).srem(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::SshrImm => {
                // Unwrap fields from instruction format a := sshr_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := sshr(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(ir::types::I32, y);
                let a = pos.func.dfg.replace(inst).sshr(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::UdivImm => {
                // Unwrap fields from instruction format a := udiv_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := udiv(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).udiv(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::UremImm => {
                // Unwrap fields from instruction format a := urem_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := urem(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(typeof_x, y);
                let a = pos.func.dfg.replace(inst).urem(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::UshrImm => {
                // Unwrap fields from instruction format a := ushr_imm(x, y)
                let (x, y, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := ushr(x, a1).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let a1 = pos.ins().iconst(ir::types::I32, y);
                let a = pos.func.dfg.replace(inst).ushr(x, a1);
                if pos.current_inst() == Some(inst) {
                    pos.next_inst();
                }
                return true;
            }

            ir::Opcode::BrIcmp => {
                expand_br_icmp(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::BrTable => {
                expand_br_table(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::Call => {
                expand_call(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::F32const => {
                expand_fconst(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::F64const => {
                expand_fconst(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::GlobalValue => {
                expand_global_value(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::HeapAddr => {
                expand_heap_addr(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::ResumableTrapnz => {
                expand_cond_trap(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::Select => {
                expand_select(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::StackLoad => {
                expand_stack_load(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::StackStore => {
                expand_stack_store(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::TableAddr => {
                expand_table_addr(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::Trapnz => {
                expand_cond_trap(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::Trapz => {
                expand_cond_trap(inst, func, cfg, isa);
                return true;
            }

            _ => {},
        }
    }
    false
}

/// Instruction expansions for architectures with flags.
///
/// Expand some instructions using CPU flags, then fall back to the normal
/// expansions. Not all architectures support CPU flags, so these patterns
/// are kept separate.
#[allow(unused_variables,unused_assignments,unused_imports,non_snake_case)]
pub fn expand_flags(
    inst: crate::ir::Inst,
    func: &mut crate::ir::Function,
    cfg: &mut crate::flowgraph::ControlFlowGraph,
    isa: &dyn crate::isa::TargetIsa,
) -> bool {
    use crate::ir::InstBuilder;
    use crate::cursor::{Cursor, FuncCursor};
    let mut pos = FuncCursor::new(func).at_inst(inst);
    pos.use_srcloc(inst);
    {
        match pos.func.dfg[inst].opcode() {
            ir::Opcode::Trapnz => {
                // Unwrap fields from instruction format () := trapnz(x, c)
                let (x, c, args) = if let ir::InstructionData::CondTrap {
                    code,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        code,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[1].contains(typeof_x);
                if predicate {
                    pos.func.dfg.clear_results(inst);
                    let a = pos.ins().ifcmp_imm(x, 0);
                    pos.ins().trapif(ir::condcodes::IntCC::NotEqual, a, c);
                    let removed = pos.remove_inst();
                    debug_assert_eq!(removed, inst);
                    return true;
                }
            }

            ir::Opcode::Trapz => {
                // Unwrap fields from instruction format () := trapz(x, c)
                let (x, c, args) = if let ir::InstructionData::CondTrap {
                    code,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        code,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1}, ints={8, 16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[1].contains(typeof_x);
                if predicate {
                    pos.func.dfg.clear_results(inst);
                    let a = pos.ins().ifcmp_imm(x, 0);
                    pos.ins().trapif(ir::condcodes::IntCC::Equal, a, c);
                    let removed = pos.remove_inst();
                    debug_assert_eq!(removed, inst);
                    return true;
                }
            }

            _ => {},
        }
    }
    crate::legalizer::expand(inst, func, cfg, isa)
}

/// Legalize instructions by narrowing.
///
/// The transformations in the 'narrow' group work by expressing
/// instructions in terms of smaller types. Operations on vector types are
/// expressed in terms of vector types with fewer lanes, and integer
/// operations are expressed in terms of smaller integer types.
#[allow(unused_variables,unused_assignments,unused_imports,non_snake_case)]
pub fn narrow(
    inst: crate::ir::Inst,
    func: &mut crate::ir::Function,
    cfg: &mut crate::flowgraph::ControlFlowGraph,
    isa: &dyn crate::isa::TargetIsa,
) -> bool {
    use crate::ir::InstBuilder;
    use crate::cursor::{Cursor, FuncCursor};
    let mut pos = FuncCursor::new(func).at_inst(inst);
    pos.use_srcloc(inst);
    {
        match pos.func.dfg[inst].opcode() {
            ir::Opcode::Band => {
                // Unwrap fields from instruction format a := band(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let al = pos.ins().band(xl, yl);
                    let ah = pos.ins().band(xh, yh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::BandNot => {
                // Unwrap fields from instruction format a := band_not(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let al = pos.ins().band_not(xl, yl);
                    let ah = pos.ins().band_not(xh, yh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Bitrev => {
                // Unwrap fields from instruction format a := bitrev.i128(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := iconcat(yl, yh).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let yh = pos.ins().bitrev(xl);
                    let yl = pos.ins().bitrev(xh);
                    let a = pos.func.dfg.replace(inst).iconcat(yl, yh);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Bnot => {
                // Unwrap fields from instruction format a := bnot(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let al = pos.ins().bnot(xl);
                    let ah = pos.ins().bnot(xh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Bor => {
                // Unwrap fields from instruction format a := bor(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let al = pos.ins().bor(xl, yl);
                    let ah = pos.ins().bor(xh, yh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::BorNot => {
                // Unwrap fields from instruction format a := bor_not(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let al = pos.ins().bor_not(xl, yl);
                    let ah = pos.ins().bor_not(xh, yh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Brnz => {
                // Unwrap fields from instruction format () := brnz.i128(x, block1, vararg)
                let (x, block1, args) = if let ir::InstructionData::Branch {
                    destination,
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    let args = args.as_slice(&pos.func.dfg.value_lists);
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        destination,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let vararg = &Vec::from(&args[1..]);

                if pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let orig_block = pos.current_block().unwrap();
                    pos.func.dfg.clear_results(inst);
                    let block2 = pos.func.dfg.make_block();
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    pos.ins().brnz(xl, block1, vararg);
                    pos.ins().jump(block2, &[]);
                    pos.insert_block(block2);
                    pos.ins().brnz(xh, block1, vararg);
                    let removed = pos.remove_inst();
                    debug_assert_eq!(removed, inst);
                    cfg.recompute_block(pos.func, orig_block);
                    cfg.recompute_block(pos.func, block2);
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let orig_block = pos.current_block().unwrap();
                    pos.func.dfg.clear_results(inst);
                    let block2 = pos.func.dfg.make_block();
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    pos.ins().brnz(xl, block1, vararg);
                    pos.ins().jump(block2, &[]);
                    pos.insert_block(block2);
                    pos.ins().brnz(xh, block1, vararg);
                    let removed = pos.remove_inst();
                    debug_assert_eq!(removed, inst);
                    cfg.recompute_block(pos.func, orig_block);
                    cfg.recompute_block(pos.func, block2);
                    return true;
                }
            }

            ir::Opcode::Brz => {
                // Unwrap fields from instruction format () := brz.i128(x, block, vararg)
                let (x, block, args) = if let ir::InstructionData::Branch {
                    destination,
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    let args = args.as_slice(&pos.func.dfg.value_lists);
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        destination,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let vararg = &Vec::from(&args[1..]);

                if pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    pos.func.dfg.clear_results(inst);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let a = pos.ins().icmp_imm(ir::condcodes::IntCC::Equal, xl, 0);
                    let b = pos.ins().icmp_imm(ir::condcodes::IntCC::Equal, xh, 0);
                    let c = pos.ins().band(a, b);
                    pos.ins().brnz(c, block, vararg);
                    let removed = pos.remove_inst();
                    debug_assert_eq!(removed, inst);
                    cfg.recompute_block(pos.func, pos.current_block().unwrap());
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    pos.func.dfg.clear_results(inst);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let a = pos.ins().icmp_imm(ir::condcodes::IntCC::Equal, xl, 0);
                    let b = pos.ins().icmp_imm(ir::condcodes::IntCC::Equal, xh, 0);
                    let c = pos.ins().band(a, b);
                    pos.ins().brnz(c, block, vararg);
                    let removed = pos.remove_inst();
                    debug_assert_eq!(removed, inst);
                    cfg.recompute_block(pos.func, pos.current_block().unwrap());
                    return true;
                }
            }

            ir::Opcode::Bxor => {
                // Unwrap fields from instruction format a := bxor(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let al = pos.ins().bxor(xl, yl);
                    let ah = pos.ins().bxor(xh, yh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::BxorNot => {
                // Unwrap fields from instruction format a := bxor_not(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let al = pos.ins().bxor_not(xl, yl);
                    let ah = pos.ins().bxor_not(xh, yh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::IaddImm => {
                // Unwrap fields from instruction format a := iadd_imm.i128(x, c)
                let (x, c, args) = if let ir::InstructionData::BinaryImm64 {
                    imm,
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        imm,
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := iadd(x, y).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let yh = pos.ins().iconst(ir::types::I128.half_width().unwrap(), 0);
                    let yl = pos.ins().iconst(ir::types::I128.half_width().unwrap(), c);
                    let y = pos.ins().iconcat(yh, yl);
                    let a = pos.func.dfg.replace(inst).iadd(x, y);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Icmp => {
                // Unwrap fields from instruction format b := icmp.i64(ir::condcodes::IntCC::Equal, x, y)
                let (cond, x, y, args) = if let ir::InstructionData::IntCompare {
                    cond,
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        cond,
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by b := band(b1, b2).
                let r = pos.func.dfg.inst_results(inst);
                let b = &r[0];
                let typeof_b = pos.func.dfg.value_type(*b);

                if predicates::is_equal(cond, ir::condcodes::IntCC::Equal) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::Equal, xl, yl);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::Equal, xh, yh);
                    let b = pos.func.dfg.replace(inst).band(b1, b2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::NotEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::NotEqual, xl, yl);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::NotEqual, xh, yh);
                    let b = pos.func.dfg.replace(inst).bor(b1, b2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedGreaterThan) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedGreaterThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedLessThan) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedLessThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedGreaterThan) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedGreaterThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedLessThan) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedLessThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::Equal) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::Equal, xl, yl);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::Equal, xh, yh);
                    let b = pos.func.dfg.replace(inst).band(b1, b2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::NotEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::NotEqual, xl, yl);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::NotEqual, xh, yh);
                    let b = pos.func.dfg.replace(inst).bor(b1, b2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedGreaterThan) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedGreaterThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedLessThan) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::SignedLessThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::SignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::SignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedGreaterThan) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedGreaterThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedLessThan) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if predicates::is_equal(cond, ir::condcodes::IntCC::UnsignedLessThanOrEqual) && pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let b1 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThan, xh, yh);
                    let b2 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedGreaterThan, xh, yh);
                    let b3 = pos.ins().icmp(ir::condcodes::IntCC::UnsignedLessThanOrEqual, xl, yl);
                    let c1 = pos.ins().bnot(b2);
                    let c2 = pos.ins().band(c1, b3);
                    let b = pos.func.dfg.replace(inst).bor(b1, c2);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Imul => {
                // Unwrap fields from instruction format a := imul.i64(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I64 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let a1 = pos.ins().imul(xh, yl);
                    let a2 = pos.ins().imul(xl, yh);
                    let a3 = pos.ins().iadd(a1, a2);
                    let a4 = pos.ins().umulhi(xl, yl);
                    let ah = pos.ins().iadd(a3, a4);
                    let al = pos.ins().imul(xl, yl);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let a1 = pos.ins().imul(xh, yl);
                    let a2 = pos.ins().imul(xl, yh);
                    let a3 = pos.ins().iadd(a1, a2);
                    let a4 = pos.ins().umulhi(xl, yl);
                    let ah = pos.ins().iadd(a3, a4);
                    let al = pos.ins().imul(xl, yl);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Popcnt => {
                // Unwrap fields from instruction format a := popcnt.i128(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := uextend(e3).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I128 {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let e1 = pos.ins().popcnt(xl);
                    let e2 = pos.ins().popcnt(xh);
                    let e3 = pos.ins().iadd(e1, e2);
                    let a = pos.func.dfg.replace(inst).uextend(ir::types::I128, e3);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Select => {
                // Unwrap fields from instruction format a := select(c, x, y)
                let (c, x, y, args) = if let ir::InstructionData::Ternary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        pos.func.dfg.resolve_aliases(args[2]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_c = pos.func.dfg.value_type(c);
                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[2].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let al = pos.ins().select(c, xl, yl);
                    let ah = pos.ins().select(c, xh, yh);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Sextend => {
                // Unwrap fields from instruction format a := sextend.i128.i64(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := iconcat(x, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I64 && pos.func.dfg.ctrl_typevar(inst) == ir::types::I128 {
                    let ah = pos.ins().sshr_imm(x, 63);
                    let a = pos.func.dfg.replace(inst).iconcat(x, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I32 && pos.func.dfg.ctrl_typevar(inst) == ir::types::I64 {
                    let ah = pos.ins().sshr_imm(x, 31);
                    let a = pos.func.dfg.replace(inst).iconcat(x, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Uextend => {
                // Unwrap fields from instruction format a := uextend.i128.i64(x)
                let (x, args) = if let ir::InstructionData::Unary {
                    arg,
                    ..
                } = pos.func.dfg[inst] {
                    let args = [arg];
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                // Results handled by a := iconcat(x, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                if pos.func.dfg.value_type(args[0]) == ir::types::I64 && pos.func.dfg.ctrl_typevar(inst) == ir::types::I128 {
                    let ah = pos.ins().iconst(ir::types::I64, 0);
                    let a = pos.func.dfg.replace(inst).iconcat(x, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }

                if pos.func.dfg.value_type(args[0]) == ir::types::I32 && pos.func.dfg.ctrl_typevar(inst) == ir::types::I64 {
                    let ah = pos.ins().iconst(ir::types::I32, 0);
                    let a = pos.func.dfg.replace(inst).iconcat(x, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::IcmpImm => {
                narrow_icmp_imm(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::Iconst => {
                narrow_iconst(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::Load => {
                narrow_load(inst, func, cfg, isa);
                return true;
            }

            ir::Opcode::Store => {
                narrow_store(inst, func, cfg, isa);
                return true;
            }

            _ => {},
        }
    }
    false
}

/// Narrow instructions for architectures with flags.
///
/// Narrow some instructions using CPU flags, then fall back to the normal
/// legalizations. Not all architectures support CPU flags, so these
/// patterns are kept separate.
#[allow(unused_variables,unused_assignments,unused_imports,non_snake_case)]
pub fn narrow_flags(
    inst: crate::ir::Inst,
    func: &mut crate::ir::Function,
    cfg: &mut crate::flowgraph::ControlFlowGraph,
    isa: &dyn crate::isa::TargetIsa,
) -> bool {
    use crate::ir::InstBuilder;
    use crate::cursor::{Cursor, FuncCursor};
    let mut pos = FuncCursor::new(func).at_inst(inst);
    pos.use_srcloc(inst);
    {
        match pos.func.dfg[inst].opcode() {
            ir::Opcode::Iadd => {
                // Unwrap fields from instruction format a := iadd(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[3].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let (al, c) = pos.ins().iadd_ifcout(xl, yl);
                    let ah = pos.ins().iadd_ifcin(xh, yh, c);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            ir::Opcode::Isub => {
                // Unwrap fields from instruction format a := isub(x, y)
                let (x, y, args) = if let ir::InstructionData::Binary {
                    ref args,
                    ..
                } = pos.func.dfg[inst] {
                    (
                        pos.func.dfg.resolve_aliases(args[0]),
                        pos.func.dfg.resolve_aliases(args[1]),
                        args
                    )
                } else {
                    unreachable!("bad instruction format")
                };

                let typeof_x = pos.func.dfg.value_type(x);
                // Results handled by a := iconcat(al, ah).
                let r = pos.func.dfg.inst_results(inst);
                let a = &r[0];
                let typeof_a = pos.func.dfg.value_type(*a);

                let predicate = true;
                // typeof_x must belong to TypeSet(lanes={1}, ints={16, 32, 64, 128})
                let predicate = predicate && TYPE_SETS[3].contains(typeof_x);
                if predicate {
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (xl, xh) = split::isplit(pos.func, cfg, curpos, srcloc, x);
                    let curpos = pos.position();
                    let srcloc = pos.srcloc();
                    let (yl, yh) = split::isplit(pos.func, cfg, curpos, srcloc, y);
                    let (al, b) = pos.ins().isub_ifbout(xl, yl);
                    let ah = pos.ins().isub_ifbin(xh, yh, b);
                    let a = pos.func.dfg.replace(inst).iconcat(al, ah);
                    if pos.current_inst() == Some(inst) {
                        pos.next_inst();
                    }
                    return true;
                }
            }

            _ => {},
        }
    }
    crate::legalizer::narrow(inst, func, cfg, isa)
}

// Table of value type sets.
const TYPE_SETS: [ir::instructions::ValueTypeSet; 4] = [
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={8, 16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(248),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
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
        // TypeSet(lanes={1, 2, 4, 8, 16, 32, 64, 128, 256}, ints={16, 32, 64, 128})
        lanes: BitSet::<u16>(511),
        ints: BitSet::<u8>(240),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
    ir::instructions::ValueTypeSet {
        // TypeSet(lanes={1}, ints={16, 32, 64, 128})
        lanes: BitSet::<u16>(1),
        ints: BitSet::<u8>(240),
        floats: BitSet::<u8>(0),
        bools: BitSet::<u8>(0),
        refs: BitSet::<u8>(0),
    },
];
