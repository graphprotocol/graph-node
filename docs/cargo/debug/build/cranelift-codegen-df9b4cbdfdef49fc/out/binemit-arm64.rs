/// Emit binary machine code for `inst` for the arm64 ISA.
pub fn emit_inst<CS: CodeSink + ?Sized>(
    func: &Function,
    inst: Inst,
    _divert: &mut RegDiversions,
    _sink: &mut CS,
    _isa: &dyn TargetIsa,
) {
    bad_encoding(func, inst)
}
