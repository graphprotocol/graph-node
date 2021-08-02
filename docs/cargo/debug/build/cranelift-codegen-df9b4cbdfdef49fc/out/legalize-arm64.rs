pub static LEGALIZE_ACTIONS: [isa::Legalize; 2] = [
    crate::legalizer::expand_flags,
    crate::legalizer::narrow_flags,
];
