// arm64 recipe predicates.

/// arm64 recipe predicate table.
///
/// One entry per recipe, set to Some only when the recipe is guarded by a predicate.
pub static RECIPE_PREDICATES: [RecipePredicate; 0] = [
];

// arm64 instruction predicates.

/// arm64 instruction predicate table.
///
/// One entry per instruction predicate, so the encoding bytecode can embed indexes into this
/// table.
pub static INST_PREDICATES: [InstPredicate; 0] = [
];

/// arm64 encoding lists.
///
/// This contains the entire encodings bytecode for every single instruction; the encodings
/// interpreter knows where to start from thanks to the initial lookup in the level 1 and level 2
/// table entries below.
pub static ENCLISTS: [u16; 0] = [
];

/// arm64 level 2 hash tables.
///
/// This hash table, keyed by instruction opcode, contains all the starting offsets for the
/// encodings interpreter, for all the CPU modes. It is jumped to after a lookup on the
/// instruction's controlling type in the level 1 hash table.
pub static LEVEL2: [Level2Entry<u16>; 0] = [
];

/// arm64 level 1 hash table for the CPU mode A64.
///
/// This hash table, keyed by instruction controlling type, contains all the level 2
/// hash-tables offsets for the given CPU mode, as well as a legalization identifier indicating
/// which legalization scheme to apply when the instruction doesn't have any valid encoding for
/// this CPU mode.
pub static LEVEL1_A64: [Level1Entry<u16>; 2] = [
    Level1Entry { ty: ir::types::INVALID, log2len: 0, offset: !0 - 1, legalize: 0 }, // expand_flags
    Level1Entry { ty: ir::types::INVALID, log2len: !0, offset: 0, legalize: 1 },
];

/// arm64 recipe names, using the same recipe index spaces as the one specified by the
/// corresponding binemit file.
static RECIPE_NAMES: [&str; 0] = [
];

/// arm64 recipe constraints list, using the same recipe index spaces as the one
/// specified by the corresponding binemit file. These constraints are used by register
/// allocation to select the right location to use for input and output values.
static RECIPE_CONSTRAINTS: [RecipeConstraints; 0] = [
];

/// arm64 recipe sizing descriptors, using the same recipe index spaces as the one
/// specified by the corresponding binemit file. These are used to compute the final size of an
/// instruction, as well as to compute the range of branches.
static RECIPE_SIZING: [RecipeSizing; 0] = [
];

pub static INFO: isa::EncInfo = isa::EncInfo {
    constraints: &RECIPE_CONSTRAINTS,
    sizing: &RECIPE_SIZING,
    names: &RECIPE_NAMES,
};
