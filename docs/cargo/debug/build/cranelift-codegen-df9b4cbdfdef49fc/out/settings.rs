#[derive(Clone, Hash)]
/// Flags group `shared`.
pub struct Flags {
    bytes: [u8; 9],
}
impl Flags {
    /// Create flags shared settings group.
    #[allow(unused_variables)]
    pub fn new(builder: Builder) -> Self {
        let bvec = builder.state_for("shared");
        let mut shared = Self { bytes: [0; 9] };
        debug_assert_eq!(bvec.len(), 9);
        shared.bytes[0..9].copy_from_slice(&bvec);
        shared
    }
}
impl Flags {
    /// Iterates the setting values.
    pub fn iter(&self) -> impl Iterator<Item = Value> {
        let mut bytes = [0; 9];
        bytes.copy_from_slice(&self.bytes[0..9]);
        DESCRIPTORS.iter().filter_map(move |d| {
            let values = match &d.detail {
                detail::Detail::Preset => return None,
                detail::Detail::Enum { last, enumerators } => Some(TEMPLATE.enums(*last, *enumerators)),
                _ => None
            };
            Some(Value{ name: d.name, detail: d.detail, values, value: bytes[d.offset as usize] })
        })
    }
}
/// Values for `shared.regalloc`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Regalloc {
    /// `backtracking`.
    Backtracking,
    /// `backtracking_checked`.
    BacktrackingChecked,
    /// `experimental_linear_scan`.
    ExperimentalLinearScan,
    /// `experimental_linear_scan_checked`.
    ExperimentalLinearScanChecked,
}
impl fmt::Display for Regalloc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match *self {
            Self::Backtracking => "backtracking",
            Self::BacktrackingChecked => "backtracking_checked",
            Self::ExperimentalLinearScan => "experimental_linear_scan",
            Self::ExperimentalLinearScanChecked => "experimental_linear_scan_checked",
        })
    }
}
impl str::FromStr for Regalloc {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "backtracking" => Ok(Self::Backtracking),
            "backtracking_checked" => Ok(Self::BacktrackingChecked),
            "experimental_linear_scan" => Ok(Self::ExperimentalLinearScan),
            "experimental_linear_scan_checked" => Ok(Self::ExperimentalLinearScanChecked),
            _ => Err(()),
        }
    }
}
/// Values for `shared.opt_level`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum OptLevel {
    /// `none`.
    None,
    /// `speed`.
    Speed,
    /// `speed_and_size`.
    SpeedAndSize,
}
impl fmt::Display for OptLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match *self {
            Self::None => "none",
            Self::Speed => "speed",
            Self::SpeedAndSize => "speed_and_size",
        })
    }
}
impl str::FromStr for OptLevel {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "speed" => Ok(Self::Speed),
            "speed_and_size" => Ok(Self::SpeedAndSize),
            _ => Err(()),
        }
    }
}
/// Values for `shared.tls_model`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TlsModel {
    /// `none`.
    None,
    /// `elf_gd`.
    ElfGd,
    /// `macho`.
    Macho,
    /// `coff`.
    Coff,
}
impl fmt::Display for TlsModel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match *self {
            Self::None => "none",
            Self::ElfGd => "elf_gd",
            Self::Macho => "macho",
            Self::Coff => "coff",
        })
    }
}
impl str::FromStr for TlsModel {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "elf_gd" => Ok(Self::ElfGd),
            "macho" => Ok(Self::Macho),
            "coff" => Ok(Self::Coff),
            _ => Err(()),
        }
    }
}
/// Values for `shared.libcall_call_conv`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum LibcallCallConv {
    /// `isa_default`.
    IsaDefault,
    /// `fast`.
    Fast,
    /// `cold`.
    Cold,
    /// `system_v`.
    SystemV,
    /// `windows_fastcall`.
    WindowsFastcall,
    /// `apple_aarch64`.
    AppleAarch64,
    /// `baldrdash_system_v`.
    BaldrdashSystemV,
    /// `baldrdash_windows`.
    BaldrdashWindows,
    /// `baldrdash_2020`.
    Baldrdash2020,
    /// `probestack`.
    Probestack,
}
impl fmt::Display for LibcallCallConv {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match *self {
            Self::IsaDefault => "isa_default",
            Self::Fast => "fast",
            Self::Cold => "cold",
            Self::SystemV => "system_v",
            Self::WindowsFastcall => "windows_fastcall",
            Self::AppleAarch64 => "apple_aarch64",
            Self::BaldrdashSystemV => "baldrdash_system_v",
            Self::BaldrdashWindows => "baldrdash_windows",
            Self::Baldrdash2020 => "baldrdash_2020",
            Self::Probestack => "probestack",
        })
    }
}
impl str::FromStr for LibcallCallConv {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "isa_default" => Ok(Self::IsaDefault),
            "fast" => Ok(Self::Fast),
            "cold" => Ok(Self::Cold),
            "system_v" => Ok(Self::SystemV),
            "windows_fastcall" => Ok(Self::WindowsFastcall),
            "apple_aarch64" => Ok(Self::AppleAarch64),
            "baldrdash_system_v" => Ok(Self::BaldrdashSystemV),
            "baldrdash_windows" => Ok(Self::BaldrdashWindows),
            "baldrdash_2020" => Ok(Self::Baldrdash2020),
            "probestack" => Ok(Self::Probestack),
            _ => Err(()),
        }
    }
}
/// User-defined settings.
#[allow(dead_code)]
impl Flags {
    /// Get a view of the boolean predicates.
    pub fn predicate_view(&self) -> crate::settings::PredicateView {
        crate::settings::PredicateView::new(&self.bytes[6..])
    }
    /// Dynamic numbered predicate getter.
    fn numbered_predicate(&self, p: usize) -> bool {
        self.bytes[6 + p / 8] & (1 << (p % 8)) != 0
    }
    /// Register allocator to use with the MachInst backend.
    ///
    /// This selects the register allocator as an option among those offered by the `regalloc.rs`
    /// crate. Please report register allocation bugs to the maintainers of this crate whenever
    /// possible.
    ///
    /// Note: this only applies to target that use the MachInst backend. As of 2020-04-17, this
    /// means the x86_64 backend doesn't use this yet.
    ///
    /// Possible values:
    ///
    /// - `backtracking` is a greedy, backtracking register allocator as implemented in
    /// Spidermonkey's optimizing tier IonMonkey. It may take more time to allocate registers, but
    /// it should generate better code in general, resulting in better throughput of generated
    /// code.
    /// - `backtracking_checked` is the backtracking allocator with additional self checks that may
    /// take some time to run, and thus these checks are disabled by default.
    /// - `experimental_linear_scan` is an experimental linear scan allocator. It may take less
    /// time to allocate registers, but generated code's quality may be inferior. As of
    /// 2020-04-17, it is still experimental and it should not be used in production settings.
    /// - `experimental_linear_scan_checked` is the linear scan allocator with additional self
    /// checks that may take some time to run, and thus these checks are disabled by default.
    pub fn regalloc(&self) -> Regalloc {
        match self.bytes[0] {
            0 => {
                Regalloc::Backtracking
            }
            1 => {
                Regalloc::BacktrackingChecked
            }
            2 => {
                Regalloc::ExperimentalLinearScan
            }
            3 => {
                Regalloc::ExperimentalLinearScanChecked
            }
            _ => {
                panic!("Invalid enum value")
            }
        }
    }
    /// Optimization level for generated code.
    ///
    /// Supported levels:
    ///
    /// - `none`: Minimise compile time by disabling most optimizations.
    /// - `speed`: Generate the fastest possible code
    /// - `speed_and_size`: like "speed", but also perform transformations aimed at reducing code size.
    pub fn opt_level(&self) -> OptLevel {
        match self.bytes[1] {
            0 => {
                OptLevel::None
            }
            1 => {
                OptLevel::Speed
            }
            2 => {
                OptLevel::SpeedAndSize
            }
            _ => {
                panic!("Invalid enum value")
            }
        }
    }
    /// Defines the model used to perform TLS accesses.
    pub fn tls_model(&self) -> TlsModel {
        match self.bytes[2] {
            3 => {
                TlsModel::Coff
            }
            1 => {
                TlsModel::ElfGd
            }
            2 => {
                TlsModel::Macho
            }
            0 => {
                TlsModel::None
            }
            _ => {
                panic!("Invalid enum value")
            }
        }
    }
    /// Defines the calling convention to use for LibCalls call expansion.
    ///
    /// This may be different from the ISA default calling convention.
    ///
    /// The default value is to use the same calling convention as the ISA
    /// default calling convention.
    ///
    /// This list should be kept in sync with the list of calling
    /// conventions available in isa/call_conv.rs.
    pub fn libcall_call_conv(&self) -> LibcallCallConv {
        match self.bytes[3] {
            5 => {
                LibcallCallConv::AppleAarch64
            }
            8 => {
                LibcallCallConv::Baldrdash2020
            }
            6 => {
                LibcallCallConv::BaldrdashSystemV
            }
            7 => {
                LibcallCallConv::BaldrdashWindows
            }
            2 => {
                LibcallCallConv::Cold
            }
            1 => {
                LibcallCallConv::Fast
            }
            0 => {
                LibcallCallConv::IsaDefault
            }
            9 => {
                LibcallCallConv::Probestack
            }
            3 => {
                LibcallCallConv::SystemV
            }
            4 => {
                LibcallCallConv::WindowsFastcall
            }
            _ => {
                panic!("Invalid enum value")
            }
        }
    }
    /// Number of pointer-sized words pushed by the baldrdash prologue.
    ///
    /// Functions with the `baldrdash` calling convention don't generate their
    /// own prologue and epilogue. They depend on externally generated code
    /// that pushes a fixed number of words in the prologue and restores them
    /// in the epilogue.
    ///
    /// This setting configures the number of pointer-sized words pushed on the
    /// stack when the Cranelift-generated code is entered. This includes the
    /// pushed return address on x86.
    pub fn baldrdash_prologue_words(&self) -> u8 {
        self.bytes[4]
    }
    /// The log2 of the size of the stack guard region.
    ///
    /// Stack frames larger than this size will have stack overflow checked
    /// by calling the probestack function.
    ///
    /// The default is 12, which translates to a size of 4096.
    pub fn probestack_size_log2(&self) -> u8 {
        self.bytes[5]
    }
    /// Run the Cranelift IR verifier at strategic times during compilation.
    ///
    /// This makes compilation slower but catches many bugs. The verifier is always enabled by
    /// default, which is useful during development.
    pub fn enable_verifier(&self) -> bool {
        self.numbered_predicate(0)
    }
    /// Enable Position-Independent Code generation.
    pub fn is_pic(&self) -> bool {
        self.numbered_predicate(1)
    }
    /// Use colocated libcalls.
    ///
    /// Generate code that assumes that libcalls can be declared "colocated",
    /// meaning they will be defined along with the current function, such that
    /// they can use more efficient addressing.
    pub fn use_colocated_libcalls(&self) -> bool {
        self.numbered_predicate(2)
    }
    /// Generate explicit checks around native division instructions to avoid their trapping.
    ///
    /// This is primarily used by SpiderMonkey which doesn't install a signal
    /// handler for SIGFPE, but expects a SIGILL trap for division by zero.
    ///
    /// On ISAs like ARM where the native division instructions don't trap,
    /// this setting has no effect - explicit checks are always inserted.
    pub fn avoid_div_traps(&self) -> bool {
        self.numbered_predicate(3)
    }
    /// Enable the use of floating-point instructions.
    ///
    /// Disabling use of floating-point instructions is not yet implemented.
    pub fn enable_float(&self) -> bool {
        self.numbered_predicate(4)
    }
    /// Enable NaN canonicalization.
    ///
    /// This replaces NaNs with a single canonical value, for users requiring
    /// entirely deterministic WebAssembly computation. This is not required
    /// by the WebAssembly spec, so it is not enabled by default.
    pub fn enable_nan_canonicalization(&self) -> bool {
        self.numbered_predicate(5)
    }
    /// Enable the use of the pinned register.
    ///
    /// This register is excluded from register allocation, and is completely under the control of
    /// the end-user. It is possible to read it via the get_pinned_reg instruction, and to set it
    /// with the set_pinned_reg instruction.
    pub fn enable_pinned_reg(&self) -> bool {
        self.numbered_predicate(6)
    }
    /// Use the pinned register as the heap base.
    ///
    /// Enabling this requires the enable_pinned_reg setting to be set to true. It enables a custom
    /// legalization of the `heap_addr` instruction so it will use the pinned register as the heap
    /// base, instead of fetching it from a global value.
    ///
    /// Warning! Enabling this means that the pinned register *must* be maintained to contain the
    /// heap base address at all times, during the lifetime of a function. Using the pinned
    /// register for other purposes when this is set is very likely to cause crashes.
    pub fn use_pinned_reg_as_heap_base(&self) -> bool {
        self.numbered_predicate(7)
    }
    /// Enable the use of SIMD instructions.
    pub fn enable_simd(&self) -> bool {
        self.numbered_predicate(8)
    }
    /// Enable the use of atomic instructions
    pub fn enable_atomics(&self) -> bool {
        self.numbered_predicate(9)
    }
    /// Enable safepoint instruction insertions.
    ///
    /// This will allow the emit_stack_maps() function to insert the safepoint
    /// instruction on top of calls and interrupt traps in order to display the
    /// live reference values at that point in the program.
    pub fn enable_safepoints(&self) -> bool {
        self.numbered_predicate(10)
    }
    /// Enable various ABI extensions defined by LLVM's behavior.
    ///
    /// In some cases, LLVM's implementation of an ABI (calling convention)
    /// goes beyond a standard and supports additional argument types or
    /// behavior. This option instructs Cranelift codegen to follow LLVM's
    /// behavior where applicable.
    ///
    /// Currently, this applies only to Windows Fastcall on x86-64, and
    /// allows an `i128` argument to be spread across two 64-bit integer
    /// registers. The Fastcall implementation otherwise does not support
    /// `i128` arguments, and will panic if they are present and this
    /// option is not set.
    pub fn enable_llvm_abi_extensions(&self) -> bool {
        self.numbered_predicate(11)
    }
    /// Generate unwind information.
    ///
    /// This increases metadata size and compile time, but allows for the
    /// debugger to trace frames, is needed for GC tracing that relies on
    /// libunwind (such as in Wasmtime), and is unconditionally needed on
    /// certain platforms (such as Windows) that must always be able to unwind.
    pub fn unwind_info(&self) -> bool {
        self.numbered_predicate(12)
    }
    /// Emit not-yet-relocated function addresses as all-ones bit patterns.
    pub fn emit_all_ones_funcaddrs(&self) -> bool {
        self.numbered_predicate(13)
    }
    /// Enable the use of stack probes for supported calling conventions.
    pub fn enable_probestack(&self) -> bool {
        self.numbered_predicate(14)
    }
    /// Enable if the stack probe adjusts the stack pointer.
    pub fn probestack_func_adjusts_sp(&self) -> bool {
        self.numbered_predicate(15)
    }
    /// Enable the use of jump tables in generated machine code.
    pub fn enable_jump_tables(&self) -> bool {
        self.numbered_predicate(16)
    }
    /// Enable Spectre mitigation on heap bounds checks.
    ///
    /// This is a no-op for any heap that needs no bounds checks; e.g.,
    /// if the limit is static and the guard region is large enough that
    /// the index cannot reach past it.
    ///
    /// This option is enabled by default because it is highly
    /// recommended for secure sandboxing. The embedder should consider
    /// the security implications carefully before disabling this option.
    pub fn enable_heap_access_spectre_mitigation(&self) -> bool {
        self.numbered_predicate(17)
    }
}
static DESCRIPTORS: [detail::Descriptor; 24] = [
    detail::Descriptor {
        name: "regalloc",
        description: "Register allocator to use with the MachInst backend.",
        offset: 0,
        detail: detail::Detail::Enum { last: 3, enumerators: 0 },
    },
    detail::Descriptor {
        name: "opt_level",
        description: "Optimization level for generated code.",
        offset: 1,
        detail: detail::Detail::Enum { last: 2, enumerators: 4 },
    },
    detail::Descriptor {
        name: "tls_model",
        description: "Defines the model used to perform TLS accesses.",
        offset: 2,
        detail: detail::Detail::Enum { last: 3, enumerators: 7 },
    },
    detail::Descriptor {
        name: "libcall_call_conv",
        description: "Defines the calling convention to use for LibCalls call expansion.",
        offset: 3,
        detail: detail::Detail::Enum { last: 9, enumerators: 11 },
    },
    detail::Descriptor {
        name: "baldrdash_prologue_words",
        description: "Number of pointer-sized words pushed by the baldrdash prologue.",
        offset: 4,
        detail: detail::Detail::Num,
    },
    detail::Descriptor {
        name: "probestack_size_log2",
        description: "The log2 of the size of the stack guard region.",
        offset: 5,
        detail: detail::Detail::Num,
    },
    detail::Descriptor {
        name: "enable_verifier",
        description: "Run the Cranelift IR verifier at strategic times during compilation.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 0 },
    },
    detail::Descriptor {
        name: "is_pic",
        description: "Enable Position-Independent Code generation.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 1 },
    },
    detail::Descriptor {
        name: "use_colocated_libcalls",
        description: "Use colocated libcalls.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 2 },
    },
    detail::Descriptor {
        name: "avoid_div_traps",
        description: "Generate explicit checks around native division instructions to avoid their trapping.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 3 },
    },
    detail::Descriptor {
        name: "enable_float",
        description: "Enable the use of floating-point instructions.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 4 },
    },
    detail::Descriptor {
        name: "enable_nan_canonicalization",
        description: "Enable NaN canonicalization.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 5 },
    },
    detail::Descriptor {
        name: "enable_pinned_reg",
        description: "Enable the use of the pinned register.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 6 },
    },
    detail::Descriptor {
        name: "use_pinned_reg_as_heap_base",
        description: "Use the pinned register as the heap base.",
        offset: 6,
        detail: detail::Detail::Bool { bit: 7 },
    },
    detail::Descriptor {
        name: "enable_simd",
        description: "Enable the use of SIMD instructions.",
        offset: 7,
        detail: detail::Detail::Bool { bit: 0 },
    },
    detail::Descriptor {
        name: "enable_atomics",
        description: "Enable the use of atomic instructions",
        offset: 7,
        detail: detail::Detail::Bool { bit: 1 },
    },
    detail::Descriptor {
        name: "enable_safepoints",
        description: "Enable safepoint instruction insertions.",
        offset: 7,
        detail: detail::Detail::Bool { bit: 2 },
    },
    detail::Descriptor {
        name: "enable_llvm_abi_extensions",
        description: "Enable various ABI extensions defined by LLVM's behavior.",
        offset: 7,
        detail: detail::Detail::Bool { bit: 3 },
    },
    detail::Descriptor {
        name: "unwind_info",
        description: "Generate unwind information.",
        offset: 7,
        detail: detail::Detail::Bool { bit: 4 },
    },
    detail::Descriptor {
        name: "emit_all_ones_funcaddrs",
        description: "Emit not-yet-relocated function addresses as all-ones bit patterns.",
        offset: 7,
        detail: detail::Detail::Bool { bit: 5 },
    },
    detail::Descriptor {
        name: "enable_probestack",
        description: "Enable the use of stack probes for supported calling conventions.",
        offset: 7,
        detail: detail::Detail::Bool { bit: 6 },
    },
    detail::Descriptor {
        name: "probestack_func_adjusts_sp",
        description: "Enable if the stack probe adjusts the stack pointer.",
        offset: 7,
        detail: detail::Detail::Bool { bit: 7 },
    },
    detail::Descriptor {
        name: "enable_jump_tables",
        description: "Enable the use of jump tables in generated machine code.",
        offset: 8,
        detail: detail::Detail::Bool { bit: 0 },
    },
    detail::Descriptor {
        name: "enable_heap_access_spectre_mitigation",
        description: "Enable Spectre mitigation on heap bounds checks.",
        offset: 8,
        detail: detail::Detail::Bool { bit: 1 },
    },
];
static ENUMERATORS: [&str; 21] = [
    "backtracking",
    "backtracking_checked",
    "experimental_linear_scan",
    "experimental_linear_scan_checked",
    "none",
    "speed",
    "speed_and_size",
    "none",
    "elf_gd",
    "macho",
    "coff",
    "isa_default",
    "fast",
    "cold",
    "system_v",
    "windows_fastcall",
    "apple_aarch64",
    "baldrdash_system_v",
    "baldrdash_windows",
    "baldrdash_2020",
    "probestack",
];
static HASH_TABLE: [u16; 32] = [
    0xffff,
    20,
    0xffff,
    2,
    11,
    6,
    15,
    13,
    14,
    0,
    21,
    18,
    5,
    17,
    16,
    0xffff,
    12,
    23,
    0xffff,
    9,
    19,
    8,
    22,
    3,
    0xffff,
    0xffff,
    0xffff,
    0xffff,
    1,
    4,
    10,
    7,
];
static PRESETS: [(u8, u8); 0] = [
];
static TEMPLATE: detail::Template = detail::Template {
    name: "shared",
    descriptors: &DESCRIPTORS,
    enumerators: &ENUMERATORS,
    hash_table: &HASH_TABLE,
    defaults: &[0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x11, 0x52, 0x03],
    presets: &PRESETS,
};
/// Create a `settings::Builder` for the shared settings group.
pub fn builder() -> Builder {
    Builder::new(&TEMPLATE)
}
impl fmt::Display for Flags {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "[shared]")?;
        for d in &DESCRIPTORS {
            if !d.detail.is_preset() {
                write!(f, "{} = ", d.name)?;
                TEMPLATE.format_toml_value(d.detail, self.bytes[d.offset as usize], f)?;
                writeln!(f)?;
            }
        }
        Ok(())
    }
}
