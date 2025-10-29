mod ident;

pub use self::ident::Ident;

pub(super) mod column_aliases {
    pub(in crate::amp) static BLOCK_NUMBER: &[&str] = &[
        "_block_num",   // Meta column present in all tables
        "block_num",    // Standard column in most raw tables
        "block",        // Common alternative name
        "block_number", // Common alternative name
    ];
    pub(in crate::amp) static BLOCK_HASH: &[&str] = &[
        "hash",       // Standard column in some raw tables
        "block_hash", // Standard column in most raw tables and common alternative name
    ];
    pub(in crate::amp) static BLOCK_TIMESTAMP: &[&str] = &[
        "timestamp",       // Standard column in most raw tables
        "block_timestamp", // Common alternative name
    ];
}
