// TODO: Remove this once there is a better way to get this information from Amp servers.
pub(super) mod column_aliases {
    pub(in crate::amp) static BLOCK_NUMBER: &[&str] = &[
        "_block_num",   // Meta column present in all tables
        "block_num",    // Standard column in most raw tables
        "blockNum",     // Common alternative name
        "blocknum",     // Common alternative name
        "block",        // Common alternative name
        "block_number", // Common alternative name
        "blockNumber",  // Common alternative name
        "blocknumber",  // Common alternative name
    ];
    pub(in crate::amp) static BLOCK_HASH: &[&str] = &[
        "hash",       // Standard column in some raw tables
        "block_hash", // Standard column in most raw tables and common alternative name
        "blockHash",  // Common alternative name
        "blockhash",  // Common alternative name
    ];
    pub(in crate::amp) static BLOCK_TIMESTAMP: &[&str] = &[
        "timestamp",       // Standard column in most raw tables
        "block_timestamp", // Common alternative name
        "blockTimestamp",  // Common alternative name
        "blocktimestamp",  // Common alternative name
    ];
}
