mod ident;

pub use self::ident::Ident;

pub(super) mod column_aliases {
    pub(in crate::nozzle) static BLOCK_NUMBER: &[&str] = &["_block_num", "block_num"];
    pub(in crate::nozzle) static BLOCK_HASH: &[&str] = &["hash", "block_hash"];
    pub(in crate::nozzle) static BLOCK_TIMESTAMP: &[&str] = &["timestamp"];
}
