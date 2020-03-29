use std::fmt;

use crate::data::store::scalar::Bytes;

#[derive(Debug, PartialEq)]
pub struct BlockPointer {
    pub number: u64,
    pub hash: Bytes,
}

impl fmt::Display for BlockPointer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{} ({})", self.number, self.hash.as_hex_string())
    }
}
