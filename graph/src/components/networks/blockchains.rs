use crate::data::store::scalar::Bytes;

#[derive(Debug, PartialEq)]
pub struct BlockPointer {
  pub number: u64,
  pub hash: Bytes,
}
