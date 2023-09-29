#[rustfmt::skip]
#[path = "pb/receipts.v1.rs"]
mod receipts;

pub mod near {
    pub use receipts::*;

    use super::receipts;
}
