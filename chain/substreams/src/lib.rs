mod block_stream;
mod chain;
mod data_source;
mod trigger;

pub mod block_ingestor;
pub mod mapper;

pub use block_stream::BlockStreamBuilder;
pub use chain::*;
pub use data_source::*;
use graph::{components::store::EntityKey, prelude::Entity};
pub use graph_chain_common::substreams::{EntityChanges, Field};
pub use trigger::*;

// ParsedChanges are an internal representation of the equivalent operations defined on the
// graph-out format used by substreams.
// Unset serves as a sentinel value, if for some reason an unknown value is sent or the value
// was empty then it's probably an unintended behaviour. This code was moved here for performance
// reasons, but the validation is still performed during trigger processing so while Unset will
// very likely just indicate an error somewhere, as far as the stream is concerned we just pass
// that along and let the downstream components deal with it.
#[derive(Debug, Clone)]
pub enum ParsedChanges {
    Unset,
    Delete(EntityKey),
    Upsert { key: EntityKey, entity: Entity },
}
