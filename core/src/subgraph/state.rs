use graph::{
    components::store::EntityLfuCache, prelude::BlockPtr, util::backoff::ExponentialBackoff,
};
use std::time::Instant;

pub struct IndexingState {
    /// `true` -> `false` on the first run
    pub should_try_unfail_non_deterministic: bool,
    /// Whether we consider the subgraph to be synced. This flag starts out
    /// as `false` every time a subgraph is started, and therefore does not
    /// necessarily reflect what the same flag in the database says. That's
    /// because the flag in the database is only set to `true` once and then
    /// never reset. But if a subgraph is far behind the chain head, e.g.
    /// because it was stopped for a while, we want to make sure it can
    /// catch up as quickly as possible. Importantly, when this flag is
    /// `false`, the store will write changes in batches rather than
    /// block-by-block. That is turned off once the runner discovers that
    /// the subgraph is caught up via a call to `deployment_synced` in teh
    /// store.
    pub synced: bool,
    /// Backoff used for the retry mechanism on non-deterministic errors
    pub backoff: ExponentialBackoff,
    /// Related to field above `backoff`
    ///
    /// Resets to `Instant::now` every time:
    /// - The time THRESHOLD is passed
    /// - Or the subgraph has triggers for the block
    pub skip_ptr_updates_timer: Instant,
    pub entity_lfu_cache: EntityLfuCache,
    pub cached_head_ptr: Option<BlockPtr>,
}
