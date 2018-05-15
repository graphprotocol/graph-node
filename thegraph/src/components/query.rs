use futures::sync::mpsc::Sender;

use data::query::Query;

/// Common trait for query runners that run queries against a [Store](../store/trait.Store.html).
pub trait QueryRunner {
    // Sender to which others can write queries that need to be run.
    fn query_sink(&mut self) -> Sender<Query>;
}
