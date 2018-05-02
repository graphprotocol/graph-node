use prelude::*;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::fmt::Debug;
use std::sync::Arc;
use tokio;
use common::query::{Query, QueryResult};

/// A mock [QueryRunner](../common/query/trait.QueryRunner.html).
pub struct MockQueryRunner<T, S> {
    query_sink: Sender<Query<T>>,
    store: Arc<S>,
}

impl<T, S> MockQueryRunner<T, S>
where
    T: Debug + Send + 'static,
    S: Store + Send + Sync + Sized + 'static,
{
    /// Creates a new mock [QueryRunner](../common/query/trait.QueryRunner.html).
    pub fn new(store: S) -> Self {
        let (sink, stream) = channel(100);
        let runner = MockQueryRunner {
            query_sink: sink,
            store: Arc::new(store),
        };
        runner.run_queries(stream);
        runner
    }

    /// Spawns a Tokio task to run any queries received through the given stream.
    fn run_queries(&self, stream: Receiver<Query<T>>) {
        let store = self.store.clone();

        tokio::spawn(stream.for_each(move |query| {
            println!("Running query: {:?}", query);

            // Here we would access the store.
            let _store = store.clone();

            query
                .result_sender
                .send(QueryResult {})
                .expect("Failed to send query result back");
            Ok(())
        }));
    }
}

impl<T, S> QueryRunner<T> for MockQueryRunner<T, S> {
    fn query_sink(&mut self) -> Sender<Query<T>> {
        self.query_sink.clone()
    }
}
