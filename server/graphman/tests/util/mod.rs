pub mod client;
pub mod server;

use std::future::Future;
use std::sync::Mutex;

use graph::TEST_RUNTIME;
use lazy_static::lazy_static;
use test_store::store::remove_subgraphs;
use test_store::store::PRIMARY_POOL;

lazy_static! {
    // Used to make sure tests will run sequentially.
    static ref SEQ_MUX: Mutex<()> = Mutex::new(());
}

pub fn run_test<T, F>(test: T)
where
    T: FnOnce() -> F,
    F: Future<Output = ()>,
{
    let _lock = SEQ_MUX.lock().unwrap_or_else(|err| err.into_inner());

    TEST_RUNTIME.block_on(async {
        cleanup_graphman_command_executions_table().await;
        remove_subgraphs().await;

        server::start().await;

        test().await;
    });
}

async fn cleanup_graphman_command_executions_table() {
    use diesel_async::RunQueryDsl;

    let mut conn = PRIMARY_POOL.get().await.unwrap();

    diesel::sql_query("truncate table public.graphman_command_executions;")
        .execute(&mut conn)
        .await
        .expect("truncate is successful");
}
