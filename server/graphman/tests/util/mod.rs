pub mod client;
pub mod server;

use std::future::Future;
use std::sync::Mutex;

use lazy_static::lazy_static;
use test_store::store::remove_subgraphs;
use test_store::store::PRIMARY_POOL;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

lazy_static! {
    // Used to make sure tests will run sequentially.
    static ref SEQ_MUX: Mutex<()> = Mutex::new(());

    // One runtime helps share the same server between the tests.
    static ref RUNTIME: Runtime = Builder::new_current_thread().enable_all().build().unwrap();
}

pub fn run_test<T, F>(test: T)
where
    T: FnOnce() -> F,
    F: Future<Output = ()>,
{
    let _lock = SEQ_MUX.lock().unwrap_or_else(|err| err.into_inner());

    cleanup_graphman_command_executions_table();
    remove_subgraphs();

    RUNTIME.block_on(async {
        server::start().await;

        test().await;
    });
}

fn cleanup_graphman_command_executions_table() {
    use diesel::prelude::*;

    let mut conn = PRIMARY_POOL.get().unwrap();

    diesel::sql_query("truncate table public.graphman_command_executions;")
        .execute(&mut conn)
        .expect("truncate is successful");
}
