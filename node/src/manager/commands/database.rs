use std::time::Instant;

use graph::prelude::anyhow;
use graph_store_postgres::connection_pool::PoolCoordinator;

pub async fn remap(coord: &PoolCoordinator) -> Result<(), anyhow::Error> {
    let pools = coord.pools();
    let servers = coord.servers();

    for server in servers.iter() {
        for pool in pools.iter() {
            let start = Instant::now();
            print!(
                "Remapping imports from {} in shard {}",
                server.shard, pool.shard
            );
            pool.remap(server)?;
            println!(" (done in {}s)", start.elapsed().as_secs());
        }
    }
    Ok(())
}
