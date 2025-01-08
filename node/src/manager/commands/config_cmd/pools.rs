use anyhow::Error;
use graphman::{commands::config::pools::pools, config::Config};

pub fn run(config: &Config, nodes: Vec<String>, shard: bool) -> Result<(), Error> {
    if nodes.is_empty() {
        return Err(Error::msg("No nodes specified"));
    }
    let res = pools(config, &nodes)?;
    if shard {
        for shard in res.shards {
            println!("{}: {}", shard.0, shard.1);
        }
    } else {
        for pool in res.pools {
            println!("{}:", pool.node);
            for shard in pool.shards {
                println!("  {}: {}", shard.0, shard.1);
            }
        }
    }
    Ok(())
}
