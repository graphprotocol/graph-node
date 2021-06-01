use std::collections::HashMap;

use graph::prelude::anyhow;
use graph_store_postgres::command_support::{catalog as store_catalog, SqlName};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::Shard;
use graph_store_postgres::PRIMARY_SHARD;

fn parse_table_name(table: &str) -> Result<(&str, SqlName), anyhow::Error> {
    let mut parts = table.split('.');
    let nsp = parts
        .next()
        .ok_or_else(|| anyhow!("the table must be in the form 'sgdNNN.table'"))?;
    let table = parts
        .next()
        .ok_or_else(|| anyhow!("the table must be in the form 'sgdNNN.table'"))?;
    let table = SqlName::from(table);

    if !parts.next().is_none() {
        return Err(anyhow!("the table must be in the form 'sgdNNN.table'"));
    }
    Ok((nsp, table))
}

pub fn account_like(
    pools: HashMap<Shard, ConnectionPool>,
    clear: bool,
    table: String,
) -> Result<(), anyhow::Error> {
    let (nsp, table_name) = parse_table_name(&table)?;

    let conn = pools.get(&*PRIMARY_SHARD).unwrap().get()?;
    let conn = store_catalog::Connection::new(conn);

    let site = conn
        .find_site_by_name(nsp)?
        .ok_or_else(|| anyhow!("deployment `{}` does not exist", table_name))?;

    let conn = pools.get(&site.shard).unwrap().get()?;
    store_catalog::set_account_like(&conn, &site, &table_name, !clear)?;
    let clear_text = if clear { "cleared" } else { "set" };
    println!("{}: account-like flag {}", table, clear_text);

    Ok(())
}
