use std::collections::HashMap;
use std::sync::Arc;

use crate::manager::deployment::DeploymentSearch;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::PooledConnection;
use diesel::PgConnection;
use graph::prelude::anyhow;
use graph_store_postgres::command_support::catalog as store_catalog;
use graph_store_postgres::command_support::catalog::Site;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::Shard;
use graph_store_postgres::SubgraphStore;
use graph_store_postgres::PRIMARY_SHARD;

fn site_and_conn(
    pools: HashMap<Shard, ConnectionPool>,
    search: &DeploymentSearch,
) -> Result<(Site, PooledConnection<ConnectionManager<PgConnection>>), anyhow::Error> {
    let primary_pool = pools.get(&*PRIMARY_SHARD).unwrap();
    let locator = search.locate_unique(primary_pool)?;

    let conn = primary_pool.get()?;
    let conn = store_catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator)?
        .ok_or_else(|| anyhow!("deployment `{}` does not exist", search))?;

    let conn = pools.get(&site.shard).unwrap().get()?;

    Ok((site, conn))
}

pub async fn account_like(
    store: Arc<SubgraphStore>,
    primary_pool: ConnectionPool,
    clear: bool,
    search: &DeploymentSearch,
    table: String,
) -> Result<(), anyhow::Error> {
    let locator = search.locate_unique(&primary_pool)?;

    store.set_account_like(&locator, &table, !clear).await?;
    let clear_text = if clear { "cleared" } else { "set" };
    println!("{}: account-like flag {}", table, clear_text);

    Ok(())
}

pub fn show(
    pools: HashMap<Shard, ConnectionPool>,
    search: &DeploymentSearch,
) -> Result<(), anyhow::Error> {
    let (site, conn) = site_and_conn(pools, search)?;

    fn header() {
        println!(
            "{:^30} | {:^10} | {:^10} | {:^7}",
            "table", "entities", "versions", "ratio"
        );
        println!("{:-^30}-+-{:-^10}-+-{:-^10}-+-{:-^7}", "", "", "", "");
    }

    fn footer() {
        println!("  (a): account-like flag set");
    }

    fn print_stats(s: &store_catalog::VersionStats, account_like: bool) {
        println!(
            "{:<26} {:3} | {:>10} | {:>10} | {:>5.1}%",
            s.tablename,
            if account_like { "(a)" } else { "   " },
            s.entities,
            s.versions,
            s.ratio * 100.0
        );
    }

    let stats = store_catalog::stats(&conn, &site.namespace)?;

    let account_like = store_catalog::account_like(&conn, &site)?;

    header();
    for s in &stats {
        print_stats(s, account_like.contains(&s.tablename));
    }
    footer();

    Ok(())
}

pub fn analyze(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    search: DeploymentSearch,
    entity_name: &str,
) -> Result<(), anyhow::Error> {
    let locator = search.locate_unique(&pool)?;
    println!("Analyzing table sgd{}.{entity_name}", locator.id);
    store.analyze(&locator, entity_name).map_err(|e| anyhow!(e))
}
