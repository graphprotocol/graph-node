use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::manager::deployment::DeploymentSearch;
use crate::manager::fmt;
use graph::components::store::DeploymentLocator;
use graph::components::store::VersionStats;
use graph::prelude::anyhow;
use graph::prelude::CheapClone as _;
use graph_store_postgres::command_support::catalog as store_catalog;
use graph_store_postgres::command_support::catalog::Site;
use graph_store_postgres::AsyncPgConnection;
use graph_store_postgres::ConnectionPool;
use graph_store_postgres::Shard;
use graph_store_postgres::SubgraphStore;
use graph_store_postgres::PRIMARY_SHARD;

async fn site_and_conn(
    pools: HashMap<Shard, ConnectionPool>,
    search: &DeploymentSearch,
) -> Result<(Arc<Site>, AsyncPgConnection), anyhow::Error> {
    let primary_pool = pools.get(&*PRIMARY_SHARD).unwrap();
    let locator = search.locate_unique(primary_pool).await?;

    let pconn = primary_pool.get_permitted().await?;
    let mut conn = store_catalog::Connection::new(pconn);

    let site = conn
        .locate_site(locator)
        .await?
        .ok_or_else(|| anyhow!("deployment `{}` does not exist", search))?;
    let site = Arc::new(site);

    let conn = pools.get(&site.shard).unwrap().get().await?;

    Ok((site, conn))
}

pub async fn account_like(
    store: Arc<SubgraphStore>,
    primary_pool: ConnectionPool,
    clear: bool,
    search: &DeploymentSearch,
    table: String,
) -> Result<(), anyhow::Error> {
    let locator = search.locate_unique(&primary_pool).await?;

    store.set_account_like(&locator, &table, !clear).await?;
    let clear_text = if clear { "cleared" } else { "set" };
    println!("{}: account-like flag {}", table, clear_text);

    Ok(())
}

pub fn show_stats(
    stats: &[VersionStats],
    account_like: HashSet<String>,
) -> Result<(), anyhow::Error> {
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

    fn print_stats(s: &VersionStats, account_like: bool) {
        println!(
            "{:<26} {:3} | {:>10} | {:>10} | {:>5.1}%",
            fmt::abbreviate(&s.tablename, 26),
            if account_like { "(a)" } else { "   " },
            s.entities,
            s.versions,
            s.ratio * 100.0
        );
    }

    header();
    for s in stats {
        print_stats(s, account_like.contains(&s.tablename));
    }
    if !account_like.is_empty() {
        footer();
    }

    Ok(())
}

pub async fn show(
    pools: HashMap<Shard, ConnectionPool>,
    search: &DeploymentSearch,
) -> Result<(), anyhow::Error> {
    let (site, mut conn) = site_and_conn(pools, search).await?;

    let catalog =
        store_catalog::Catalog::load(&mut conn, site.cheap_clone(), false, vec![]).await?;
    let stats = catalog.stats(&mut conn).await?;

    let account_like = store_catalog::account_like(&mut conn, &site).await?;

    show_stats(stats.as_slice(), account_like)
}

pub async fn analyze(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    search: DeploymentSearch,
    entity_name: Option<&str>,
) -> Result<(), anyhow::Error> {
    let locator = search.locate_unique(&pool).await?;
    analyze_loc(store, &locator, entity_name).await
}

async fn analyze_loc(
    store: Arc<SubgraphStore>,
    locator: &DeploymentLocator,
    entity_name: Option<&str>,
) -> Result<(), anyhow::Error> {
    match entity_name {
        Some(entity_name) => println!("Analyzing table sgd{}.{entity_name}", locator.id),
        None => println!("Analyzing all tables for sgd{}", locator.id),
    }
    store
        .analyze(locator, entity_name)
        .await
        .map_err(|e| anyhow!(e))
}

pub async fn target(
    store: Arc<SubgraphStore>,
    primary: ConnectionPool,
    search: &DeploymentSearch,
) -> Result<(), anyhow::Error> {
    let locator = search.locate_unique(&primary).await?;
    let (default, targets) = store.stats_targets(&locator).await?;

    let has_targets = targets
        .values()
        .any(|cols| cols.values().any(|target| *target > 0));

    if has_targets {
        println!(
            "{:^74}",
            format!(
                "Statistics targets for sgd{} (default: {default})",
                locator.id
            )
        );
        println!("{:^30} | {:^30} | {:^8}", "table", "column", "target");
        println!("{:-^30}-+-{:-^30}-+-{:-^8}", "", "", "");
        for (table, columns) in targets {
            for (column, target) in columns {
                if target > 0 {
                    println!("{:<30} | {:<30} | {:>8}", table, column, target);
                }
            }
        }
    } else {
        println!(
            "no statistics targets set for sgd{}, global default is {default}",
            locator.id
        );
    }
    Ok(())
}

pub async fn set_target(
    store: Arc<SubgraphStore>,
    primary: ConnectionPool,
    search: &DeploymentSearch,
    entity: Option<&str>,
    columns: Vec<String>,
    target: i32,
    no_analyze: bool,
) -> Result<(), anyhow::Error> {
    let columns = if columns.is_empty() {
        vec!["id".to_string(), "block_range".to_string()]
    } else {
        columns
    };

    let locator = search.locate_unique(&primary).await?;

    store
        .set_stats_target(&locator, entity, columns, target)
        .await?;

    if !no_analyze {
        analyze_loc(store, &locator, entity).await?;
    }
    Ok(())
}
