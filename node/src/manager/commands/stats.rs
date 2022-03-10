use std::collections::HashMap;
use std::sync::Arc;

use crate::manager::deployment::find_single_deployment_locator;
use diesel::r2d2::ConnectionManager;
use diesel::r2d2::PooledConnection;
use diesel::sql_query;
use diesel::sql_types::{Integer, Text};
use diesel::PgConnection;
use diesel::RunQueryDsl;
use graph::prelude::anyhow;
use graph::prelude::anyhow::bail;
use graph_store_postgres::command_support::catalog::Site;
use graph_store_postgres::command_support::{catalog as store_catalog, SqlName};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::Shard;
use graph_store_postgres::SubgraphStore;
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

fn site_and_conn(
    pools: HashMap<Shard, ConnectionPool>,
    nsp: &str,
) -> Result<(Site, PooledConnection<ConnectionManager<PgConnection>>), anyhow::Error> {
    let conn = pools.get(&*PRIMARY_SHARD).unwrap().get()?;
    let conn = store_catalog::Connection::new(conn);

    let site = conn
        .find_site_by_name(nsp)?
        .ok_or_else(|| anyhow!("deployment `{}` does not exist", nsp))?;

    let conn = pools.get(&site.shard).unwrap().get()?;

    Ok((site, conn))
}

pub fn account_like(
    pools: HashMap<Shard, ConnectionPool>,
    clear: bool,
    table: String,
) -> Result<(), anyhow::Error> {
    let (nsp, table_name) = parse_table_name(&table)?;
    let (site, conn) = site_and_conn(pools, nsp)?;

    store_catalog::set_account_like(&conn, &site, &table_name, !clear)?;
    let clear_text = if clear { "cleared" } else { "set" };
    println!("{}: account-like flag {}", table, clear_text);

    Ok(())
}

pub fn show(
    pools: HashMap<Shard, ConnectionPool>,
    nsp: String,
    table: Option<String>,
) -> Result<(), anyhow::Error> {
    let (site, conn) = site_and_conn(pools, &nsp)?;

    #[derive(Queryable, QueryableByName)]
    struct VersionStats {
        #[sql_type = "Integer"]
        entities: i32,
        #[sql_type = "Integer"]
        versions: i32,
        #[sql_type = "Text"]
        tablename: String,
    }

    impl VersionStats {
        fn header() {
            println!(
                "{:^30} | {:^10} | {:^10} | {:^7}",
                "table", "entities", "versions", "ratio"
            );
            println!("{:-^30}-+-{:-^10}-+-{:-^10}-+-{:-^7}", "", "", "", "");
        }

        fn print(&self, account_like: bool) {
            println!(
                "{:<26} {:3} | {:>10} | {:>10} | {:>5.1}%",
                self.tablename,
                if account_like { "(a)" } else { "   " },
                self.entities,
                self.versions,
                self.entities as f32 * 100.0 / self.versions as f32
            );
        }

        fn footer() {
            println!("  (a): account-like flag set");
        }
    }

    let query = format!(
        "select s.n_distinct::int4 as entities,
                c.reltuples::int4  as versions,
                c.relname as tablename
           from pg_namespace n, pg_class c, pg_stats s
          where n.nspname = $1
            and c.relnamespace = n.oid
            and s.schemaname = n.nspname
            and s.attname = 'id'
            and c.relname = s.tablename
          order by c.relname"
    );
    let stats = sql_query(query)
        .bind::<Text, _>(&site.namespace.as_str())
        .load::<VersionStats>(&conn)?;

    let account_like = store_catalog::account_like(&conn, &site)?;

    VersionStats::header();
    for stat in &stats {
        stat.print(account_like.contains(&stat.tablename));
    }
    VersionStats::footer();

    if let Some(table) = table {
        if !stats.iter().any(|stat| stat.tablename == table) {
            bail!(
                "deployment {} does not have a table `{}`",
                site.namespace,
                table
            );
        }

        println!("doing a full count on {}.{} ...", site.namespace, table);
        let query = format!(
            "select count(distinct id)::int4 as entities,
                    count(*)::int4 as versions,
                    '{table}' as tablename
               from {nsp}.{table}",
            nsp = &site.namespace,
            table = table
        );
        let stat = sql_query(query).get_result::<VersionStats>(&conn)?;
        stat.print(account_like.contains(&stat.tablename));
    }

    Ok(())
}

pub async fn analyze(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    deployment_id: String,
    entity_name: &str,
) -> Result<(), anyhow::Error> {
    println!("Running ANALYZE for {entity_name} entity");
    let deployment_locator = find_single_deployment_locator(&pool, &deployment_id)?;
    store
        .analyze(&deployment_locator, entity_name)
        .await
        .map_err(|e| anyhow!(e))
}
