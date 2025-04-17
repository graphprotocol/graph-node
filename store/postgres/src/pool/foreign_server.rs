use diesel::{connection::SimpleConnection, pg::PgConnection};

use graph::{
    prelude::{
        anyhow::{self, anyhow, bail},
        StoreError, ENV_VARS,
    },
    util::security::SafeDisplay,
};

use std::fmt::Write;

use postgres::config::{Config, Host};

use crate::catalog;
use crate::primary::NAMESPACE_PUBLIC;
use crate::{Shard, PRIMARY_SHARD};

use super::{PRIMARY_PUBLIC, PRIMARY_TABLES, SHARDED_TABLES};

pub struct ForeignServer {
    pub name: String,
    pub shard: Shard,
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub dbname: String,
}

impl ForeignServer {
    /// The name of the foreign server under which data for `shard` is
    /// accessible
    pub fn name(shard: &Shard) -> String {
        format!("shard_{}", shard.as_str())
    }

    /// The name of the schema under which the `subgraphs` schema for
    /// `shard` is accessible in shards that are not `shard`. In most cases
    /// you actually want to use `metadata_schema_in`
    pub fn metadata_schema(shard: &Shard) -> String {
        format!("{}_subgraphs", Self::name(shard))
    }

    /// The name of the schema under which the `subgraphs` schema for
    /// `shard` is accessible in the shard `current`. It is permissible for
    /// `shard` and `current` to be the same.
    pub fn metadata_schema_in(shard: &Shard, current: &Shard) -> String {
        if shard == current {
            "subgraphs".to_string()
        } else {
            Self::metadata_schema(&shard)
        }
    }

    pub fn new_from_raw(shard: String, postgres_url: &str) -> Result<Self, anyhow::Error> {
        Self::new(Shard::new(shard)?, postgres_url)
    }

    pub fn new(shard: Shard, postgres_url: &str) -> Result<Self, anyhow::Error> {
        let config: Config = match postgres_url.parse() {
            Ok(config) => config,
            Err(e) => panic!(
                "failed to parse Postgres connection string `{}`: {}",
                SafeDisplay(postgres_url),
                e
            ),
        };

        let host = match config.get_hosts().get(0) {
            Some(Host::Tcp(host)) => host.to_string(),
            _ => bail!("can not find host name in `{}`", SafeDisplay(postgres_url)),
        };

        let user = config
            .get_user()
            .ok_or_else(|| anyhow!("could not find user in `{}`", SafeDisplay(postgres_url)))?
            .to_string();
        let password = String::from_utf8(
            config
                .get_password()
                .ok_or_else(|| {
                    anyhow!(
                        "could not find password in `{}`; you must provide one.",
                        SafeDisplay(postgres_url)
                    )
                })?
                .into(),
        )?;
        let port = config.get_ports().first().cloned().unwrap_or(5432u16);
        let dbname = config
            .get_dbname()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("could not find user in `{}`", SafeDisplay(postgres_url)))?;

        Ok(Self {
            name: Self::name(&shard),
            shard,
            user,
            password,
            host,
            port,
            dbname,
        })
    }

    /// Create a new foreign server and user mapping on `conn` for this foreign
    /// server
    pub(super) fn create(&self, conn: &mut PgConnection) -> Result<(), StoreError> {
        let query = format!(
            "\
        create server \"{name}\"
               foreign data wrapper postgres_fdw
               options (host '{remote_host}', \
                        port '{remote_port}', \
                        dbname '{remote_db}', \
                        fetch_size '{fetch_size}', \
                        updatable 'false');
        create user mapping
               for current_user server \"{name}\"
               options (user '{remote_user}', password '{remote_password}');",
            name = self.name,
            remote_host = self.host,
            remote_port = self.port,
            remote_db = self.dbname,
            remote_user = self.user,
            remote_password = self.password,
            fetch_size = ENV_VARS.store.fdw_fetch_size,
        );
        Ok(conn.batch_execute(&query)?)
    }

    /// Update an existing user mapping with possibly new details
    pub(super) fn update(&self, conn: &mut PgConnection) -> Result<(), StoreError> {
        let options = catalog::server_options(conn, &self.name)?;
        let set_or_add = |option: &str| -> &'static str {
            if options.contains_key(option) {
                "set"
            } else {
                "add"
            }
        };

        let query = format!(
            "\
        alter server \"{name}\"
              options (set host '{remote_host}', \
                       {set_port} port '{remote_port}', \
                       set dbname '{remote_db}', \
                       {set_fetch_size} fetch_size '{fetch_size}');
        alter user mapping
              for current_user server \"{name}\"
              options (set user '{remote_user}', set password '{remote_password}');",
            name = self.name,
            remote_host = self.host,
            set_port = set_or_add("port"),
            set_fetch_size = set_or_add("fetch_size"),
            remote_port = self.port,
            remote_db = self.dbname,
            remote_user = self.user,
            remote_password = self.password,
            fetch_size = ENV_VARS.store.fdw_fetch_size,
        );
        Ok(conn.batch_execute(&query)?)
    }

    /// Map key tables from the primary into our local schema. If we are the
    /// primary, set them up as views.
    pub(super) fn map_primary(conn: &mut PgConnection, shard: &Shard) -> Result<(), StoreError> {
        catalog::recreate_schema(conn, PRIMARY_PUBLIC)?;

        let mut query = String::new();
        for table_name in PRIMARY_TABLES {
            let create_stmt = if shard == &*PRIMARY_SHARD {
                format!(
                    "create view {nsp}.{table_name} as select * from public.{table_name};",
                    nsp = PRIMARY_PUBLIC,
                    table_name = table_name
                )
            } else {
                catalog::create_foreign_table(
                    conn,
                    NAMESPACE_PUBLIC,
                    table_name,
                    PRIMARY_PUBLIC,
                    Self::name(&PRIMARY_SHARD).as_str(),
                )?
            };
            write!(query, "{}", create_stmt)?;
        }
        conn.batch_execute(&query)?;
        Ok(())
    }

    /// Map the `subgraphs` schema from the foreign server `self` into the
    /// database accessible through `conn`
    pub(super) fn map_metadata(&self, conn: &mut PgConnection) -> Result<(), StoreError> {
        let nsp = Self::metadata_schema(&self.shard);
        catalog::recreate_schema(conn, &nsp)?;
        let mut query = String::new();
        for (src_nsp, src_tables) in SHARDED_TABLES {
            for src_table in src_tables {
                let create_stmt =
                    catalog::create_foreign_table(conn, src_nsp, src_table, &nsp, &self.name)?;
                write!(query, "{}", create_stmt)?;
            }
        }
        Ok(conn.batch_execute(&query)?)
    }

    pub(super) fn needs_remap(&self, conn: &mut PgConnection) -> Result<bool, StoreError> {
        fn different(mut existing: Vec<String>, mut needed: Vec<String>) -> bool {
            existing.sort();
            needed.sort();
            existing != needed
        }

        if &self.shard == &*PRIMARY_SHARD {
            let existing = catalog::foreign_tables(conn, PRIMARY_PUBLIC)?;
            let needed = PRIMARY_TABLES
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>();
            if different(existing, needed) {
                return Ok(true);
            }
        }

        let existing = catalog::foreign_tables(conn, &Self::metadata_schema(&self.shard))?;
        let needed = SHARDED_TABLES
            .iter()
            .flat_map(|(_, tables)| *tables)
            .map(|table| table.to_string())
            .collect::<Vec<_>>();
        Ok(different(existing, needed))
    }
}
