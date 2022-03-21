use diesel::sql_types::{Bool, Integer};
use diesel::{connection::SimpleConnection, prelude::RunQueryDsl, select};
use diesel::{insert_into, OptionalExtension};
use diesel::{pg::PgConnection, sql_query};
use diesel::{
    sql_types::{Array, Nullable, Text},
    ExpressionMethods, QueryDsl,
};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::iter::FromIterator;
use std::sync::Arc;

use graph::prelude::anyhow::anyhow;
use graph::{data::subgraph::schema::POI_TABLE, prelude::StoreError};

use crate::connection_pool::ForeignServer;
use crate::{
    primary::{Namespace, Site},
    relational::SqlName,
};

// This is a view not a table. We only read from it
table! {
    information_schema.foreign_tables(foreign_table_schema, foreign_table_name) {
        foreign_table_catalog -> Text,
        foreign_table_schema -> Text,
        foreign_table_name -> Text,
        foreign_server_catalog -> Text,
        foreign_server_name -> Text,
    }
}

// Readonly; we only access the name
table! {
    pg_namespace(nspname) {
        nspname -> Text,
    }
}

table! {
    subgraphs.table_stats {
        id -> Integer,
        deployment -> Integer,
        table_name -> Text,
        is_account_like -> Nullable<Bool>,
    }
}

/// Information about what tables and columns we have in the database
#[derive(Debug, Clone)]
pub struct Catalog {
    pub site: Arc<Site>,
    text_columns: HashMap<String, HashSet<String>>,
    pub use_poi: bool,
    /// Whether `bytea` columns are indexed with just a prefix (`true`) or
    /// in their entirety. This influences both DDL generation and how
    /// queries are generated
    pub use_bytea_prefix: bool,
}

impl Catalog {
    /// Load the catalog for an existing subgraph
    pub fn load(
        conn: &PgConnection,
        site: Arc<Site>,
        use_bytea_prefix: bool,
    ) -> Result<Self, StoreError> {
        let text_columns = get_text_columns(conn, &site.namespace)?;
        let use_poi = supports_proof_of_indexing(conn, &site.namespace)?;
        Ok(Catalog {
            site,
            text_columns,
            use_poi,
            use_bytea_prefix,
        })
    }

    /// Return a new catalog suitable for creating a new subgraph
    pub fn for_creation(site: Arc<Site>) -> Self {
        Catalog {
            site,
            text_columns: HashMap::default(),
            // DDL generation creates a POI table
            use_poi: true,
            // DDL generation creates indexes for prefixes of bytes columns
            use_bytea_prefix: true,
        }
    }

    /// Make a catalog as if the given `schema` did not exist in the database
    /// yet. This function should only be used in situations where a database
    /// connection is definitely not available, such as in unit tests
    pub fn for_tests(site: Arc<Site>) -> Result<Self, StoreError> {
        Ok(Catalog {
            site,
            text_columns: HashMap::default(),
            use_poi: false,
            use_bytea_prefix: true,
        })
    }

    /// Return `true` if `table` exists and contains the given `column` and
    /// if that column is of data type `text`
    pub fn is_existing_text_column(&self, table: &SqlName, column: &SqlName) -> bool {
        self.text_columns
            .get(table.as_str())
            .map(|cols| cols.contains(column.as_str()))
            .unwrap_or(false)
    }
}

fn get_text_columns(
    conn: &PgConnection,
    namespace: &Namespace,
) -> Result<HashMap<String, HashSet<String>>, StoreError> {
    const QUERY: &str = "
        select table_name, column_name
          from information_schema.columns
         where table_schema = $1 and data_type = 'text'";

    #[derive(Debug, QueryableByName)]
    struct Column {
        #[sql_type = "Text"]
        pub table_name: String,
        #[sql_type = "Text"]
        pub column_name: String,
    }

    let map: HashMap<String, HashSet<String>> = diesel::sql_query(QUERY)
        .bind::<Text, _>(namespace.as_str())
        .load::<Column>(conn)?
        .into_iter()
        .fold(HashMap::new(), |mut map, col| {
            map.entry(col.table_name)
                .or_default()
                .insert(col.column_name);
            map
        });
    Ok(map)
}

pub fn supports_proof_of_indexing(
    conn: &diesel::pg::PgConnection,
    namespace: &Namespace,
) -> Result<bool, StoreError> {
    #[derive(Debug, QueryableByName)]
    struct Table {
        #[sql_type = "Text"]
        #[allow(dead_code)]
        pub table_name: String,
    }
    let query =
        "SELECT table_name FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2";
    let result: Vec<Table> = diesel::sql_query(query)
        .bind::<Text, _>(namespace.as_str())
        .bind::<Text, _>(POI_TABLE)
        .load(conn)?;
    Ok(result.len() > 0)
}

pub fn current_servers(conn: &PgConnection) -> Result<Vec<String>, StoreError> {
    #[derive(QueryableByName)]
    struct Srv {
        #[sql_type = "Text"]
        srvname: String,
    }
    Ok(sql_query("select srvname from pg_foreign_server")
        .get_results::<Srv>(conn)?
        .into_iter()
        .map(|srv| srv.srvname)
        .collect())
}

/// Return the options for the foreign server `name` as a map of option
/// names to values
pub fn server_options(
    conn: &PgConnection,
    name: &str,
) -> Result<HashMap<String, Option<String>>, StoreError> {
    #[derive(QueryableByName)]
    struct Srv {
        #[sql_type = "Array<Text>"]
        srvoptions: Vec<String>,
    }
    let entries = sql_query("select srvoptions from pg_foreign_server where srvname = $1")
        .bind::<Text, _>(name)
        .get_result::<Srv>(conn)?
        .srvoptions
        .into_iter()
        .filter_map(|opt| {
            let mut parts = opt.splitn(2, '=');
            let key = parts.next();
            let value = parts.next().map(|value| value.to_string());

            key.map(|key| (key.to_string(), value))
        });
    Ok(HashMap::from_iter(entries))
}

pub fn has_namespace(conn: &PgConnection, namespace: &Namespace) -> Result<bool, StoreError> {
    use pg_namespace as nsp;

    Ok(select(diesel::dsl::exists(
        nsp::table.filter(nsp::nspname.eq(namespace.as_str())),
    ))
    .get_result::<bool>(conn)?)
}

/// Drop the schema for `src` if it is a foreign schema imported from
/// another database. If the schema does not exist, or is not a foreign
/// schema, do nothing. This crucially depends on the fact that we never mix
/// foreign and local tables in the same schema.
pub fn drop_foreign_schema(conn: &PgConnection, src: &Site) -> Result<(), StoreError> {
    use foreign_tables as ft;

    let is_foreign = select(diesel::dsl::exists(
        ft::table.filter(ft::foreign_table_schema.eq(src.namespace.as_str())),
    ))
    .get_result::<bool>(conn)?;

    if is_foreign {
        let query = format!("drop schema if exists {} cascade", src.namespace);
        conn.batch_execute(&query)?;
    }
    Ok(())
}

/// Drop the schema `nsp` and all its contents if it exists, and create it
/// again so that `nsp` is an empty schema
pub fn recreate_schema(conn: &PgConnection, nsp: &str) -> Result<(), StoreError> {
    let query = format!(
        "drop schema if exists {nsp} cascade;\
         create schema {nsp};",
        nsp = nsp
    );
    Ok(conn.batch_execute(&query)?)
}

pub fn account_like(conn: &PgConnection, site: &Site) -> Result<HashSet<String>, StoreError> {
    use table_stats as ts;
    let names = ts::table
        .filter(ts::deployment.eq(site.id))
        .select((ts::table_name, ts::is_account_like))
        .get_results::<(String, Option<bool>)>(conn)
        .optional()?
        .unwrap_or(vec![])
        .into_iter()
        .filter_map(|(name, account_like)| {
            if account_like == Some(true) {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    Ok(names)
}

pub fn set_account_like(
    conn: &PgConnection,
    site: &Site,
    table_name: &SqlName,
    is_account_like: bool,
) -> Result<(), StoreError> {
    use table_stats as ts;
    insert_into(ts::table)
        .values((
            ts::deployment.eq(site.id),
            ts::table_name.eq(table_name.as_str()),
            ts::is_account_like.eq(is_account_like),
        ))
        .on_conflict((ts::deployment, ts::table_name))
        .do_update()
        .set(ts::is_account_like.eq(is_account_like))
        .execute(conn)?;
    Ok(())
}

pub fn copy_account_like(conn: &PgConnection, src: &Site, dst: &Site) -> Result<usize, StoreError> {
    let src_nsp = if src.shard == dst.shard {
        "subgraphs".to_string()
    } else {
        ForeignServer::metadata_schema(&src.shard)
    };
    let query = format!(
        "insert into subgraphs.table_stats(deployment, table_name, is_account_like)
         select $2 as deployment, ts.table_name, ts.is_account_like
           from {src_nsp}.table_stats ts
          where ts.deployment = $1",
        src_nsp = src_nsp
    );
    Ok(sql_query(&query)
        .bind::<Integer, _>(src.id)
        .bind::<Integer, _>(dst.id)
        .execute(conn)?)
}

pub(crate) mod table_schema {
    use super::*;

    /// The name and data type for the column in a table. The data type is
    /// in a form that it can be used in a `create table` statement
    pub struct Column {
        pub column_name: String,
        pub data_type: String,
    }

    #[derive(QueryableByName)]
    struct ColumnInfo {
        #[sql_type = "Text"]
        column_name: String,
        #[sql_type = "Text"]
        data_type: String,
        #[sql_type = "Text"]
        udt_name: String,
        #[sql_type = "Text"]
        udt_schema: String,
        #[sql_type = "Nullable<Text>"]
        elem_type: Option<String>,
    }

    impl From<ColumnInfo> for Column {
        fn from(ci: ColumnInfo) -> Self {
            // See description of `data_type` in
            // https://www.postgresql.org/docs/current/infoschema-columns.html
            let data_type = match ci.data_type.as_str() {
                "ARRAY" => format!(
                    "{}[]",
                    ci.elem_type.expect("array columns have an elem_type")
                ),
                "USER-DEFINED" => format!("{}.{}", ci.udt_schema, ci.udt_name),
                _ => ci.data_type.clone(),
            };
            Self {
                column_name: ci.column_name,
                data_type,
            }
        }
    }

    pub fn columns(
        conn: &PgConnection,
        nsp: &str,
        table_name: &str,
    ) -> Result<Vec<Column>, StoreError> {
        const QUERY: &str = " \
    select c.column_name::text, c.data_type::text,
           c.udt_name::text, c.udt_schema::text, e.data_type::text as elem_type
      from information_schema.columns c
      left join information_schema.element_types e
           on ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
            = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
     where c.table_schema = $1
       and c.table_name = $2
     order by c.ordinal_position";

        Ok(sql_query(QUERY)
            .bind::<Text, _>(nsp)
            .bind::<Text, _>(table_name)
            .get_results::<ColumnInfo>(conn)?
            .into_iter()
            .map(|ci| ci.into())
            .collect())
    }
}

/// Return a SQL statement to create the foreign table
/// `{dst_nsp}.{table_name}` for the server `server` which has the same
/// schema as the (local) table `{src_nsp}.{table_name}`
pub fn create_foreign_table(
    conn: &PgConnection,
    src_nsp: &str,
    table_name: &str,
    dst_nsp: &str,
    server: &str,
) -> Result<String, StoreError> {
    fn build_query(
        columns: Vec<table_schema::Column>,
        src_nsp: &str,
        table_name: &str,
        dst_nsp: &str,
        server: &str,
    ) -> Result<String, std::fmt::Error> {
        let mut query = String::new();
        write!(
            query,
            "create foreign table \"{}\".\"{}\" (",
            dst_nsp, table_name
        )?;
        for (idx, column) in columns.into_iter().enumerate() {
            if idx > 0 {
                write!(query, ", ")?;
            }
            write!(query, "\"{}\" {}", column.column_name, column.data_type)?;
        }
        writeln!(
            query,
            ") server \"{}\" options(schema_name '{}');",
            server, src_nsp
        )?;
        Ok(query)
    }

    let columns = table_schema::columns(conn, src_nsp, table_name)?;
    let query = build_query(columns, src_nsp, table_name, dst_nsp, server).map_err(|_| {
        anyhow!(
            "failed to generate 'create foreign table' query for {}.{}",
            dst_nsp,
            table_name
        )
    })?;
    Ok(query)
}

/// Checks in the database if a given index is valid.
pub(crate) fn check_index_is_valid(
    conn: &PgConnection,
    schema_name: &str,
    index_name: &str,
) -> Result<bool, StoreError> {
    #[derive(Queryable, QueryableByName)]
    struct ManualIndexCheck {
        #[sql_type = "Bool"]
        is_valid: bool,
    }

    let query = "
        select
            i.indisvalid as is_valid
        from
            pg_class c
            join pg_index i on i.indexrelid = c.oid
            join pg_namespace n on c.relnamespace = n.oid
        where
            n.nspname = $1
            and c.relname = $2";
    let result = sql_query(query)
        .bind::<Text, _>(schema_name)
        .bind::<Text, _>(index_name)
        .get_result::<ManualIndexCheck>(conn)
        .optional()
        .map_err::<StoreError, _>(Into::into)?
        .map(|check| check.is_valid);
    Ok(matches!(result, Some(true)))
}

pub(crate) fn indexes_for_table(
    conn: &PgConnection,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<String>, StoreError> {
    #[derive(Queryable, QueryableByName)]
    struct IndexName {
        #[sql_type = "Text"]
        #[column_name = "indexdef"]
        def: String,
    }

    let query = "
        select
            indexdef
        from
            pg_indexes
        where
            schemaname = $1
            and tablename = $2
        order by indexname";
    let results = sql_query(query)
        .bind::<Text, _>(schema_name)
        .bind::<Text, _>(table_name)
        .load::<IndexName>(conn)
        .map_err::<StoreError, _>(Into::into)?;

    Ok(results.into_iter().map(|i| i.def).collect())
}
pub(crate) fn drop_index(
    conn: &PgConnection,
    schema_name: &str,
    index_name: &str,
) -> Result<(), StoreError> {
    let query = format!("drop index concurrently {schema_name}.{index_name}");
    sql_query(&query)
        .bind::<Text, _>(schema_name)
        .bind::<Text, _>(index_name)
        .execute(conn)
        .map_err::<StoreError, _>(Into::into)?;
    Ok(())
}
