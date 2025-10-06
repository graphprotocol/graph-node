use diesel::select;
use diesel::sql_query;
use diesel::sql_types::{Bool, Integer};
use diesel::{insert_into, OptionalExtension};
use diesel::{
    sql_types::{Array, BigInt, Double, Nullable, Text},
    ExpressionMethods, QueryDsl,
};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use graph::components::store::VersionStats;
use graph::prelude::BlockNumber;
use graph::schema::EntityType;
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Duration;

use graph::prelude::anyhow::anyhow;
use graph::{
    data::subgraph::schema::POI_TABLE,
    prelude::{lazy_static, StoreError, BLOCK_NUMBER_MAX},
};

use crate::AsyncPgConnection;
use crate::{
    block_range::BLOCK_RANGE_COLUMN,
    pool::ForeignServer,
    primary::{Namespace, Site, NAMESPACE_PUBLIC},
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

// Readonly;  not all columns are mapped
table! {
    pg_namespace(oid) {
        oid -> Oid,
        #[sql_name = "nspname"]
        name -> Text,
    }
}
// Readonly; only mapping the columns we want
table! {
    pg_database(datname) {
        datname -> Text,
        datcollate -> Text,
        datctype -> Text,
    }
}

// Readonly; not all columns are mapped
table! {
    pg_class(oid) {
        oid -> Oid,
        #[sql_name = "relname"]
        name -> Text,
        #[sql_name = "relnamespace"]
        namespace -> Oid,
        #[sql_name = "relpages"]
        pages -> Integer,
        #[sql_name = "reltuples"]
        tuples -> Integer,
        #[sql_name = "relkind"]
        kind -> Char,
        #[sql_name = "relnatts"]
        natts -> Smallint,
    }
}

// Readonly; not all columns are mapped
table! {
    pg_attribute(oid) {
        #[sql_name = "attrelid"]
        oid -> Oid,
        #[sql_name = "attrelid"]
        relid -> Oid,
        #[sql_name = "attname"]
        name -> Text,
        #[sql_name = "attnum"]
        num -> Smallint,
        #[sql_name = "attstattarget"]
        stats_target -> Integer,
    }
}

joinable!(pg_class -> pg_namespace(namespace));
joinable!(pg_attribute -> pg_class(relid));
allow_tables_to_appear_in_same_query!(pg_class, pg_namespace, pg_attribute);

table! {
    subgraphs.table_stats {
        id -> Integer,
        deployment -> Integer,
        table_name -> Text,
        is_account_like -> Nullable<Bool>,
        last_pruned_block -> Nullable<Integer>,
    }
}

table! {
    __diesel_schema_migrations(version) {
        version -> Text,
        run_on -> Timestamp,
    }
}

lazy_static! {
    /// The name of the table in which Diesel records migrations
    static ref MIGRATIONS_TABLE: SqlName =
        SqlName::verbatim("__diesel_schema_migrations".to_string());
}

pub struct Locale {
    collate: String,
    ctype: String,
    encoding: String,
}

impl Locale {
    /// Load locale information for current database
    pub async fn load(conn: &mut AsyncPgConnection) -> Result<Locale, StoreError> {
        use diesel::dsl::sql;
        use pg_database as db;

        let (collate, ctype, encoding) = db::table
            .filter(db::datname.eq(sql("current_database()")))
            .select((
                db::datcollate,
                db::datctype,
                sql::<Text>("pg_encoding_to_char(encoding)::text"),
            ))
            .get_result::<(String, String, String)>(conn)
            .await?;
        Ok(Locale {
            collate,
            ctype,
            encoding,
        })
    }

    pub fn suitable(&self) -> Result<(), String> {
        if self.collate != "C" {
            return Err(format!(
                "database collation is `{}` but must be `C`",
                self.collate
            ));
        }
        if self.ctype != "C" {
            return Err(format!(
                "database ctype is `{}` but must be `C`",
                self.ctype
            ));
        }
        if self.encoding != "UTF8" {
            return Err(format!(
                "database encoding is `{}` but must be `UTF8`",
                self.encoding
            ));
        }
        Ok(())
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

    /// Set of tables which have an explicit causality region column.
    pub(crate) entities_with_causality_region: BTreeSet<EntityType>,

    /// Whether the database supports `int4_minmax_multi_ops` etc.
    /// See the [Postgres docs](https://www.postgresql.org/docs/15/brin-builtin-opclasses.html)
    has_minmax_multi_ops: bool,

    /// Whether the column `pg_stats.range_bounds_histogram` introduced in
    /// Postgres 17 exists. See the [Postgres
    /// docs](https://www.postgresql.org/docs/17/view-pg-stats.html)
    pg_stats_has_range_bounds_histogram: bool,
}

impl Catalog {
    /// Load the catalog for an existing subgraph
    pub async fn load(
        conn: &mut AsyncPgConnection,
        site: Arc<Site>,
        use_bytea_prefix: bool,
        entities_with_causality_region: Vec<EntityType>,
    ) -> Result<Self, StoreError> {
        let text_columns = get_text_columns(conn, &site.namespace).await?;
        let use_poi = supports_proof_of_indexing(conn, &site.namespace).await?;
        let has_minmax_multi_ops = has_minmax_multi_ops(conn).await?;
        let pg_stats_has_range_bounds_histogram = pg_stats_has_range_bounds_histogram(conn).await?;

        Ok(Catalog {
            site,
            text_columns,
            use_poi,
            use_bytea_prefix,
            entities_with_causality_region: entities_with_causality_region.into_iter().collect(),
            has_minmax_multi_ops,
            pg_stats_has_range_bounds_histogram,
        })
    }

    /// Return a new catalog suitable for creating a new subgraph
    pub async fn for_creation(
        conn: &mut AsyncPgConnection,
        site: Arc<Site>,
        entities_with_causality_region: BTreeSet<EntityType>,
    ) -> Result<Self, StoreError> {
        let has_minmax_multi_ops = has_minmax_multi_ops(conn).await?;
        let pg_stats_has_range_bounds_histogram = pg_stats_has_range_bounds_histogram(conn).await?;

        Ok(Catalog {
            site,
            text_columns: HashMap::default(),
            // DDL generation creates a POI table
            use_poi: true,
            // DDL generation creates indexes for prefixes of bytes columns
            // see: attr-bytea-prefix
            use_bytea_prefix: true,
            entities_with_causality_region,
            has_minmax_multi_ops,
            pg_stats_has_range_bounds_histogram,
        })
    }

    /// Make a catalog as if the given `schema` did not exist in the database
    /// yet. This function should only be used in situations where a database
    /// connection is definitely not available, such as in unit tests
    pub fn for_tests(
        site: Arc<Site>,
        entities_with_causality_region: BTreeSet<EntityType>,
    ) -> Result<Self, StoreError> {
        Ok(Catalog {
            site,
            text_columns: HashMap::default(),
            use_poi: false,
            use_bytea_prefix: true,
            entities_with_causality_region,
            has_minmax_multi_ops: false,
            pg_stats_has_range_bounds_histogram: false,
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

    /// The operator classes to use for BRIN indexes. The first entry if the
    /// operator class for `int4`, the second is for `int8`
    pub fn minmax_ops(&self) -> (&str, &str) {
        const MINMAX_OPS: (&str, &str) = ("int4_minmax_ops", "int8_minmax_ops");
        const MINMAX_MULTI_OPS: (&str, &str) = ("int4_minmax_multi_ops", "int8_minmax_multi_ops");

        if self.has_minmax_multi_ops {
            MINMAX_MULTI_OPS
        } else {
            MINMAX_OPS
        }
    }

    pub async fn stats(
        &self,
        conn: &mut AsyncPgConnection,
    ) -> Result<Vec<VersionStats>, StoreError> {
        #[derive(Queryable, QueryableByName)]
        pub struct DbStats {
            #[diesel(sql_type = BigInt)]
            pub entities: i64,
            #[diesel(sql_type = BigInt)]
            pub versions: i64,
            #[diesel(sql_type = Text)]
            pub tablename: String,
            /// The ratio `entities / versions`
            #[diesel(sql_type = Double)]
            pub ratio: f64,
            #[diesel(sql_type = Nullable<Integer>)]
            pub last_pruned_block: Option<i32>,
        }

        impl From<DbStats> for VersionStats {
            fn from(s: DbStats) -> Self {
                VersionStats {
                    entities: s.entities,
                    versions: s.versions,
                    tablename: s.tablename,
                    ratio: s.ratio,
                    last_pruned_block: s.last_pruned_block,
                    block_range_upper: vec![],
                }
            }
        }

        #[derive(Queryable, QueryableByName)]
        struct RangeHistogram {
            #[diesel(sql_type = Text)]
            tablename: String,
            #[diesel(sql_type = Array<Integer>)]
            upper: Vec<i32>,
        }

        async fn block_range_histogram(
            conn: &mut AsyncPgConnection,
            namespace: &Namespace,
        ) -> Result<Vec<RangeHistogram>, StoreError> {
            let query = format!(
                "select tablename, \
                array_agg(coalesce(upper(block_range), {BLOCK_NUMBER_MAX})) upper \
           from (select tablename,
                        unnest(range_bounds_histogram::text::int4range[]) block_range
                   from pg_stats where schemaname = $1 and attname = '{BLOCK_RANGE_COLUMN}') a
          group by tablename
          order by tablename"
            );
            let result = sql_query(query)
                .bind::<Text, _>(namespace.as_str())
                .get_results::<RangeHistogram>(conn)
                .await?;
            Ok(result)
        }

        // Get an estimate of number of rows (pg_class.reltuples) and number of
        // distinct entities (based on the planners idea of how many distinct
        // values there are in the `id` column) See the [Postgres
        // docs](https://www.postgresql.org/docs/current/view-pg-stats.html) for
        // the precise meaning of n_distinct
        let query = "select case when s.n_distinct < 0 then (- s.n_distinct * c.reltuples)::int8
                     else s.n_distinct::int8
                 end as entities,
                 c.reltuples::int8 as versions,
                 c.relname as tablename,
                case when c.reltuples = 0 then 0::float8
                     when s.n_distinct < 0 then (-s.n_distinct)::float8
                     else greatest(s.n_distinct, 1)::float8 / c.reltuples::float8
                 end as ratio,
                 ts.last_pruned_block
           from pg_namespace n, pg_class c, pg_stats s
                left outer join subgraphs.table_stats ts
                     on (ts.table_name = s.tablename
                     and ts.deployment = $1)
          where n.nspname = $2
            and c.relnamespace = n.oid
            and s.schemaname = n.nspname
            and s.attname = 'id'
            and c.relname = s.tablename
          order by c.relname"
            .to_string();

        let stats = sql_query(query)
            .bind::<Integer, _>(self.site.id)
            .bind::<Text, _>(self.site.namespace.as_str())
            .load::<DbStats>(conn)
            .await
            .map_err(StoreError::from)?;

        let mut range_histogram = if self.pg_stats_has_range_bounds_histogram {
            block_range_histogram(conn, &self.site.namespace).await?
        } else {
            vec![]
        };

        let stats = stats
            .into_iter()
            .map(|s| {
                let pos = range_histogram
                    .iter()
                    .position(|h| h.tablename == s.tablename);
                let mut upper = pos
                    .map(|pos| range_histogram.swap_remove(pos))
                    .map(|h| h.upper)
                    .unwrap_or(vec![]);
                // Since lower and upper are supposed to be histograms, we
                // sort them
                upper.sort_unstable();
                let mut vs = VersionStats::from(s);
                vs.block_range_upper = upper;
                vs
            })
            .collect::<Vec<_>>();

        Ok(stats)
    }
}

async fn get_text_columns(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
) -> Result<HashMap<String, HashSet<String>>, StoreError> {
    const QUERY: &str = "
        select table_name, column_name
          from information_schema.columns
         where table_schema = $1 and data_type = 'text'";

    #[derive(Debug, QueryableByName)]
    struct Column {
        #[diesel(sql_type = Text)]
        pub table_name: String,
        #[diesel(sql_type = Text)]
        pub column_name: String,
    }

    let map: HashMap<String, HashSet<String>> = diesel::sql_query(QUERY)
        .bind::<Text, _>(namespace.as_str())
        .load::<Column>(conn)
        .await?
        .into_iter()
        .fold(HashMap::new(), |mut map, col| {
            map.entry(col.table_name)
                .or_default()
                .insert(col.column_name);
            map
        });
    Ok(map)
}

pub async fn table_exists(
    conn: &mut AsyncPgConnection,
    namespace: &str,
    table: &SqlName,
) -> Result<bool, StoreError> {
    #[derive(Debug, QueryableByName)]
    struct Table {
        #[diesel(sql_type = Text)]
        #[allow(dead_code)]
        pub table_name: String,
    }
    let query =
        "SELECT table_name FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2";
    let result: Vec<Table> = diesel::sql_query(query)
        .bind::<Text, _>(namespace)
        .bind::<Text, _>(table.as_str())
        .load(conn)
        .await?;
    Ok(!result.is_empty())
}

pub async fn supports_proof_of_indexing(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
) -> Result<bool, StoreError> {
    lazy_static! {
        static ref POI_TABLE_NAME: SqlName = SqlName::verbatim(POI_TABLE.to_owned());
    }
    table_exists(conn, namespace.as_str(), &POI_TABLE_NAME).await
}

pub async fn current_servers(conn: &mut AsyncPgConnection) -> Result<Vec<String>, StoreError> {
    #[derive(QueryableByName)]
    struct Srv {
        #[diesel(sql_type = Text)]
        srvname: String,
    }
    Ok(sql_query("select srvname from pg_foreign_server")
        .get_results::<Srv>(conn)
        .await?
        .into_iter()
        .map(|srv| srv.srvname)
        .collect())
}

/// Return the options for the foreign server `name` as a map of option
/// names to values
pub async fn server_options(
    conn: &mut AsyncPgConnection,
    name: &str,
) -> Result<HashMap<String, Option<String>>, StoreError> {
    #[derive(QueryableByName)]
    struct Srv {
        #[diesel(sql_type = Array<Text>)]
        srvoptions: Vec<String>,
    }
    let entries = sql_query("select srvoptions from pg_foreign_server where srvname = $1")
        .bind::<Text, _>(name)
        .get_result::<Srv>(conn)
        .await?
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

pub async fn has_namespace(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
) -> Result<bool, StoreError> {
    use pg_namespace as nsp;

    Ok(select(diesel::dsl::exists(
        nsp::table.filter(nsp::name.eq(namespace.as_str())),
    ))
    .get_result::<bool>(conn)
    .await?)
}

/// Drop the schema for `src` if it is a foreign schema imported from
/// another database. If the schema does not exist, or is not a foreign
/// schema, do nothing. This crucially depends on the fact that we never mix
/// foreign and local tables in the same schema.
pub async fn drop_foreign_schema(
    conn: &mut AsyncPgConnection,
    src: &Site,
) -> Result<(), StoreError> {
    use foreign_tables as ft;

    let is_foreign = select(diesel::dsl::exists(
        ft::table.filter(ft::foreign_table_schema.eq(src.namespace.as_str())),
    ))
    .get_result::<bool>(conn)
    .await?;

    if is_foreign {
        let query = format!("drop schema if exists {} cascade", src.namespace);
        conn.batch_execute(&query).await?;
    }
    Ok(())
}

pub async fn foreign_tables(
    conn: &mut AsyncPgConnection,
    nsp: &str,
) -> Result<Vec<String>, StoreError> {
    use foreign_tables as ft;

    ft::table
        .filter(ft::foreign_table_schema.eq(nsp))
        .select(ft::foreign_table_name)
        .get_results::<String>(conn)
        .await
        .map_err(StoreError::from)
}

/// Drop the schema `nsp` and all its contents if it exists, and create it
/// again so that `nsp` is an empty schema
pub async fn recreate_schema(conn: &mut AsyncPgConnection, nsp: &str) -> Result<(), StoreError> {
    let query = format!(
        "drop schema if exists {nsp} cascade;\
         create schema {nsp};",
        nsp = nsp
    );
    Ok(conn.batch_execute(&query).await?)
}

/// Drop the schema `nsp` and all its contents if it exists
pub async fn drop_schema(conn: &mut AsyncPgConnection, nsp: &str) -> Result<(), StoreError> {
    let query = format!("drop schema if exists {nsp} cascade;", nsp = nsp);
    Ok(conn.batch_execute(&query).await?)
}

pub async fn migration_count(conn: &mut AsyncPgConnection) -> Result<usize, StoreError> {
    use __diesel_schema_migrations as m;

    if !table_exists(conn, NAMESPACE_PUBLIC, &MIGRATIONS_TABLE).await? {
        return Ok(0);
    }

    m::table
        .count()
        .get_result(conn)
        .await
        .map(|n: i64| n as usize)
        .map_err(StoreError::from)
}

pub async fn account_like(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<HashSet<String>, StoreError> {
    use table_stats as ts;
    let names = ts::table
        .filter(ts::deployment.eq(site.id))
        .select((ts::table_name, ts::is_account_like))
        .get_results::<(String, Option<bool>)>(conn)
        .await
        .optional()?
        .unwrap_or_default()
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

pub async fn set_account_like(
    conn: &mut AsyncPgConnection,
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
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn copy_account_like(
    conn: &mut AsyncPgConnection,
    src: &Site,
    dst: &Site,
) -> Result<usize, StoreError> {
    let src_nsp = ForeignServer::metadata_schema_in(&src.shard, &dst.shard);
    let query = format!(
        "insert into subgraphs.table_stats(deployment, table_name, is_account_like, last_pruned_block)
         select $2 as deployment, ts.table_name, ts.is_account_like, ts.last_pruned_block
           from {src_nsp}.table_stats ts
          where ts.deployment = $1",
        src_nsp = src_nsp
    );
    Ok(sql_query(query)
        .bind::<Integer, _>(src.id)
        .bind::<Integer, _>(dst.id)
        .execute(conn)
        .await?)
}

pub async fn set_last_pruned_block(
    conn: &mut AsyncPgConnection,
    site: &Site,
    table_name: &SqlName,
    last_pruned_block: BlockNumber,
) -> Result<(), StoreError> {
    use table_stats as ts;

    insert_into(ts::table)
        .values((
            ts::deployment.eq(site.id),
            ts::table_name.eq(table_name.as_str()),
            ts::last_pruned_block.eq(last_pruned_block),
        ))
        .on_conflict((ts::deployment, ts::table_name))
        .do_update()
        .set(ts::last_pruned_block.eq(last_pruned_block))
        .execute(conn)
        .await?;
    Ok(())
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
        #[diesel(sql_type = Text)]
        column_name: String,
        #[diesel(sql_type = Text)]
        data_type: String,
        #[diesel(sql_type = Text)]
        udt_name: String,
        #[diesel(sql_type = Text)]
        udt_schema: String,
        #[diesel(sql_type = Nullable<Text>)]
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

    pub async fn columns(
        conn: &mut AsyncPgConnection,
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
            .get_results::<ColumnInfo>(conn)
            .await?
            .into_iter()
            .map(|ci| ci.into())
            .collect())
    }
}

/// Return a SQL statement to create the foreign table
/// `{dst_nsp}.{table_name}` for the server `server` which has the same
/// schema as the (local) table `{src_nsp}.{table_name}`
pub async fn create_foreign_table(
    conn: &mut AsyncPgConnection,
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

    let columns = table_schema::columns(conn, src_nsp, table_name).await?;
    let query = build_query(columns, src_nsp, table_name, dst_nsp, server).map_err(|_| {
        anyhow!(
            "failed to generate 'create foreign table' query for {}.{}",
            dst_nsp,
            table_name
        )
    })?;
    Ok(query)
}

/// Create a SQL statement unioning imported tables from all shards,
/// something like
///
/// ```sql
/// create view "dst_nsp"."src_table" as
/// select 'shard1' as shard, "col1", "col2" from "shard_shard1_subgraphs"."table_name"
/// union all
/// ...
/// ````
///
/// The list `shard_nsps` consists of pairs `(name, namespace)` where `name`
/// is the name of the shard and `namespace` is the namespace where the
/// `src_table` is mapped
pub async fn create_cross_shard_view(
    conn: &mut AsyncPgConnection,
    src_nsp: &str,
    src_table: &str,
    dst_nsp: &str,
    shard_nsps: &[(&str, String)],
) -> Result<String, StoreError> {
    fn build_query(
        columns: &[table_schema::Column],
        table_name: &str,
        dst_nsp: &str,
        shard_nsps: &[(&str, String)],
    ) -> Result<String, std::fmt::Error> {
        let mut query = String::new();
        write!(query, "create view \"{}\".\"{}\" as ", dst_nsp, table_name)?;
        for (idx, (name, nsp)) in shard_nsps.into_iter().enumerate() {
            if idx > 0 {
                write!(query, " union all ")?;
            }
            write!(query, "select '{name}' as shard")?;
            for column in columns {
                write!(query, ", \"{}\"", column.column_name)?;
            }
            writeln!(query, " from \"{}\".\"{}\"", nsp, table_name)?;
        }
        Ok(query)
    }

    let columns = table_schema::columns(conn, src_nsp, src_table).await?;
    let query = build_query(&columns, src_table, dst_nsp, shard_nsps).map_err(|_| {
        anyhow!(
            "failed to generate 'create foreign table' query for {}.{}",
            dst_nsp,
            src_table
        )
    })?;
    Ok(query)
}

/// Checks in the database if a given index is valid.
pub(crate) async fn check_index_is_valid(
    conn: &mut AsyncPgConnection,
    schema_name: &str,
    index_name: &str,
) -> Result<bool, StoreError> {
    #[derive(Queryable, QueryableByName)]
    struct ManualIndexCheck {
        #[diesel(sql_type = Bool)]
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
        .await
        .optional()
        .map_err::<StoreError, _>(Into::into)?
        .map(|check| check.is_valid);
    Ok(matches!(result, Some(true)))
}

pub(crate) async fn indexes_for_table(
    conn: &mut AsyncPgConnection,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<String>, StoreError> {
    #[derive(Queryable, QueryableByName)]
    struct IndexName {
        #[diesel(sql_type = Text)]
        #[diesel(column_name = indexdef)]
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
        .await
        .map_err::<StoreError, _>(Into::into)?;

    Ok(results.into_iter().map(|i| i.def).collect())
}

pub(crate) async fn drop_index(
    conn: &mut AsyncPgConnection,
    schema_name: &str,
    index_name: &str,
) -> Result<(), StoreError> {
    let query = format!("drop index concurrently {schema_name}.{index_name}");
    sql_query(query)
        .bind::<Text, _>(schema_name)
        .bind::<Text, _>(index_name)
        .execute(conn)
        .await
        .map_err::<StoreError, _>(Into::into)?;
    Ok(())
}

/// Return by how much the slowest replica connected to the database `conn`
/// is lagging. The returned value has millisecond precision. If the
/// database has no replicas, return `0`
pub(crate) async fn replication_lag(conn: &mut AsyncPgConnection) -> Result<Duration, StoreError> {
    #[derive(Queryable, QueryableByName)]
    struct Lag {
        #[diesel(sql_type = Nullable<Integer>)]
        ms: Option<i32>,
    }

    let lag = sql_query(
        "select (extract(epoch from max(greatest(write_lag, flush_lag, replay_lag)))*1000)::int as ms \
           from pg_stat_replication",
    )
    .get_result::<Lag>(conn).await?;

    let lag = lag
        .ms
        .map(|ms| if ms <= 0 { 0 } else { ms as u64 })
        .unwrap_or(0);

    Ok(Duration::from_millis(lag))
}

pub(crate) async fn cancel_vacuum(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
) -> Result<(), StoreError> {
    sql_query(
        "select pg_cancel_backend(v.pid) \
           from pg_stat_progress_vacuum v, \
                pg_class c, \
                pg_namespace n \
          where v.relid = c.oid \
            and c.relnamespace = n.oid \
            and n.nspname = $1",
    )
    .bind::<Text, _>(namespace)
    .execute(conn)
    .await?;
    Ok(())
}

pub(crate) async fn default_stats_target(conn: &mut AsyncPgConnection) -> Result<i32, StoreError> {
    #[derive(Queryable, QueryableByName)]
    struct Target {
        #[diesel(sql_type = Integer)]
        setting: i32,
    }

    let target =
        sql_query("select setting::int from pg_settings where name = 'default_statistics_target'")
            .get_result::<Target>(conn)
            .await?;
    Ok(target.setting)
}

pub(crate) async fn stats_targets(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
) -> Result<BTreeMap<SqlName, BTreeMap<SqlName, i32>>, StoreError> {
    use pg_attribute as a;
    use pg_class as c;
    use pg_namespace as n;

    let targets = c::table
        .inner_join(n::table)
        .inner_join(a::table)
        .filter(c::kind.eq("r"))
        .filter(n::name.eq(namespace.as_str()))
        .filter(a::num.ge(1))
        .select((c::name, a::name, a::stats_target))
        .load::<(String, String, i32)>(conn)
        .await?
        .into_iter()
        .map(|(table, column, target)| (SqlName::from(table), SqlName::from(column), target));

    let map = targets.into_iter().fold(
        BTreeMap::<SqlName, BTreeMap<SqlName, i32>>::new(),
        |mut map, (table, column, target)| {
            map.entry(table).or_default().insert(column, target);
            map
        },
    );
    Ok(map)
}

pub(crate) async fn set_stats_target(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
    table: &SqlName,
    columns: &[&SqlName],
    target: i32,
) -> Result<(), StoreError> {
    let columns = columns
        .iter()
        .map(|column| format!("alter column {} set statistics {}", column.quoted(), target))
        .join(", ");
    let query = format!("alter table {}.{} {}", namespace, table.quoted(), columns);
    conn.batch_execute(&query).await?;
    Ok(())
}

/// Return the names of all tables in the `namespace` that need to be
/// analyzed. Whether a table needs to be analyzed is determined with the
/// same logic that Postgres' [autovacuum
/// daemon](https://www.postgresql.org/docs/current/routine-vacuuming.html#AUTOVACUUM)
/// uses
pub(crate) async fn needs_autoanalyze(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
) -> Result<Vec<SqlName>, StoreError> {
    const QUERY: &str = "select relname \
                           from pg_stat_user_tables \
                          where (select setting::numeric from pg_settings where name = 'autovacuum_analyze_threshold') \
                              + (select setting::numeric from pg_settings where name = 'autovacuum_analyze_scale_factor')*(n_live_tup + n_dead_tup) < n_mod_since_analyze
                            and schemaname = $1";

    #[derive(Queryable, QueryableByName)]
    struct TableName {
        #[diesel(sql_type = Text)]
        name: SqlName,
    }

    let tables = sql_query(QUERY)
        .bind::<Text, _>(namespace.as_str())
        .get_results::<TableName>(conn)
        .await
        .optional()?
        .map(|tables| tables.into_iter().map(|t| t.name).collect())
        .unwrap_or(vec![]);

    Ok(tables)
}

/// Check whether the database for `conn` supports the `minmax_multi_ops`
/// introduced in Postgres 14
async fn has_minmax_multi_ops(conn: &mut AsyncPgConnection) -> Result<bool, StoreError> {
    const QUERY: &str = "select count(*) = 2 as has_ops \
                           from pg_opclass \
                          where opcname in('int8_minmax_multi_ops', 'int4_minmax_multi_ops')";

    #[derive(Queryable, QueryableByName)]
    struct Ops {
        #[diesel(sql_type = Bool)]
        has_ops: bool,
    }

    Ok(sql_query(QUERY).get_result::<Ops>(conn).await?.has_ops)
}

/// Check whether the database for `conn` has the column
/// `pg_stats.range_bounds_histogram` introduced in Postgres 17
async fn pg_stats_has_range_bounds_histogram(
    conn: &mut AsyncPgConnection,
) -> Result<bool, StoreError> {
    #[derive(Queryable, QueryableByName)]
    struct HasIt {
        #[diesel(sql_type = Bool)]
        has_it: bool,
    }

    let query = "
          select exists (\
            select 1 \
              from information_schema.columns \
             where table_name = 'pg_stats' \
               and table_schema = 'pg_catalog' \
               and column_name = 'range_bounds_histogram') as has_it";
    sql_query(query)
        .get_result::<HasIt>(conn)
        .await
        .map(|h| h.has_it)
        .map_err(StoreError::from)
}

pub(crate) async fn histogram_bounds(
    conn: &mut AsyncPgConnection,
    namespace: &Namespace,
    table: &SqlName,
    column: &str,
) -> Result<Vec<i64>, StoreError> {
    const QUERY: &str = "select histogram_bounds::text::int8[] bounds \
                           from pg_stats \
                          where schemaname = $1 \
                            and tablename = $2 \
                            and attname = $3";

    #[derive(Queryable, QueryableByName)]
    struct Bounds {
        #[diesel(sql_type = Array<BigInt>)]
        bounds: Vec<i64>,
    }

    sql_query(QUERY)
        .bind::<Text, _>(namespace.as_str())
        .bind::<Text, _>(table.as_str())
        .bind::<Text, _>(column)
        .get_result::<Bounds>(conn)
        .await
        .optional()
        .map(|bounds| bounds.map(|b| b.bounds).unwrap_or_default())
        .map_err(StoreError::from)
}

/// Return the name of the sequence that Postgres uses to handle
/// auto-incrementing columns. This takes Postgres' way of dealing with long
/// table and sequence names into account.
pub(crate) fn seq_name(table_name: &str, column_name: &str) -> String {
    // Postgres limits all identifiers to 63 characters. When it
    // constructs the name of a sequence for a column in a table, it
    // truncates the table name so that appending '_{column}_seq' to
    // it is at most 63 characters
    let len = 63 - (5 + column_name.len());
    let len = len.min(table_name.len());
    format!("{}_{column_name}_seq", &table_name[0..len])
}

#[cfg(test)]
mod test {
    use super::seq_name;

    #[test]
    fn seq_name_works() {
        // Pairs of (table_name, vid_seq_name)
        const DATA: &[(&str, &str)] = &[
            ("token", "token_vid_seq"),
            (
                "frax_vst_curve_strategy_total_reward_token_collected_event",
                "frax_vst_curve_strategy_total_reward_token_collected_ev_vid_seq",
            ),
            (
                "rolling_asset_sent_for_last_24_hours_per_chain_and_token",
                "rolling_asset_sent_for_last_24_hours_per_chain_and_toke_vid_seq",
            ),
        ];

        for (tbl, exp) in DATA {
            let act = seq_name(tbl, "vid");
            assert_eq!(exp, &act);
        }
    }
}
