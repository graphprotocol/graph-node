use diesel::{connection::SimpleConnection, prelude::RunQueryDsl, select};
use diesel::{pg::PgConnection, sql_query};
use diesel::{sql_types::Text, ExpressionMethods, QueryDsl};
use std::collections::{HashMap, HashSet};

use graph::{data::subgraph::schema::POI_TABLE, prelude::StoreError};

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

/// Information about what tables and columns we have in the database
#[derive(Debug, Clone)]
pub struct Catalog {
    pub namespace: Namespace,
    text_columns: HashMap<String, HashSet<String>>,
}

impl Catalog {
    pub fn new(conn: &PgConnection, namespace: Namespace) -> Result<Self, StoreError> {
        let text_columns = get_text_columns(conn, &namespace)?;
        Ok(Catalog {
            namespace,
            text_columns,
        })
    }

    /// Make a catalog as if the given `schema` did not exist in the database
    /// yet. This function should only be used in situations where a database
    /// connection is definitely not available, such as in unit tests
    pub fn make_empty(namespace: Namespace) -> Result<Self, StoreError> {
        Ok(Catalog {
            namespace,
            text_columns: HashMap::default(),
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
