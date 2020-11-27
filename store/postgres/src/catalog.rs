use diesel::pg::PgConnection;
use diesel::prelude::RunQueryDsl;
use diesel::sql_types::Text;
use std::collections::{HashMap, HashSet};

use graph::prelude::StoreError;

use crate::{primary::Namespace, relational::SqlName};

/// Information about what tables and columns we have in the database
#[derive(Debug, Clone)]
pub struct Catalog {
    pub schema: Namespace,
    text_columns: HashMap<String, HashSet<String>>,
}

impl Catalog {
    pub fn new(conn: &PgConnection, namespace: Namespace) -> Result<Self, StoreError> {
        let text_columns = get_text_columns(conn, &namespace)?;
        Ok(Catalog {
            schema: namespace,
            text_columns,
        })
    }

    /// Make a catalog as if the given `schema` did not exist in the database
    /// yet. This function should only be used in situations where a database
    /// connection is definitely not available, such as in unit tests
    pub fn make_empty(namespace: Namespace) -> Result<Self, StoreError> {
        Ok(Catalog {
            schema: namespace,
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
