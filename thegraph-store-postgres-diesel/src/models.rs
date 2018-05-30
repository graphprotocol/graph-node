use diesel::sql_types::Int4;
use diesel::sql_types::Jsonb;
use diesel::sql_types::VarChar;
use serde_json;

pub type EntityJson = serde_json::Value;

#[derive(Queryable, QueryableByName, Debug)]
pub struct EntityTable {
    #[sql_type = "Int4"]
    pub id: i32,
    #[sql_type = "VarChar"]
    pub data_source: String,
    #[sql_type = "VarChar"]
    pub entity: String,
    #[sql_type = "Jsonb"]
    pub data: EntityJson
}
