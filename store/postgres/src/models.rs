use diesel::sql_types::Jsonb;
use diesel::sql_types::VarChar;
use serde_json;

pub type EntityJSON = serde_json::Value;

#[derive(Queryable, QueryableByName, Debug)]
pub struct EntityTable {
    #[sql_type = "VarChar"]
    pub id: String,
    #[sql_type = "VarChar"]
    pub data_source: String,
    #[sql_type = "VarChar"]
    pub entity: String,
    #[sql_type = "Jsonb"]
    pub data: EntityJSON,
}
