///! This module contains the gory details of using Diesel to query
///! a database schema that is not known at compile time. The code in this
///! module is mostly concerned with constructing SQL queries and some
///! helpers for serializing and deserializing entities.
///!
///! Code in this module works very hard to minimize the number of allocations
///! that it performs
use diesel::pg::{Pg, PgConnection};
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;
use diesel::sql_types::{Jsonb, Text};
use diesel::Connection;
use failure::Fail;
use std::convert::TryFrom;
use std::str::FromStr;

use graph::data::store::scalar;
use graph::prelude::{format_err, serde_json, Entity, StoreError, Value};

use crate::mapping::{ColumnType, Mapping, SqlName, Table};

/// Helper struct for retrieving entities from the database. With diesel, we
/// can only run queries that return columns whose number and type are known
/// at compile time. Because of that, we retrieve the actual data for an
/// entity as Jsonb by converting the row containing the entity using the
/// `to_jsonb` function.
#[derive(QueryableByName)]
pub struct EntityData {
    #[sql_type = "Text"]
    entity: String,
    #[sql_type = "Jsonb"]
    data: serde_json::Value,
}

impl EntityData {
    fn value_from_json(
        column_type: ColumnType,
        json: serde_json::Value,
    ) -> Result<graph::prelude::Value, StoreError> {
        use graph::prelude::Value as g;
        use serde_json::Value as j;
        // Many possible conversion errors are already caught by how
        // we define the schema; for example, we can only get a NULL for
        // a column that is actually nullable
        match json {
            j::Null => Ok(g::Null),
            j::Bool(b) => Ok(g::Bool(b)),
            j::Number(number) => match column_type {
                ColumnType::Int => match number.as_i64() {
                    Some(i) => i32::try_from(i).map(|i| g::Int(i)).map_err(|e| {
                        StoreError::Unknown(format_err!(
                            "failed to convert {} to Int: {}",
                            number,
                            e
                        ))
                    }),
                    None => Err(StoreError::Unknown(format_err!(
                        "failed to convert {} to Int",
                        number
                    ))),
                },
                ColumnType::BigDecimal => {
                    let s = number.to_string();
                    scalar::BigDecimal::from_str(s.as_str())
                        .map(|d| g::BigDecimal(d))
                        .map_err(|e| {
                            StoreError::Unknown(format_err!(
                                "failed to convert {} to BigDecimal: {}",
                                number,
                                e
                            ))
                        })
                }
                ColumnType::BigInt => {
                    let s = number.to_string();
                    scalar::BigInt::from_str(s.as_str())
                        .map(|d| g::BigInt(d))
                        .map_err(|e| {
                            StoreError::Unknown(format_err!(
                                "failed to convert {} to BigInt: {}",
                                number,
                                e
                            ))
                        })
                }
                column_type => Err(StoreError::Unknown(format_err!(
                    "can not convert number {} to {:?}",
                    number,
                    column_type
                ))),
            },
            j::String(s) => match column_type {
                ColumnType::String => Ok(g::String(s)),
                ColumnType::Bytes => scalar::Bytes::from_str(s.trim_start_matches("\\x"))
                    .map(|b| g::Bytes(b))
                    .map_err(|e| {
                        StoreError::Unknown(format_err!("failed to convert {} to Bytes: {}", s, e))
                    }),
                column_type => Err(StoreError::Unknown(format_err!(
                    "can not convert string {} to {:?}",
                    s,
                    column_type
                ))),
            },
            j::Array(values) => Ok(g::List(
                values
                    .into_iter()
                    .map(|v| Self::value_from_json(column_type, v))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            j::Object(_) => unimplemented!(),
        }
    }

    /// Map the `EntityData` to an entity using the schema information
    /// in `Mapping`
    pub fn to_entity(self, mapping: &Mapping) -> Result<Entity, StoreError> {
        let table = mapping.table_for_entity(&self.entity)?;

        use serde_json::Value as j;
        match self.data {
            j::Object(map) => {
                let mut entity = Entity::new();
                entity.insert(
                    "__typename".to_owned(),
                    graph::prelude::Value::from(self.entity),
                );
                for (key, json) in map {
                    let column = table.field(&SqlName::from(&*key))?;
                    let value = Self::value_from_json(column.column_type, json)?;
                    if value != Value::Null {
                        entity.insert(column.field.clone(), value);
                    }
                }
                Ok(entity)
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Constructor)]
pub struct FindQuery<'a> {
    mapping: &'a Mapping,
    entity: &'a str,
    id: &'a str,
}

impl<'a> FindQuery<'a> {
    fn object_query(&self, table: &'a Table, mut out: AstPass<Pg>) -> QueryResult<()> {
        // Generate
        //    select '..' as entity, to_jsonb(e.*) as data from schema.table e where id = $1
        out.push_sql("select ");
        out.push_bind_param::<Text, _>(&self.entity)?;
        out.push_sql(" as entity, to_jsonb(e.*) as data\n");
        out.push_sql("  from ");
        out.push_identifier(&self.mapping.schema)?;
        out.push_sql(".");
        out.push_identifier(table.name.as_str())?;
        out.push_sql(" e\n where ");
        out.push_identifier(crate::mapping::PRIMARY_KEY_COLUMN)?;
        out.push_sql(" = ");
        out.push_bind_param::<Text, _>(&self.id)
    }
}

impl<'a> QueryFragment<Pg> for FindQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        if let Some(table) = self.mapping.table_for_entity(self.entity).ok() {
            self.object_query(table, out)
        } else if let Some(tables) = self.mapping.interfaces.get(self.entity) {
            for (i, table) in tables.iter().enumerate() {
                if i > 0 {
                    out.push_sql("\nunion all\n");
                }
                self.object_query(table, out.reborrow())?;
            }
            Ok(())
        } else {
            Err(diesel::result::Error::QueryBuilderError(Box::new(
                StoreError::UnknownTable(self.entity.to_owned()).compat(),
            )))
        }
    }
}

impl<'a> QueryId for FindQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, EntityData> for FindQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<EntityData>> {
        conn.query_by_name(&self)
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for FindQuery<'a> {}
