use diesel::connection::SimpleConnection;
use diesel::{debug_query, OptionalExtension, PgConnection, RunQueryDsl};
use graphql_parser::query as q;
use graphql_parser::schema as s;
use inflector::Inflector;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Write};
use std::rc::Rc;
use std::str::FromStr;

use crate::relational_queries::{
    ClampRangeQuery, ConflictingEntityQuery, EntityData, FilterQuery, FindQuery, InsertQuery,
    RevertClampQuery, RevertRemoveQuery,
};
use graph::prelude::{
    format_err, Entity, EntityChange, EntityChangeOperation, EntityFilter, EntityKey,
    QueryExecutionError, StoreError, StoreEvent, SubgraphDeploymentId, ValueType,
};

use crate::block_range::BlockNumber;
use crate::entities::STRING_PREFIX_SIZE;

/// A string we use as a SQL name for a table or column. The important thing
/// is that SQL names are snake cased. Using this type makes it easier to
/// spot cases where we use a GraphQL name like 'bigThing' when we should
/// really use the SQL version 'big_thing'
///
/// We use `SqlName` for example for table and column names, and feed these
/// directly to Postgres. Postgres truncates names to 63 characters; if
/// users have GraphQL type names that do not differ in the first
/// 63 characters after snakecasing, schema creation will fail because, to
/// Postgres, we would create the same table twice. We consider this case
/// to be pathological and so unlikely in practice that we do not try to work
/// around it in the application.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct SqlName(String);

impl SqlName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    // Check that `name` matches the regular expression `/[A-Za-z][A-Za-z0-9_]*/`
    // without pulling in a regex matcher
    fn check_valid_identifier(name: &str, kind: &str) -> Result<(), StoreError> {
        let mut chars = name.chars();
        match chars.next() {
            Some(c) => {
                if !c.is_ascii_alphabetic() {
                    let msg = format!(
                        "the name `{}` can not be used for a {}; \
                         it must start with an ASCII alphabetic character",
                        name, kind
                    );
                    return Err(StoreError::InvalidIdentifier(msg));
                }
            }
            None => {
                let msg = format!("can not use an empty name for a {}", kind);
                return Err(StoreError::InvalidIdentifier(msg));
            }
        }
        for c in chars {
            if !c.is_ascii_alphanumeric() && c != '_' {
                let msg = format!(
                    "the name `{}` can not be used for a {}; \
                     it can only contain alphanumeric characters and `_`",
                    name, kind
                );
                return Err(StoreError::InvalidIdentifier(msg));
            }
        }
        Ok(())
    }
}

impl From<&str> for SqlName {
    fn from(name: &str) -> Self {
        SqlName(name.to_snake_case())
    }
}

impl From<String> for SqlName {
    fn from(name: String) -> Self {
        SqlName(name.to_snake_case())
    }
}

impl fmt::Display for SqlName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The SQL type to use for GraphQL ID properties. We support
/// strings and byte arrays
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IdType {
    String,
    #[allow(dead_code)]
    Bytes,
}

#[derive(Debug, Clone)]
pub struct Mapping {
    /// The SQL type for columns with GraphQL type `ID`
    id_type: IdType,
    /// Maps the GraphQL name of a type to the relational table
    pub tables: HashMap<String, Rc<Table>>,
    /// The subgraph id
    pub subgraph: SubgraphDeploymentId,
    /// The database schema for this subgraph
    pub schema: String,
    /// Map the entity names of interfaces to the list of
    /// database tables that contain entities implementing
    /// that interface
    pub interfaces: HashMap<String, Vec<Rc<Table>>>,
    /// The query to count all entities
    pub count_query: String,
}

impl Mapping {
    /// Generate a mapping for a relational schema for entities in the
    /// GraphQL schema `document`. Attributes of type `ID` will use the
    /// SQL type `id_type`. The subgraph ID is passed in `subgraph`, and
    /// the name of the database schema in which the subgraph's tables live
    /// is in `schema`.
    pub fn new<V>(
        document: &s::Document,
        id_type: IdType,
        subgraph: SubgraphDeploymentId,
        schema: V,
    ) -> Result<Mapping, StoreError>
    where
        V: Into<String>,
    {
        use s::Definition::*;
        use s::TypeDefinition::*;

        let schema = schema.into();
        SqlName::check_valid_identifier(&schema, "database schema")?;

        // Check that we can handle all the definitions
        for defn in &document.definitions {
            match defn {
                TypeDefinition(Object(_)) | TypeDefinition(Interface(_)) => (),
                other => {
                    return Err(StoreError::Unknown(format_err!(
                        "can not handle {:?}",
                        other
                    )))
                }
            }
        }

        // Extract interfaces and tables
        let mut interfaces: HashMap<String, Vec<SqlName>> = HashMap::new();
        let mut tables = Vec::new();

        for defn in &document.definitions {
            match defn {
                TypeDefinition(Object(obj_type)) => {
                    let table =
                        Table::new(obj_type, &mut interfaces, id_type, tables.len() as u32)?;
                    tables.push(table);
                }
                TypeDefinition(Interface(interface_type)) => {
                    SqlName::check_valid_identifier(&interface_type.name, "interface")?;
                    interfaces.insert(interface_type.name.clone(), vec![]);
                }
                other => {
                    return Err(StoreError::Unknown(format_err!(
                        "can not handle {:?}",
                        other
                    )))
                }
            }
        }

        let tables: Vec<_> = tables.into_iter().map(|table| Rc::new(table)).collect();
        let interfaces = interfaces
            .into_iter()
            .map(|(k, v)| {
                // The unwrap here is ok because tables only contains entries
                // for which we know that a table exists
                let v: Vec<_> = v
                    .iter()
                    .map(|name| {
                        tables
                            .iter()
                            .find(|table| &table.name == name)
                            .unwrap()
                            .clone()
                    })
                    .collect();
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        let count_query = tables
            .iter()
            .map(|table| {
                format!(
                    "select count(*) from \"{}\".\"{}\" where upper_inf(block_range)",
                    schema, table.name
                )
            })
            .collect::<Vec<_>>()
            .join("\nunion all\n");
        let count_query = format!("select sum(e.count) from ({}) e", count_query);

        let tables: HashMap<_, _> = tables
            .into_iter()
            .fold(HashMap::new(), |mut tables, table| {
                tables.insert(table.object.clone(), table);
                tables
            });

        Ok(Mapping {
            id_type,
            subgraph,
            schema,
            tables,
            interfaces,
            count_query,
        })
    }

    pub fn create_relational_schema(
        conn: &PgConnection,
        schema_name: &str,
        subgraph: SubgraphDeploymentId,
        document: &s::Document,
    ) -> Result<Mapping, StoreError> {
        let mapping =
            crate::relational::Mapping::new(document, IdType::String, subgraph, schema_name)?;
        let sql = mapping
            .as_ddl()
            .map_err(|_| StoreError::Unknown(format_err!("failed to generate DDL for mapping")))?;
        conn.batch_execute(&sql)?;
        Ok(mapping)
    }

    /// Generate the DDL for the entire mapping, i.e., all `create table`
    /// and `create index` etc. statements needed in the database schema
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    pub fn as_ddl(&self) -> Result<String, fmt::Error> {
        let mut out = String::new();

        // We sort tables here solely because the unit tests rely on
        // 'create table' statements appearing in a fixed order
        let mut tables = self.tables.values().collect::<Vec<_>>();
        tables.sort_by_key(|table| table.position);
        // Output 'create table' statements for all tables
        for table in tables {
            table.as_ddl(&mut out, self)?;
        }
        Ok(out)
    }

    /// Find the table with the provided `name`. The name must exactly match
    /// the name of an existing table. No conversions of the name are done
    pub fn table(&self, name: &SqlName) -> Result<&Table, StoreError> {
        self.tables
            .values()
            .find(|table| &table.name == name)
            .map(|rc| rc.as_ref())
            .ok_or_else(|| StoreError::UnknownTable(name.to_string()))
    }

    pub fn table_for_entity(&self, entity: &str) -> Result<&Rc<Table>, StoreError> {
        self.tables
            .get(entity)
            .ok_or_else(|| StoreError::UnknownTable(entity.to_owned()))
    }

    pub fn find(
        &self,
        conn: &PgConnection,
        entity: &str,
        id: &str,
        block: BlockNumber,
    ) -> Result<Option<Entity>, StoreError> {
        FindQuery::new(self, entity, id, block)
            .get_result::<EntityData>(conn)
            .optional()?
            .map(|entity_data| entity_data.to_entity(self))
            .transpose()
    }

    pub fn insert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: &Entity,
        block: BlockNumber,
    ) -> Result<(), StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        let query = InsertQuery::new(&self.schema, table, key, entity, block)?;
        query.execute(conn)?;
        Ok(())
    }

    pub fn conflicting_entity(
        &self,
        conn: &PgConnection,
        entity_id: &String,
        entities: Vec<&String>,
    ) -> Result<Option<String>, StoreError> {
        Ok(ConflictingEntityQuery::new(self, &entities, entity_id)
            .load(conn)?
            .pop()
            .map(|data| data.entity))
    }

    /// order is a tuple (attribute, cast, direction)
    pub fn query(
        &self,
        conn: &PgConnection,
        entity_types: Vec<String>,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, &str, &str)>,
        first: Option<u32>,
        skip: u32,
        block: BlockNumber,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let tables = entity_types
            .into_iter()
            .map(|entity| self.table_for_entity(&entity).map(|rc| rc.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let first = first.map(|first| first.to_string());
        let skip = if skip == 0 {
            None
        } else {
            Some(skip.to_string())
        };

        let query = FilterQuery::new(&self.schema, tables, filter, order, first, skip, block);
        let query_debug_info = query.clone();

        let values = query.load::<EntityData>(conn).map_err(|e| {
            QueryExecutionError::ResolveEntitiesError(format!(
                "{}, query = {:?}",
                e,
                debug_query(&query_debug_info).to_string()
            ))
        })?;

        values
            .into_iter()
            .map(|entity_data| entity_data.to_entity(self).map_err(|e| e.into()))
            .collect()
    }

    pub fn update(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: &Entity,
        block: BlockNumber,
    ) -> Result<(), StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        ClampRangeQuery::new(&self.schema, table, key, block).execute(conn)?;
        let query = InsertQuery::new(&self.schema, table, key, entity, block)?;
        query.execute(conn)?;
        Ok(())
    }

    pub fn delete(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        block: BlockNumber,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        Ok(ClampRangeQuery::new(&self.schema, table, key, block).execute(conn)?)
    }

    pub fn revert_block(
        &self,
        conn: &PgConnection,
        block: BlockNumber,
    ) -> Result<(StoreEvent, i32), StoreError> {
        let mut changes: Vec<EntityChange> = Vec::new();
        let mut count: i32 = 0;

        for table in self.tables.values() {
            // Remove all versions whose entire block range lies beyond
            // `block`
            let removed = RevertRemoveQuery::new(&self.schema, table, block)
                .get_results(conn)?
                .into_iter()
                .map(|data| data.id)
                .collect::<HashSet<_>>();
            // Make the versions current that existed at `block - 1` but that
            // are not current yet. Those are the ones that were updated or
            // deleted at `block`
            let unclamped = RevertClampQuery::new(&self.schema, table, block - 1)
                .get_results(conn)?
                .into_iter()
                .map(|data| data.id)
                .collect::<HashSet<_>>();
            // Adjust the entity count; we can tell which operation was
            // initially performed by
            //   id in (unset - unclamped)  => insert (we now deleted)
            //   id in (unset && unclamped) => update (we reversed the update)
            //   id in (unclamped - unset)  => delete (we now inserted)
            let deleted = removed.difference(&unclamped).count() as i32;
            let inserted = unclamped.difference(&removed).count() as i32;
            count += inserted - deleted;
            // EntityChange for versions we just deleted
            let deleted = removed
                .into_iter()
                .filter(|id| !unclamped.contains(id))
                .map(|id| EntityChange {
                    subgraph_id: self.subgraph.clone(),
                    entity_type: table.object.clone(),
                    entity_id: id,
                    operation: EntityChangeOperation::Removed,
                });
            changes.extend(deleted);
            // EntityChange for versions that we just updated or inserted
            let set = unclamped.into_iter().map(|id| EntityChange {
                subgraph_id: self.subgraph.clone(),
                entity_type: table.object.clone(),
                entity_id: id,
                operation: EntityChangeOperation::Set,
            });
            changes.extend(set);
        }
        Ok((StoreEvent::new(changes), count))
    }
}

/// This is almost the same as graph::data::store::ValueType, but without
/// ID and List; with this type, we only care about scalar types that directly
/// correspond to Postgres scalar types
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ColumnType {
    Boolean,
    BigDecimal,
    BigInt,
    Bytes,
    Int,
    String,
}

impl From<IdType> for ColumnType {
    fn from(id_type: IdType) -> Self {
        match id_type {
            IdType::Bytes => ColumnType::Bytes,
            IdType::String => ColumnType::String,
        }
    }
}

impl ColumnType {
    fn from_value_type(value_type: ValueType, id_type: IdType) -> Result<ColumnType, StoreError> {
        match value_type {
            ValueType::Boolean => Ok(ColumnType::Boolean),
            ValueType::BigDecimal => Ok(ColumnType::BigDecimal),
            ValueType::BigInt => Ok(ColumnType::BigInt),
            ValueType::Bytes => Ok(ColumnType::Bytes),
            ValueType::Int => Ok(ColumnType::Int),
            ValueType::String => Ok(ColumnType::String),
            ValueType::ID => Ok(ColumnType::from(id_type)),
            ValueType::List => Err(StoreError::Unknown(format_err!(
                "can not convert ValueType::List to ColumnType"
            ))),
        }
    }

    fn sql_type(&self) -> &str {
        match self {
            ColumnType::Boolean => "boolean",
            ColumnType::BigDecimal => "numeric",
            ColumnType::BigInt => "numeric",
            ColumnType::Bytes => "bytea",
            ColumnType::Int => "integer",
            ColumnType::String => "text",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: SqlName,
    pub field: String,
    pub field_type: q::Type,
    pub column_type: ColumnType,
}

impl Column {
    fn new(field: &s::Field, id_type: IdType) -> Result<Column, StoreError> {
        SqlName::check_valid_identifier(&*field.name, "attribute")?;

        let sql_name = SqlName::from(&*field.name);
        Ok(Column {
            name: sql_name,
            field: field.name.clone(),
            column_type: ColumnType::from_value_type(base_type(&field.field_type), id_type)?,
            field_type: field.field_type.clone(),
        })
    }

    pub fn is_primary_key(&self) -> bool {
        PRIMARY_KEY_COLUMN == self.name.as_str()
    }

    fn sql_type(&self) -> &str {
        self.column_type.sql_type()
    }

    pub fn is_nullable(&self) -> bool {
        fn is_nullable(field_type: &q::Type) -> bool {
            match field_type {
                q::Type::NonNullType(_) => false,
                _ => true,
            }
        }
        is_nullable(&self.field_type)
    }

    pub fn is_list(&self) -> bool {
        fn is_list(field_type: &q::Type) -> bool {
            use q::Type::*;

            match field_type {
                ListType(_) => true,
                NonNullType(inner) => is_list(inner),
                NamedType(_) => false,
            }
        }
        is_list(&self.field_type)
    }

    /// Generate the DDL for one column, i.e. the part of a `create table`
    /// statement for this column.
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    fn as_ddl(&self, out: &mut String) -> fmt::Result {
        write!(out, "    ")?;
        write!(out, "{:20} {}", self.name, self.sql_type())?;
        if self.is_list() {
            write!(out, "[]")?;
        }
        if self.name.0 == PRIMARY_KEY_COLUMN || !self.is_nullable() {
            write!(out, " not null")?;
        }
        Ok(())
    }
}

/// The name for the primary key column of a table; hardcoded for now
pub(crate) const PRIMARY_KEY_COLUMN: &str = "id";

pub(crate) const BLOCK_RANGE: &str = "block_range";

#[derive(Clone, Debug)]
pub struct Table {
    /// The name of the GraphQL object type ('Thing')
    pub object: s::Name,
    /// The name of the database table for this type ('thing'), snakecased
    /// version of `object`
    pub name: SqlName,

    pub columns: Vec<Column>,
    /// The position of this table in all the tables for this mapping; this
    /// is really only needed for the tests to make the names of indexes
    /// predictable
    position: u32,
}

impl Table {
    fn new(
        defn: &s::ObjectType,
        interfaces: &mut HashMap<String, Vec<SqlName>>,
        id_type: IdType,
        position: u32,
    ) -> Result<Table, StoreError> {
        SqlName::check_valid_identifier(&*defn.name, "object")?;

        let table_name = SqlName::from(&*defn.name);
        let columns = defn
            .fields
            .iter()
            .filter(|field| !derived_column(field))
            .map(|field| Column::new(field, id_type))
            .collect::<Result<Vec<_>, _>>()?;
        let table = Table {
            object: defn.name.clone(),
            name: table_name.clone(),
            columns,
            position,
        };
        for interface_name in &defn.implements_interfaces {
            match interfaces.get_mut(interface_name) {
                Some(tables) => tables.push(table.name.clone()),
                None => {
                    return Err(StoreError::Unknown(format_err!(
                        "unknown interface {}",
                        interface_name
                    )))
                }
            }
        }
        Ok(table)
    }
    /// Find the column `name` in this table. The name must be in snake case,
    /// i.e., use SQL conventions
    pub fn column(&self, name: &SqlName) -> Result<&Column, StoreError> {
        self.columns
            .iter()
            .find(|column| &column.name == name)
            .ok_or_else(|| StoreError::UnknownField(name.to_string()))
    }

    /// Find the column for `field` in this table. The name must be the
    /// GraphQL name of an entity field
    pub fn column_for_field(&self, field: &str) -> Result<&Column, StoreError> {
        self.columns
            .iter()
            .find(|column| &column.field == field)
            .ok_or_else(|| StoreError::UnknownField(field.to_string()))
    }

    /// Generate the DDL for one table, i.e. one `create table` statement
    /// and all `create index` statements for the table's columns
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    fn as_ddl(&self, out: &mut String, mapping: &Mapping) -> fmt::Result {
        write!(out, "create table {}.{} (\n", mapping.schema, self.name)?;
        for column in self.columns.iter() {
            write!(out, "    ")?;
            column.as_ddl(out)?;
            write!(out, ",\n")?;
        }
        // Add block_range column and constraint
        write!(
            out,
            "\n        {block_range}          int4range not null,
        exclude using gist   (id with =, {block_range} with &&)\n);\n",
            block_range = BLOCK_RANGE
        )?;

        for (i, column) in self.columns.iter().enumerate() {
            // Attributes that are plain strings are indexed with a BTree; but
            // they can be too large for Postgres' limit on values that can go
            // into a BTree. For those attributes, only index the first
            // STRING_PREFIX_SIZE characters
            let index_expr =
                if !column.is_list() && base_type(&column.field_type) == ValueType::String {
                    format!("left({}, {})", column.name, STRING_PREFIX_SIZE)
                } else {
                    column.name.to_string()
                };

            let method = if column.is_list() { "gin" } else { "btree" };
            write!(
                out,
                "create index attr_{table_index}_{column_index}_{table_name}_{column_name}\n    on {schema_name}.{table_name} using {method}({index_expr});\n",
                table_index = self.position,
                table_name = self.name,
                column_index = i,
                column_name = column.name,
                schema_name = mapping.schema,
                method = method,
                index_expr = index_expr,
            )?;
        }
        write!(out, "\n")
    }
}

/// Return the base type underlying the given field type, i.e., the type
/// after stripping List and NonNull. For types that are not the builtin
/// GraphQL scalar types, and therefore references to other GraphQL objects,
/// use `ValueType::ID`
fn base_type(field_type: &q::Type) -> ValueType {
    let name = named_type(field_type);
    ValueType::from_str(name).unwrap_or(ValueType::ID)
}

/// Return the enclosed named type for a field type, i.e., the type after
/// stripping List and NonNull.
fn named_type(field_type: &q::Type) -> &str {
    match field_type {
        q::Type::NamedType(name) => name.as_str(),
        q::Type::ListType(child) => named_type(child),
        q::Type::NonNullType(child) => named_type(child),
    }
}

fn derived_column(field: &s::Field) -> bool {
    field
        .directives
        .iter()
        .any(|dir| dir.name == s::Name::from("derivedFrom"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphql_parser::parse_schema;

    const ID_TYPE: ColumnType = ColumnType::String;

    fn test_mapping(gql: &str) -> Mapping {
        let schema = parse_schema(gql).expect("Test schema invalid");
        let subgraph = SubgraphDeploymentId::new("subgraph").unwrap();
        Mapping::new(&schema, IdType::String, subgraph, "rel").expect("Failed to construct Mapping")
    }

    #[test]
    fn table_is_sane() {
        let mapping = test_mapping(THING_GQL);
        let table = mapping
            .table(&"thing".into())
            .expect("failed to get 'thing' table");
        assert_eq!(SqlName::from("thing"), table.name);
        assert_eq!("Thing", table.object);

        let id = table
            .column(&PRIMARY_KEY_COLUMN.into())
            .expect("failed to get 'id' column for 'thing' table");
        assert_eq!(ID_TYPE, id.column_type);
        assert!(!id.is_nullable());
        assert!(!id.is_list());

        let big_thing = table
            .column(&"big_thing".into())
            .expect("failed to get 'big_thing' column for 'thing' table");
        assert_eq!(ID_TYPE, big_thing.column_type);
        assert!(!big_thing.is_nullable());
        // Field lookup happens by the SQL name, not the GraphQL name
        let bad_sql_name = SqlName("bigThing".to_owned());
        assert!(table.column(&bad_sql_name).is_err());
    }

    #[test]
    fn generate_ddl() {
        let mapping = test_mapping(THING_GQL);
        let sql = mapping.as_ddl().expect("Failed to generate DDL");
        assert_eq!(THING_DDL, sql);

        let mapping = test_mapping(MUSIC_GQL);
        let sql = mapping.as_ddl().expect("Failed to generate DDL");
        assert_eq!(MUSIC_DDL, sql);

        let mapping = test_mapping(FOREST_GQL);
        let sql = mapping.as_ddl().expect("Failed to generate DDL");
        assert_eq!(FOREST_DDL, sql);
    }

    const THING_GQL: &str = "
        type Thing @entity {
            id: ID!
            bigThing: Thing!
        }

        type Scalar {
            id: ID,
            bool: Boolean,
            int: Int,
            bigDecimal: BigDecimal,
            string: String,
            bytes: Bytes,
            bigInt: BigInt,
        }";

    const THING_DDL: &str = "create table rel.thing (
        id                   text not null,
        big_thing            text not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_0_0_thing_id
    on rel.thing using btree(id);
create index attr_0_1_thing_big_thing
    on rel.thing using btree(big_thing);

create table rel.scalar (
        id                   text not null,
        bool                 boolean,
        int                  integer,
        big_decimal          numeric,
        string               text,
        bytes                bytea,
        big_int              numeric,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_0_scalar_id
    on rel.scalar using btree(id);
create index attr_1_1_scalar_bool
    on rel.scalar using btree(bool);
create index attr_1_2_scalar_int
    on rel.scalar using btree(int);
create index attr_1_3_scalar_big_decimal
    on rel.scalar using btree(big_decimal);
create index attr_1_4_scalar_string
    on rel.scalar using btree(left(string, 2048));
create index attr_1_5_scalar_bytes
    on rel.scalar using btree(bytes);
create index attr_1_6_scalar_big_int
    on rel.scalar using btree(big_int);

";

    const MUSIC_GQL: &str = "type Musician @entity {
    id: ID!
    name: String!
    mainBand: Band
    bands: [Band!]!
    writtenSongs: [Song]! @derivedFrom(field: \"writtenBy\")
}

type Band @entity {
    id: ID!
    name: String!
    members: [Musician!]! @derivedFrom(field: \"bands\")
    originalSongs: [Song!]!
}

type Song @entity {
    id: ID!
    title: String!
    writtenBy: Musician!
    band: Band @derivedFrom(field: \"originalSongs\")
}

type SongStat @entity {
    id: ID!
    song: Song @derivedFrom(field: \"id\")
    played: Int!
}";
    const MUSIC_DDL: &str = "create table rel.musician (
        id                   text not null,
        name                 text not null,
        main_band            text,
        bands                text[] not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_0_0_musician_id
    on rel.musician using btree(id);
create index attr_0_1_musician_name
    on rel.musician using btree(left(name, 2048));
create index attr_0_2_musician_main_band
    on rel.musician using btree(main_band);
create index attr_0_3_musician_bands
    on rel.musician using gin(bands);

create table rel.band (
        id                   text not null,
        name                 text not null,
        original_songs       text[] not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_0_band_id
    on rel.band using btree(id);
create index attr_1_1_band_name
    on rel.band using btree(left(name, 2048));
create index attr_1_2_band_original_songs
    on rel.band using gin(original_songs);

create table rel.song (
        id                   text not null,
        title                text not null,
        written_by           text not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_2_0_song_id
    on rel.song using btree(id);
create index attr_2_1_song_title
    on rel.song using btree(left(title, 2048));
create index attr_2_2_song_written_by
    on rel.song using btree(written_by);

create table rel.song_stat (
        id                   text not null,
        played               integer not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_3_0_song_stat_id
    on rel.song_stat using btree(id);
create index attr_3_1_song_stat_played
    on rel.song_stat using btree(played);

";

    const FOREST_GQL: &str = "
interface ForestDweller {
    id: ID!,
    forest: Forest
}
type Animal implements ForestDweller @entity {
     id: ID!,
     forest: Forest
}
type Forest @entity {
    id: ID!,
    # Array of interfaces as derived reference
    dwellers: [ForestDweller!]! @derivedFrom(field: \"forest\")
}
type Habitat @entity {
    id: ID!,
    # Use interface as direct reference
    most_common: ForestDweller!,
    dwellers: [ForestDweller!]!
}";

    const FOREST_DDL: &str = "create table rel.animal (
        id                   text not null,
        forest               text,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_0_0_animal_id
    on rel.animal using btree(id);
create index attr_0_1_animal_forest
    on rel.animal using btree(forest);

create table rel.forest (
        id                   text not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_0_forest_id
    on rel.forest using btree(id);

create table rel.habitat (
        id                   text not null,
        most_common          text not null,
        dwellers             text[] not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_2_0_habitat_id
    on rel.habitat using btree(id);
create index attr_2_1_habitat_most_common
    on rel.habitat using btree(most_common);
create index attr_2_2_habitat_dwellers
    on rel.habitat using gin(dwellers);

";

}
