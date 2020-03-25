//! Support for storing the entities of a subgraph in a relational schema,
//! i.e., one where each entity type gets its own table, and the table
//! structure follows the structure of the GraphQL type, using the
//! native SQL types that most appropriately map to the corresponding
//! GraphQL types
//!
//! The pivotal struct in this module is the `Layout` which handles all the
//! information about mapping a GraphQL schema to database tables
use diesel::connection::SimpleConnection;
use diesel::{debug_query, OptionalExtension, PgConnection, RunQueryDsl};
use graphql_parser::query as q;
use graphql_parser::schema as s;
use inflector::Inflector;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::From;
use std::fmt::{self, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::relational_queries::{
    ClampRangeQuery, ConflictingEntityQuery, DeleteByPrefixQuery, DeleteDynamicDataSourcesQuery,
    DeleteQuery, EntityData, FilterCollection, FilterQuery, FindManyQuery, FindQuery, InsertQuery,
    RevertClampQuery, RevertRemoveQuery, UpdateQuery,
};
use graph::data::schema::{FulltextConfig, FulltextDefinition, Schema, SCHEMA_TYPE_NAME};
use graph::prelude::{
    format_err, info, BlockNumber, Entity, EntityChange, EntityChangeOperation, EntityCollection,
    EntityFilter, EntityKey, EntityOrder, EntityRange, Logger, QueryExecutionError, StoreError,
    StoreEvent, SubgraphDeploymentId, Value, ValueType,
};

use crate::block_range::{BLOCK_RANGE_COLUMN, BLOCK_UNVERSIONED};
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct SqlName(String);

impl SqlName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0)
    }

    // Check that `name` matches the regular expression `/[A-Za-z][A-Za-z0-9_]*/`
    // without pulling in a regex matcher
    fn check_valid_identifier(name: &str, kind: &str) -> Result<(), StoreError> {
        let mut chars = name.chars();
        match chars.next() {
            Some(c) => {
                if !c.is_ascii_alphabetic() && c != '_' {
                    let msg = format!(
                        "the name `{}` can not be used for a {}; \
                         it must start with an ASCII alphabetic character or `_`",
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

    pub fn from_snake_case(s: String) -> Self {
        SqlName(s)
    }

    pub fn qualified_name(schema: &str, name: &SqlName) -> Self {
        SqlName(format!("\"{}\".\"{}\"", schema, name.as_str()))
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

type EnumMap = BTreeMap<String, Vec<String>>;

#[derive(Debug, Clone)]
pub struct Layout {
    /// The SQL type for columns with GraphQL type `ID`
    id_type: IdType,
    /// Maps the GraphQL name of a type to the relational table
    pub tables: HashMap<String, Arc<Table>>,
    /// The subgraph id
    pub subgraph: SubgraphDeploymentId,
    /// The database schema for this subgraph
    pub schema: String,
    /// Enums defined in the schema and their possible values. The names
    /// are the original GraphQL names
    pub enums: EnumMap,
    /// The query to count all entities
    pub count_query: String,
}

impl Layout {
    /// Generate a layout for a relational schema for entities in the
    /// GraphQL schema `document`. Attributes of type `ID` will use the
    /// SQL type `id_type`. The subgraph ID is passed in `subgraph`, and
    /// the name of the database schema in which the subgraph's tables live
    /// is in `schema`.
    pub fn new<V>(
        document: &s::Document,
        id_type: IdType,
        subgraph: SubgraphDeploymentId,
        schema: V,
    ) -> Result<Layout, StoreError>
    where
        V: Into<String>,
    {
        use s::Definition::*;
        use s::TypeDefinition::*;

        let schema = schema.into();
        SqlName::check_valid_identifier(&schema, "database schema")?;

        // Extract interfaces and tables
        let mut tables = Vec::new();
        let mut enums = EnumMap::new();

        for defn in &document.definitions {
            match defn {
                // Do not create a table for the _Schema_ type
                TypeDefinition(Object(obj_type)) if obj_type.name.eq(SCHEMA_TYPE_NAME) => {}
                TypeDefinition(Object(obj_type)) => {
                    tables.push(Table::new(
                        obj_type,
                        &schema,
                        Schema::entity_fulltext_definitions(&obj_type.name, document),
                        &enums,
                        id_type,
                        tables.len() as u32,
                    )?);
                }
                TypeDefinition(Interface(_)) => { /* we do not care about interfaces */ }
                TypeDefinition(Enum(enum_type)) => {
                    SqlName::check_valid_identifier(&enum_type.name, "enum")?;
                    enums.insert(
                        enum_type.name.clone(),
                        enum_type
                            .values
                            .iter()
                            .map(|value| value.name.to_owned())
                            .collect(),
                    );
                }
                other => {
                    return Err(StoreError::Unknown(format_err!(
                        "can not handle {:?}",
                        other
                    )))
                }
            }
        }

        let tables: Vec<_> = tables.into_iter().map(|table| Arc::new(table)).collect();

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

        Ok(Layout {
            id_type,
            subgraph,
            schema,
            tables,
            enums,
            count_query,
        })
    }

    pub fn create_relational_schema(
        conn: &PgConnection,
        schema_name: &str,
        subgraph: SubgraphDeploymentId,
        document: &s::Document,
    ) -> Result<Layout, StoreError> {
        let layout =
            crate::relational::Layout::new(document, IdType::String, subgraph, schema_name)?;
        let sql = layout
            .as_ddl()
            .map_err(|_| StoreError::Unknown(format_err!("failed to generate DDL for layout")))?;
        conn.batch_execute(&sql)?;
        Ok(layout)
    }

    /// Generate the DDL for the entire layout, i.e., all `create table`
    /// and `create index` etc. statements needed in the database schema
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    pub fn as_ddl(&self) -> Result<String, fmt::Error> {
        let mut out = String::new();

        // Output enums first
        for (name, values) in &self.enums {
            let mut sep = "";
            let name = SqlName::from(name.as_str());
            write!(
                out,
                "create type {}.{}\n    as enum (",
                self.schema,
                name.quoted()
            )?;
            for value in values {
                write!(out, "{}'{}'", sep, value)?;
                sep = ", "
            }
            write!(out, ");\n")?;
        }
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

    pub fn table_for_entity(&self, entity: &str) -> Result<&Arc<Table>, StoreError> {
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
        let table = self.table_for_entity(entity)?;
        FindQuery::new(table.as_ref(), id, block)
            .get_result::<EntityData>(conn)
            .optional()?
            .map(|entity_data| entity_data.to_entity(self))
            .transpose()
    }

    pub fn find_many(
        &self,
        conn: &PgConnection,
        ids_for_type: BTreeMap<&str, Vec<&str>>,
        block: BlockNumber,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError> {
        let mut tables = Vec::new();
        for entity_type in ids_for_type.keys() {
            tables.push(self.table_for_entity(entity_type)?.as_ref());
        }
        let query = FindManyQuery {
            schema: &self.schema,
            ids_for_type,
            tables,
            block,
        };
        let mut entities_for_type: BTreeMap<String, Vec<Entity>> = BTreeMap::new();
        for data in query.load::<EntityData>(conn)? {
            entities_for_type
                .entry(data.entity_type())
                .or_default()
                .push(data.to_entity(self)?);
        }
        Ok(entities_for_type)
    }

    pub fn insert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: Entity,
        block: BlockNumber,
    ) -> Result<(), StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        let query = InsertQuery::new(table, key, entity, block)?;
        query.execute(conn)?;
        Ok(())
    }

    pub fn insert_unversioned(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: Entity,
    ) -> Result<(), StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        let query = InsertQuery::new(table, key, entity, BLOCK_UNVERSIONED)?;
        query.execute(conn)?;
        Ok(())
    }

    pub fn conflicting_entity(
        &self,
        conn: &PgConnection,
        entity_id: &String,
        entities: Vec<&String>,
    ) -> Result<Option<String>, StoreError> {
        Ok(ConflictingEntityQuery::new(self, entities, entity_id)?
            .load(conn)?
            .pop()
            .map(|data| data.entity))
    }

    /// order is a tuple (attribute, value_type, direction)
    pub fn query(
        &self,
        logger: &Logger,
        conn: &PgConnection,
        collection: EntityCollection,
        filter: Option<EntityFilter>,
        order: Option<(String, ValueType, EntityOrder)>,
        range: EntityRange,
        block: BlockNumber,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        fn log_query_timing(
            logger: &Logger,
            query: &FilterQuery,
            elapsed: Duration,
            entity_count: usize,
        ) {
            // 20kB
            const MAXLEN: usize = 20_480;

            if !*graph::log::LOG_SQL_TIMING {
                return;
            }

            let mut text = debug_query(&query).to_string().replace("\n", " ");
            // If the query + bind variables is more than MAXLEN, truncate it;
            // this will happen when queries have very large bind variables
            // (e.g., long arrays of string ids)
            if text.len() > MAXLEN {
                text.truncate(MAXLEN);
                text.push_str(" ...");
            }
            info!(
                logger,
                "Query timing (SQL)";
                "query" => text,
                "time_ms" => elapsed.as_millis(),
                "entity_count" => entity_count
            );
        }

        let filter_collection = FilterCollection::new(&self, collection, filter.as_ref())?;
        let query = FilterQuery::new(&filter_collection, filter.as_ref(), order, range, block)?;
        let query_clone = query.clone();

        let start = Instant::now();
        let values = query.load::<EntityData>(conn).map_err(|e| {
            QueryExecutionError::ResolveEntitiesError(format!(
                "{}, query = {:?}",
                e,
                debug_query(&query_clone).to_string()
            ))
        })?;
        log_query_timing(logger, &query_clone, start.elapsed(), values.len());
        values
            .into_iter()
            .map(|entity_data| entity_data.to_entity(self).map_err(|e| e.into()))
            .collect()
    }

    pub fn update(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: Entity,
        block: BlockNumber,
    ) -> Result<(), StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        ClampRangeQuery::new(table, key, block).execute(conn)?;
        let query = InsertQuery::new(table, key, entity, block)?;
        query.execute(conn)?;
        Ok(())
    }

    pub fn update_unversioned(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: &Entity,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        let query = UpdateQuery::new(table, key, entity)?;
        Ok(query.execute(conn)?)
    }

    pub fn overwrite_unversioned(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        mut entity: Entity,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        // Set any attributes not mentioned in the entity to
        // their default (NULL)
        for column in table.columns.iter() {
            if !entity.contains_key(&column.field) {
                entity.insert(column.field.clone(), Value::Null);
            }
        }
        let query = UpdateQuery::new(table, key, &entity)?;
        Ok(query.execute(conn)?)
    }

    pub fn delete(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        block: BlockNumber,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        Ok(ClampRangeQuery::new(table, key, block).execute(conn)?)
    }

    pub fn delete_unversioned(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        Ok(DeleteQuery::new(table, key).execute(conn)?)
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
            let removed = RevertRemoveQuery::new(table, block)
                .get_results(conn)?
                .into_iter()
                .map(|data| data.id)
                .collect::<HashSet<_>>();
            // Make the versions current that existed at `block - 1` but that
            // are not current yet. Those are the ones that were updated or
            // deleted at `block`
            let unclamped = RevertClampQuery::new(table, block - 1)
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

    /// Revert the metadata (dynamic data sources and related entities) for
    /// the given `subgraph`. This function can only be called on the `Layout`
    /// for the metadata subgraph.
    ///
    /// For metadata, reversion always means deletion since the metadata that
    /// is subject to reversion is only ever created but never updated
    pub fn revert_metadata(
        &self,
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
        block: BlockNumber,
    ) -> Result<StoreEvent, StoreError> {
        assert!(self.subgraph.is_meta());
        const DDS: &str = "DynamicEthereumContractDataSource";

        // Delete dynamic data sources for this subgraph at the given block
        // and get their id's
        let dds = DeleteDynamicDataSourcesQuery::new(subgraph.as_str(), block)
            .get_results(conn)?
            .into_iter()
            .map(|data| data.id)
            .collect::<Vec<_>>();

        // Calculate how long id's are and make sure they have all the same length
        let prefix_len = dds.iter().map(|id| id.len()).min();
        assert_eq!(prefix_len, dds.iter().map(|id| id.len()).max());
        let prefix_len = prefix_len.unwrap_or(0) as i32;

        let mut changes: Vec<EntityChange> = dds
            .iter()
            .map(|id| EntityChange {
                subgraph_id: self.subgraph.clone(),
                entity_type: DDS.to_owned(),
                entity_id: id.to_owned(),
                operation: EntityChangeOperation::Removed,
            })
            .collect();

        if !dds.is_empty() {
            // Remove subordinate entities for the dynamic data sources from
            // the various metadata tables. We do not need to consider the
            // table for dynamic data sources, not any table whose name starts
            // with 'Subgraph'. Since the set of metadata entities might change
            // in the future, it is safer to exclude entity types that we know
            // do not contain data of interest rather than check for inclusion
            // in a whitelist, as it should generally be safe to do the deletion
            // on all metadata tables
            //
            // This code is far from ideal since it relies on a few unenforceable
            // assumptions, most importantly, that the id of any entity that
            // belongs to a dynmaic data source starts with the id of that data
            // source
            for table in self
                .tables
                .values()
                .filter(|table| table.object != DDS && !table.object.starts_with("Subgraph"))
            {
                let deleted = DeleteByPrefixQuery::new(table, &dds, prefix_len)
                    .get_results(conn)?
                    .into_iter()
                    .map(|data| EntityChange {
                        subgraph_id: self.subgraph.clone(),
                        entity_type: table.object.clone(),
                        entity_id: data.id,
                        operation: EntityChangeOperation::Removed,
                    });
                changes.extend(deleted);
            }
        }
        Ok(StoreEvent::new(changes))
    }
}

/// This is almost the same as graph::data::store::ValueType, but without
/// ID and List; with this type, we only care about scalar types that directly
/// correspond to Postgres scalar types
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    Boolean,
    BigDecimal,
    BigInt,
    Bytes,
    Int,
    String,
    TSVector(FulltextConfig),
    /// A user-defined enum. The string contains the name of the Postgres
    /// enum we created for it, fully qualified with the schema
    Enum(SqlName),
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
    fn from_field_type(
        field_type: &q::Type,
        schema: &str,
        enums: &EnumMap,
        id_type: IdType,
    ) -> Result<ColumnType, StoreError> {
        let name = named_type(field_type);

        // Check if it's an enum, and if it is, return an appropriate
        // ColumnType::Enum
        if enums.contains_key(&*name) {
            // We do things this convoluted way to make sure field_type gets
            // snakecased, but the `.` must stay a `.`
            return Ok(ColumnType::Enum(SqlName::qualified_name(
                schema,
                &SqlName::from(name),
            )));
        }

        // It is not an enum, and therefore one of our builtin primitive types
        // or a reference to another type. For the latter, we use `ValueType::ID`
        // as the underlying type
        match ValueType::from_str(name).unwrap_or(ValueType::ID) {
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

    pub fn sql_type(&self) -> &str {
        match self {
            ColumnType::Boolean => "boolean",
            ColumnType::BigDecimal => "numeric",
            ColumnType::BigInt => "numeric",
            ColumnType::Bytes => "bytea",
            ColumnType::Int => "integer",
            ColumnType::String => "text",
            ColumnType::TSVector(_) => "tsvector",
            ColumnType::Enum(name) => name.as_str(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: SqlName,
    pub field: String,
    pub field_type: q::Type,
    pub column_type: ColumnType,
    pub fulltext_fields: Option<HashSet<String>>,
    is_reference: bool,
}

impl Column {
    fn new(
        field: &s::Field,
        schema: &str,
        enums: &EnumMap,
        id_type: IdType,
    ) -> Result<Column, StoreError> {
        SqlName::check_valid_identifier(&*field.name, "attribute")?;

        let sql_name = SqlName::from(&*field.name);
        let is_reference =
            sql_name.as_str() != PRIMARY_KEY_COLUMN && is_object_type(&field.field_type, enums);

        Ok(Column {
            name: sql_name,
            field: field.name.clone(),
            column_type: ColumnType::from_field_type(&field.field_type, schema, enums, id_type)?,
            field_type: field.field_type.clone(),
            fulltext_fields: None,
            is_reference,
        })
    }

    fn new_fulltext(def: &FulltextDefinition) -> Result<Column, StoreError> {
        SqlName::check_valid_identifier(&def.name, "attribute")?;
        let sql_name = SqlName::from(def.name.as_str());

        Ok(Column {
            name: sql_name,
            field: def.name.to_string(),
            field_type: q::Type::NamedType(String::from("fulltext".to_string())),
            column_type: ColumnType::TSVector(def.config.clone()),
            fulltext_fields: Some(def.included_fields.clone()),
            is_reference: false,
        })
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

    pub fn is_enum(&self) -> bool {
        if let ColumnType::Enum(_) = self.column_type {
            true
        } else {
            false
        }
    }

    pub fn is_fulltext(&self) -> bool {
        named_type(&self.field_type) == "fulltext"
    }

    pub fn is_reference(&self) -> bool {
        self.is_reference
    }

    pub fn is_primary_key(&self) -> bool {
        self.name.as_str() == PRIMARY_KEY_COLUMN
    }

    /// Return `true` if this column stores user-supplied text. Such
    /// columns may contain very large values and need to be handled
    /// specially for indexing
    pub fn is_text(&self) -> bool {
        named_type(&self.field_type) == "String" && !self.is_list()
    }

    /// Generate the DDL for one column, i.e. the part of a `create table`
    /// statement for this column.
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    fn as_ddl(&self, out: &mut String) -> fmt::Result {
        write!(out, "    ")?;
        write!(out, "{:20} {}", self.name.quoted(), self.sql_type())?;
        if self.is_list() {
            write!(out, "[]")?;
        }
        if self.is_primary_key() || !self.is_nullable() {
            write!(out, " not null")?;
        }
        Ok(())
    }
}

/// The name for the primary key column of a table; hardcoded for now
pub(crate) const PRIMARY_KEY_COLUMN: &str = "id";

/// We give every version of every entity in our tables, i.e., every row, a
/// synthetic primary key. This is the name of the column we use.
pub(crate) const VID_COLUMN: &str = "vid";

#[derive(Clone, Debug)]
pub struct Table {
    /// The name of the GraphQL object type ('Thing')
    pub object: s::Name,
    /// The name of the database table for this type ('thing'), snakecased
    /// version of `object`
    pub name: SqlName,

    /// The table name qualified with the schema in which the table lives,
    /// `schema.table`
    pub qualified_name: SqlName,

    pub columns: Vec<Column>,
    /// The position of this table in all the tables for this layout; this
    /// is really only needed for the tests to make the names of indexes
    /// predictable
    position: u32,
}

impl Table {
    fn new(
        defn: &s::ObjectType,
        schema: &str,
        fulltexts: Vec<FulltextDefinition>,
        enums: &EnumMap,
        id_type: IdType,
        position: u32,
    ) -> Result<Table, StoreError> {
        SqlName::check_valid_identifier(&*defn.name, "object")?;

        let table_name = SqlName::from(&*defn.name);
        let columns = defn
            .fields
            .iter()
            .filter(|field| !derived_column(field))
            .map(|field| Column::new(field, schema, enums, id_type))
            .chain(fulltexts.iter().map(|def| Column::new_fulltext(def)))
            .collect::<Result<Vec<Column>, StoreError>>()?;

        let table = Table {
            object: defn.name.clone(),
            name: table_name.clone(),
            qualified_name: SqlName::qualified_name(schema, &table_name),
            columns,
            position,
        };
        Ok(table)
    }

    /// Find the column `name` in this table. The name must be in snake case,
    /// i.e., use SQL conventions
    pub fn column(&self, name: &SqlName) -> Result<&Column, StoreError> {
        self.columns
            .iter()
            .filter(|column| match column.column_type {
                ColumnType::TSVector(_) => false,
                _ => true,
            })
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
    fn as_ddl(&self, out: &mut String, layout: &Layout) -> fmt::Result {
        write!(
            out,
            "create table {}.{} (\n",
            layout.schema,
            self.name.quoted()
        )?;
        for column in self.columns.iter() {
            write!(out, "    ")?;
            column.as_ddl(out)?;
            write!(out, ",\n")?;
        }
        // Add block_range column and constraint
        write!(
            out,
            "\n        {vid}                  bigserial primary key,\
             \n        {block_range}          int4range not null,
        exclude using gist   (id with =, {block_range} with &&)\n);\n",
            vid = VID_COLUMN,
            block_range = BLOCK_RANGE_COLUMN
        )?;

        // Create indexes. Skip columns whose type is an array of enum,
        // since there is no good way to index them with Postgres 9.6.
        // Once we move to Postgres 11, we can enable that
        // (tracked in graph-node issue #1330)
        for (i, column) in self
            .columns
            .iter()
            .filter(|col| !(col.is_list() && col.is_enum()))
            .enumerate()
        {
            // Attributes that are plain strings are indexed with a BTree; but
            // they can be too large for Postgres' limit on values that can go
            // into a BTree. For those attributes, only index the first
            // STRING_PREFIX_SIZE characters
            let index_expr = if column.is_text() {
                format!("left({}, {})", column.name.quoted(), STRING_PREFIX_SIZE)
            } else {
                column.name.quoted()
            };

            let method = if column.is_list() || column.is_fulltext() {
                "gin"
            } else {
                "btree"
            };
            write!(
                out,
                "create index attr_{table_index}_{column_index}_{table_name}_{column_name}\n    on {schema_name}.\"{table_name}\" using {method}({index_expr});\n",
                table_index = self.position,
                table_name = self.name,
                column_index = i,
                column_name = column.name,
                schema_name = layout.schema,
                method = method,
                index_expr = index_expr,
            )?;
        }
        write!(out, "\n")
    }
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

fn is_object_type(field_type: &q::Type, enums: &EnumMap) -> bool {
    let name = named_type(field_type);

    !enums.contains_key(&*name)
        && ValueType::from_str(name).unwrap_or(ValueType::ID) == ValueType::ID
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphql_parser::parse_schema;

    const ID_TYPE: ColumnType = ColumnType::String;

    fn test_layout(gql: &str) -> Layout {
        let schema = parse_schema(gql).expect("Test schema invalid");
        let subgraph = SubgraphDeploymentId::new("subgraph").unwrap();
        Layout::new(&schema, IdType::String, subgraph, "rel").expect("Failed to construct Layout")
    }

    #[test]
    fn table_is_sane() {
        let layout = test_layout(THING_GQL);
        let table = layout
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
        let layout = test_layout(THING_GQL);
        let sql = layout.as_ddl().expect("Failed to generate DDL");
        assert_eq!(THING_DDL, sql);

        let layout = test_layout(MUSIC_GQL);
        let sql = layout.as_ddl().expect("Failed to generate DDL");
        assert_eq!(MUSIC_DDL, sql);

        let layout = test_layout(FOREST_GQL);
        let sql = layout.as_ddl().expect("Failed to generate DDL");
        assert_eq!(FOREST_DDL, sql);

        let layout = test_layout(FULLTEXT_GQL);
        let sql = layout.as_ddl().expect("Failed to generate DDL");
        assert_eq!(FULLTEXT_DDL, sql);
    }

    const THING_GQL: &str = "
        type Thing @entity {
            id: ID!
            bigThing: Thing!
        }

        enum Color { yellow, red, BLUE }

        enum Size { small, medium, large }

        type Scalar {
            id: ID,
            bool: Boolean,
            int: Int,
            bigDecimal: BigDecimal,
            string: String,
            bytes: Bytes,
            bigInt: BigInt,
            color: Color,
        }";

    const THING_DDL: &str = "create type rel.\"color\"
    as enum ('yellow', 'red', 'BLUE');
create type rel.\"size\"
    as enum (\'small\', \'medium\', \'large\');
create table rel.\"thing\" (
        \"id\"                 text not null,
        \"big_thing\"          text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_0_0_thing_id
    on rel.\"thing\" using btree(\"id\");
create index attr_0_1_thing_big_thing
    on rel.\"thing\" using btree(\"big_thing\");

create table rel.\"scalar\" (
        \"id\"                 text not null,
        \"bool\"               boolean,
        \"int\"                integer,
        \"big_decimal\"        numeric,
        \"string\"             text,
        \"bytes\"              bytea,
        \"big_int\"            numeric,
        \"color\"              \"rel\".\"color\",

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_0_scalar_id
    on rel.\"scalar\" using btree(\"id\");
create index attr_1_1_scalar_bool
    on rel.\"scalar\" using btree(\"bool\");
create index attr_1_2_scalar_int
    on rel.\"scalar\" using btree(\"int\");
create index attr_1_3_scalar_big_decimal
    on rel.\"scalar\" using btree(\"big_decimal\");
create index attr_1_4_scalar_string
    on rel.\"scalar\" using btree(left(\"string\", 256));
create index attr_1_5_scalar_bytes
    on rel.\"scalar\" using btree(\"bytes\");
create index attr_1_6_scalar_big_int
    on rel.\"scalar\" using btree(\"big_int\");
create index attr_1_7_scalar_color
    on rel.\"scalar\" using btree(\"color\");

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
    const MUSIC_DDL: &str = "create table rel.\"musician\" (
        \"id\"                 text not null,
        \"name\"               text not null,
        \"main_band\"          text,
        \"bands\"              text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_0_0_musician_id
    on rel.\"musician\" using btree(\"id\");
create index attr_0_1_musician_name
    on rel.\"musician\" using btree(left(\"name\", 256));
create index attr_0_2_musician_main_band
    on rel.\"musician\" using btree(\"main_band\");
create index attr_0_3_musician_bands
    on rel.\"musician\" using gin(\"bands\");

create table rel.\"band\" (
        \"id\"                 text not null,
        \"name\"               text not null,
        \"original_songs\"     text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_0_band_id
    on rel.\"band\" using btree(\"id\");
create index attr_1_1_band_name
    on rel.\"band\" using btree(left(\"name\", 256));
create index attr_1_2_band_original_songs
    on rel.\"band\" using gin(\"original_songs\");

create table rel.\"song\" (
        \"id\"                 text not null,
        \"title\"              text not null,
        \"written_by\"         text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_2_0_song_id
    on rel.\"song\" using btree(\"id\");
create index attr_2_1_song_title
    on rel.\"song\" using btree(left(\"title\", 256));
create index attr_2_2_song_written_by
    on rel.\"song\" using btree(\"written_by\");

create table rel.\"song_stat\" (
        \"id\"                 text not null,
        \"played\"             integer not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_3_0_song_stat_id
    on rel.\"song_stat\" using btree(\"id\");
create index attr_3_1_song_stat_played
    on rel.\"song_stat\" using btree(\"played\");

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

    const FOREST_DDL: &str = "create table rel.\"animal\" (
        \"id\"                 text not null,
        \"forest\"             text,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_0_0_animal_id
    on rel.\"animal\" using btree(\"id\");
create index attr_0_1_animal_forest
    on rel.\"animal\" using btree(\"forest\");

create table rel.\"forest\" (
        \"id\"                 text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_0_forest_id
    on rel.\"forest\" using btree(\"id\");

create table rel.\"habitat\" (
        \"id\"                 text not null,
        \"most_common\"        text not null,
        \"dwellers\"           text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_2_0_habitat_id
    on rel.\"habitat\" using btree(\"id\");
create index attr_2_1_habitat_most_common
    on rel.\"habitat\" using btree(\"most_common\");
create index attr_2_2_habitat_dwellers
    on rel.\"habitat\" using gin(\"dwellers\");

";
    const FULLTEXT_GQL: &str = "
type _Schema_ @fulltext(
    name: \"search\"
    language: en
    algorithm: rank
    include: [\
        {
            entity: \"Animal\",
            fields: [
                {name: \"name\"},
                {name: \"species\"}
            ]
        }
    ]
)
type Animal @entity  {
    id: ID!,
    name: String!
    species: String!
    forest: Forest
}
type Forest @entity {
    id: ID!,
    dwellers: [Animal!]! @derivedFrom(field: \"forest\")
}
type Habitat @entity {
    id: ID!,
    most_common: Animal!,
    dwellers: [Animal!]!
}";

    const FULLTEXT_DDL: &str = "create table rel.\"animal\" (
        \"id\"                 text not null,
        \"name\"               text not null,
        \"species\"            text not null,
        \"forest\"             text,
        \"search\"             tsvector,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_0_0_animal_id
    on rel.\"animal\" using btree(\"id\");
create index attr_0_1_animal_name
    on rel.\"animal\" using btree(left(\"name\", 256));
create index attr_0_2_animal_species
    on rel.\"animal\" using btree(left(\"species\", 256));
create index attr_0_3_animal_forest
    on rel.\"animal\" using btree(\"forest\");
create index attr_0_4_animal_search
    on rel.\"animal\" using gin(\"search\");

create table rel.\"forest\" (
        \"id\"                 text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_0_forest_id
    on rel.\"forest\" using btree(\"id\");

create table rel.\"habitat\" (
        \"id\"                 text not null,
        \"most_common\"        text not null,
        \"dwellers\"           text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_2_0_habitat_id
    on rel.\"habitat\" using btree(\"id\");
create index attr_2_1_habitat_most_common
    on rel.\"habitat\" using btree(\"most_common\");
create index attr_2_2_habitat_dwellers
    on rel.\"habitat\" using gin(\"dwellers\");

";
}
