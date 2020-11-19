//! Support for storing the entities of a subgraph in a relational schema,
//! i.e., one where each entity type gets its own table, and the table
//! structure follows the structure of the GraphQL type, using the
//! native SQL types that most appropriately map to the corresponding
//! GraphQL types
//!
//! The pivotal struct in this module is the `Layout` which handles all the
//! information about mapping a GraphQL schema to database tables
use diesel::connection::SimpleConnection;
use diesel::{
    debug_query, ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl,
};
use graphql_parser::query as q;
use graphql_parser::schema as s;
use inflector::Inflector;
use lazy_static::lazy_static;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::{From, TryFrom, TryInto};
use std::env;
use std::fmt::{self, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::relational_queries::{
    self as rq, ClampRangeQuery, ConflictingEntityQuery, DeleteByPrefixQuery,
    DeleteDynamicDataSourcesQuery, DeleteQuery, EntityData, FilterCollection, FilterQuery,
    FindManyQuery, FindQuery, InsertQuery, RevertClampQuery, RevertRemoveQuery, UpdateQuery,
};
use graph::data::graphql::ext::{DocumentExt, ObjectTypeExt};
use graph::data::schema::{FulltextConfig, FulltextDefinition, Schema, SCHEMA_TYPE_NAME};
use graph::data::store::BYTES_SCALAR;
use graph::data::subgraph::schema::{
    DynamicEthereumContractDataSourceEntity, POI_OBJECT, POI_TABLE,
};
use graph::prelude::{
    format_err, info, BlockNumber, Entity, EntityChange, EntityChangeOperation, EntityCollection,
    EntityFilter, EntityKey, EntityOrder, EntityRange, EthereumBlockPointer, Logger,
    QueryExecutionError, StoreError, StoreEvent, SubgraphDeploymentId, Value, ValueType,
    BLOCK_NUMBER_MAX,
};

use crate::block_range::{BLOCK_RANGE_COLUMN, BLOCK_UNVERSIONED};
pub use crate::catalog::Catalog;
use crate::entities::STRING_PREFIX_SIZE;

lazy_static! {
    /// Experimental: a list of fully qualified table names that contain
    /// entities that are like accounts in that they have a relatively small
    /// number of entities, with a large number of change for each entity. It
    /// is useful to treat such tables special in queries by changing the
    /// clause that selects for a specific block range in a way that makes
    /// the BRIN index on block_range usable
    ///
    /// Example: GRAPH_ACCOUNT_TABLES=sgd21902.pair,sgd1708.things
    static ref ACCOUNT_TABLES: HashSet<String> = {
        env::var("GRAPH_ACCOUNT_TABLES")
            .ok()
            .map(|v| v.split(",").map(|s| s.to_owned()).collect())
            .unwrap_or(HashSet::new())
    };
}

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
    pub fn check_valid_identifier(name: &str, kind: &str) -> Result<(), StoreError> {
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

    pub fn verbatim(s: String) -> Self {
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
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum IdType {
    String,
    Bytes,
}

impl TryFrom<&s::ObjectType> for IdType {
    type Error = StoreError;

    fn try_from(obj_type: &s::ObjectType) -> Result<Self, Self::Error> {
        let pk = obj_type
            .field(&PRIMARY_KEY_COLUMN.to_owned())
            .expect("Each ObjectType has an `id` field");
        Self::try_from(&pk.field_type)
    }
}

impl TryFrom<&s::Type> for IdType {
    type Error = StoreError;

    fn try_from(field_type: &s::Type) -> Result<Self, Self::Error> {
        let name = named_type(field_type);

        match ValueType::from_str(name)? {
            ValueType::String => Ok(IdType::String),
            ValueType::Bytes => Ok(IdType::Bytes),
            _ => Err(format_err!(
                "The `id` field has type `{}` but only `String`, `Bytes`, and `ID` are allowed",
                &name
            )
            .into()),
        }
    }
}

type IdTypeMap = HashMap<String, IdType>;

type EnumMap = BTreeMap<String, Arc<BTreeSet<String>>>;

#[derive(Debug, Clone)]
pub struct Layout {
    /// Maps the GraphQL name of a type to the relational table
    pub tables: HashMap<String, Arc<Table>>,
    /// The subgraph id
    pub subgraph: SubgraphDeploymentId,
    /// The database schema for this subgraph
    pub catalog: Catalog,
    /// Enums defined in the schema and their possible values. The names
    /// are the original GraphQL names
    pub enums: EnumMap,
    /// The query to count all entities
    pub count_query: String,
}

impl Layout {
    /// Generate a layout for a relational schema for entities in the
    /// GraphQL schema `schema`. The name of the database schema in which
    /// the subgraph's tables live is in `schema`.
    pub fn new(
        schema: &Schema,
        catalog: Catalog,
        create_proof_of_indexing: bool,
    ) -> Result<Self, StoreError> {
        // Extract enum types
        let enums: EnumMap = schema
            .document
            .get_enum_definitions()
            .iter()
            .map(
                |enum_type| -> Result<(String, Arc<BTreeSet<String>>), StoreError> {
                    SqlName::check_valid_identifier(&enum_type.name, "enum")?;
                    Ok((
                        enum_type.name.clone(),
                        Arc::new(
                            enum_type
                                .values
                                .iter()
                                .map(|value| value.name.to_owned())
                                .collect::<BTreeSet<_>>(),
                        ),
                    ))
                },
            )
            .collect::<Result<_, _>>()?;

        // List of all object types that are not __SCHEMA__
        let object_types = schema
            .document
            .get_object_type_definitions()
            .into_iter()
            .filter(|obj_type| obj_type.name != SCHEMA_TYPE_NAME)
            .collect::<Vec<_>>();

        // For interfaces, check that all implementors use the same IdType
        // and build a list of name/IdType pairs
        let id_types_for_interface = schema.types_for_interface.iter().map(|(interface, types)| {
            types
                .iter()
                .map(|obj_type| IdType::try_from(obj_type))
                .collect::<Result<HashSet<_>, _>>()
                .and_then(move |types| {
                    if types.len() > 1 {
                        Err(format_err!(
                            "The implementations of interface \
                            `{}` use different types for the `id` field",
                            interface
                        )
                        .into())
                    } else {
                        // For interfaces that are not implemented at all, pretend
                        // they have a String `id` field
                        let id_type = types.iter().next().cloned().unwrap_or(IdType::String);
                        Ok((interface.to_owned(), id_type))
                    }
                })
        });

        // Map of type name to the type of the ID column for the object_types
        // and interfaces in the schema
        let id_types = object_types
            .iter()
            .map(|obj_type| IdType::try_from(*obj_type).map(|t| (obj_type.name.to_owned(), t)))
            .chain(id_types_for_interface)
            .collect::<Result<IdTypeMap, _>>()?;

        // Construct a Table struct for each ObjectType
        let mut tables = object_types
            .iter()
            .enumerate()
            .map(|(i, obj_type)| {
                Table::new(
                    obj_type,
                    &catalog,
                    Schema::entity_fulltext_definitions(&obj_type.name, &schema.document),
                    &enums,
                    &id_types,
                    i as u32,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if create_proof_of_indexing {
            tables.push(Self::make_poi_table(&catalog, tables.len()))
        }

        let tables: Vec<_> = tables.into_iter().map(|table| Arc::new(table)).collect();

        let count_query = tables
            .iter()
            .map(|table| {
                format!(
                    "select count(*) from \"{}\".\"{}\" where upper_inf(block_range)",
                    &catalog.schema, table.name
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
            subgraph: schema.id.clone(),
            catalog,
            tables,
            enums,
            count_query,
        })
    }

    fn make_poi_table(catalog: &Catalog, position: usize) -> Table {
        let table_name = SqlName::verbatim(POI_TABLE.to_owned());
        Table {
            object: POI_OBJECT.to_owned(),
            qualified_name: SqlName::qualified_name(&catalog.schema, &table_name),
            name: table_name,
            columns: vec![
                Column {
                    name: SqlName::from("digest"),
                    field: "digest".to_owned(),
                    field_type: q::Type::NonNullType(Box::new(q::Type::NamedType(
                        BYTES_SCALAR.to_owned(),
                    ))),
                    column_type: ColumnType::Bytes,
                    fulltext_fields: None,
                    is_reference: false,
                },
                Column {
                    name: SqlName::from(PRIMARY_KEY_COLUMN),
                    field: PRIMARY_KEY_COLUMN.to_owned(),
                    field_type: q::Type::NonNullType(Box::new(q::Type::NamedType(
                        "String".to_owned(),
                    ))),
                    column_type: ColumnType::String,
                    fulltext_fields: None,
                    is_reference: false,
                },
            ],
            /// The position of this table in all the tables for this layout; this
            /// is really only needed for the tests to make the names of indexes
            /// predictable
            position: position as u32,
            is_account_like: false,
        }
    }

    pub fn create_relational_schema(
        conn: &PgConnection,
        schema: &Schema,
        schema_name: String,
    ) -> Result<Layout, StoreError> {
        let catalog = Catalog::new(conn, schema_name)?;
        let layout = Self::new(schema, catalog, true)?;
        let sql = layout
            .as_ddl()
            .map_err(|_| StoreError::Unknown(format_err!("failed to generate DDL for layout")))?;
        conn.batch_execute(&sql)?;
        Ok(layout)
    }

    pub fn copy_from(
        &self,
        logger: &Logger,
        conn: &PgConnection,
        base: &Layout,
        block: EthereumBlockPointer,
        metadata: &Layout,
    ) -> Result<(), StoreError> {
        // This can not be used to copy data to or from the metadata subgraph
        assert!(!self.subgraph.is_meta());
        assert!(!base.subgraph.is_meta());

        info!(
            logger,
            "Initializing graft by copying data from {} to {}", base.subgraph, self.subgraph
        );

        // 1. Copy subgraph data
        // We allow both not copying tables at all from the source, as well
        // as adding new tables in `self`; we only need to check that tables
        // that actually need to be copied from the source are compatible
        // with the corresponding tables in `self`
        for (dst, src) in self
            .tables
            .values()
            .filter_map(|dst| base.table(&dst.name).map(|src| (dst, src)))
        {
            let start = Instant::now();
            let count = rq::CopyEntityDataQuery::new(dst, src)?.execute(conn)?;
            info!(logger, "Copied {} {} entities", count, src.object;
                  "time_ms" => start.elapsed().as_millis());
        }

        // 2. Copy dynamic data sources and adjust their ID
        use crate::metadata::dynamic_ethereum_contract_data_source as decds;
        // Find existing dynamic data sources
        let start = Instant::now();
        let dds = decds::table
            .select(decds::id)
            .filter(decds::deployment.eq(base.subgraph.as_str()))
            .load::<String>(conn)?;
        // Create an equal number of brand new ids
        let new_dds = (0..dds.len())
            .map(|_| DynamicEthereumContractDataSourceEntity::make_id())
            .collect::<Vec<_>>();
        // Copy the data sources and all their subordinate entities, translating
        // ids into new ids in the process and attaching them to `self.subgraph`
        rq::CopyDynamicDataSourceQuery::new(&dds, &new_dds, self.subgraph.as_str())
            .execute(conn)?;
        info!(logger, "Copied {} dynamic data sources", dds.len();
              "time_ms" => start.elapsed().as_millis());

        // 3. Rewind the subgraph. `revert_block` gets rid of everything
        // including the block passed to it. We want to preserve `block`
        // and therefore revert `block+1`
        let start = Instant::now();
        let block_to_revert: BlockNumber = (block.number + 1)
            .try_into()
            .expect("block numbers fit into an i32");
        self.revert_block(conn, block_to_revert)?;
        metadata.revert_metadata(conn, &self.subgraph, block_to_revert)?;
        info!(logger, "Rewound subgraph to block {}", block.number;
              "time_ms" => start.elapsed().as_millis());
        Ok(())
    }

    /// Determine if it is possible to copy the data of `source` into `self`
    /// by checking that our schema is compatible with `source`.
    /// Returns a list of errors if copying is not possible. An empty
    /// vector indicates that copying is possible
    pub fn can_copy_from(&self, base: &Layout) -> Vec<String> {
        // We allow both not copying tables at all from the source, as well
        // as adding new tables in `self`; we only need to check that tables
        // that actually need to be copied from the source are compatible
        // with the corresponding tables in `self`
        self.tables
            .values()
            .filter_map(|dst| base.table(&dst.name).map(|src| (dst, src)))
            .map(|(dst, src)| dst.can_copy_from(src))
            .flatten()
            .collect()
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
                self.catalog.schema,
                name.quoted()
            )?;
            for value in values.iter() {
                write!(out, "{}'{}'", sep, value)?;
                sep = ", "
            }
            writeln!(out, ");")?;
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
    pub fn table(&self, name: &SqlName) -> Option<&Table> {
        self.tables
            .values()
            .find(|table| &table.name == name)
            .map(|rc| rc.as_ref())
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
            .map(|entity_data| entity_data.deserialize_with_layout(self))
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
            schema: &self.catalog.schema,
            ids_for_type,
            tables,
            block,
        };
        let mut entities_for_type: BTreeMap<String, Vec<Entity>> = BTreeMap::new();
        for data in query.load::<EntityData>(conn)? {
            entities_for_type
                .entry(data.entity_type())
                .or_default()
                .push(data.deserialize_with_layout(self)?);
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
    pub fn query<T: crate::relational_queries::FromEntityData>(
        &self,
        logger: &Logger,
        conn: &PgConnection,
        collection: EntityCollection,
        filter: Option<EntityFilter>,
        order: EntityOrder,
        range: EntityRange,
        block: BlockNumber,
        query_id: Option<String>,
    ) -> Result<Vec<T>, QueryExecutionError> {
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
        let query = FilterQuery::new(
            &filter_collection,
            filter.as_ref(),
            order,
            range,
            block,
            query_id,
        )?;
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
            .map(|entity_data| {
                entity_data
                    .deserialize_with_layout(self)
                    .map_err(|e| e.into())
            })
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

        crate::metadata::revert_subgraph_errors(conn, &self.subgraph, block)?;

        Ok(StoreEvent::new(changes))
    }

    pub fn is_cacheable(&self) -> bool {
        // This would be false if we still needed to migrate the Layout, but
        // since there are no migrations in the code right now, it is always
        // safe to cache a Layout
        true
    }
}

/// A user-defined enum
#[derive(Clone, Debug, PartialEq)]
pub struct EnumType {
    /// The name of the Postgres enum we created, fully qualified with the schema
    pub name: SqlName,
    /// The possible values the enum can take
    values: Arc<BTreeSet<String>>,
}

impl EnumType {
    fn is_assignable_from(&self, source: &Self) -> Option<String> {
        if source.values.is_subset(self.values.as_ref()) {
            None
        } else {
            Some(format!(
                "the enum type {} contains values not present in {}",
                source.name, self.name
            ))
        }
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
    Enum(EnumType),
    /// A `bytea` in SQL, represented as a ValueType::String; this is
    /// used for `id` columns of type `Bytes`
    BytesId,
}

impl From<IdType> for ColumnType {
    fn from(id_type: IdType) -> Self {
        match id_type {
            IdType::Bytes => ColumnType::BytesId,
            IdType::String => ColumnType::String,
        }
    }
}

impl ColumnType {
    fn from_field_type(
        field_type: &q::Type,
        catalog: &Catalog,
        enums: &EnumMap,
        id_types: &IdTypeMap,
        is_existing_text_column: bool,
    ) -> Result<ColumnType, StoreError> {
        let name = named_type(field_type);

        // See if its an object type defined in the schema
        if let Some(id_type) = id_types.get(name) {
            return Ok(id_type.clone().into());
        }

        // Check if it's an enum, and if it is, return an appropriate
        // ColumnType::Enum
        if let Some(values) = enums.get(&*name) {
            // We do things this convoluted way to make sure field_type gets
            // snakecased, but the `.` must stay a `.`
            let name = SqlName::qualified_name(&catalog.schema, &SqlName::from(name));
            if is_existing_text_column {
                // We used to have a bug where columns that should have really
                // been of an enum type were created as text columns. To make
                // queries work against such misgenerated tables, we pretend
                // that this GraphQL attribute is really of type `String`
                return Ok(ColumnType::String);
            } else {
                return Ok(ColumnType::Enum(EnumType {
                    name,
                    values: values.clone(),
                }));
            }
        }

        // It is not an object type or an enum, and therefore one of our
        // builtin primitive types
        match ValueType::from_str(name)? {
            ValueType::Boolean => Ok(ColumnType::Boolean),
            ValueType::BigDecimal => Ok(ColumnType::BigDecimal),
            ValueType::BigInt => Ok(ColumnType::BigInt),
            ValueType::Bytes => Ok(ColumnType::Bytes),
            ValueType::Int => Ok(ColumnType::Int),
            ValueType::String => Ok(ColumnType::String),
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
            ColumnType::Enum(enum_type) => enum_type.name.as_str(),
            ColumnType::BytesId => "bytea",
        }
    }

    /// Return the `IdType` corresponding to this column type. This can only
    /// be called on a column that stores an `ID` and will panic otherwise
    pub(crate) fn id_type(&self) -> IdType {
        match self {
            ColumnType::String => IdType::String,
            ColumnType::BytesId => IdType::Bytes,
            _ => unreachable!(
                "only String and BytesId are allowed as primary keys but not {:?}",
                self
            ),
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
        table_name: &SqlName,
        field: &s::Field,
        catalog: &Catalog,
        enums: &EnumMap,
        id_types: &IdTypeMap,
    ) -> Result<Column, StoreError> {
        SqlName::check_valid_identifier(&*field.name, "attribute")?;

        let sql_name = SqlName::from(&*field.name);
        let is_reference =
            sql_name.as_str() != PRIMARY_KEY_COLUMN && is_object_type(&field.field_type, enums);

        let column_type = if sql_name.as_str() == PRIMARY_KEY_COLUMN {
            IdType::try_from(&field.field_type)?.into()
        } else {
            let is_existing_text_column = catalog.is_existing_text_column(table_name, &sql_name);
            ColumnType::from_field_type(
                &field.field_type,
                catalog,
                enums,
                id_types,
                is_existing_text_column,
            )?
        };
        Ok(Column {
            name: sql_name,
            field: field.name.clone(),
            column_type,
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

    pub fn is_assignable_from(&self, source: &Self, object: &str) -> Option<String> {
        if !self.is_nullable() && source.is_nullable() {
            Some(format!(
                "The attribute {}.{} is non-nullable, \
                             but the corresponding attribute in the source is nullable",
                object, self.field
            ))
        } else if let ColumnType::Enum(self_enum_type) = &self.column_type {
            if let ColumnType::Enum(source_enum_type) = &source.column_type {
                self_enum_type.is_assignable_from(source_enum_type)
            } else {
                Some(format!(
                    "The attribute {}.{} is an enum {}, \
                                 but its type in the source is {}",
                    object, self.field, self.field_type, source.field_type
                ))
            }
        } else if self.column_type != source.column_type || self.is_list() != source.is_list() {
            Some(format!(
                "The attribute {}.{} has type {}, \
                             but its type in the source is {}",
                object, self.field, self.field_type, source.field_type
            ))
        } else {
            None
        }
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

    /// This kind of entity behaves like an account in that it has a low
    /// ratio of distinct entities to overall number of rows because
    /// entities are updated frequently on average
    pub is_account_like: bool,

    /// The position of this table in all the tables for this layout; this
    /// is really only needed for the tests to make the names of indexes
    /// predictable
    position: u32,
}

impl Table {
    fn new(
        defn: &s::ObjectType,
        catalog: &Catalog,
        fulltexts: Vec<FulltextDefinition>,
        enums: &EnumMap,
        id_types: &IdTypeMap,
        position: u32,
    ) -> Result<Table, StoreError> {
        SqlName::check_valid_identifier(&*defn.name, "object")?;

        let table_name = SqlName::from(&*defn.name);
        let columns = defn
            .fields
            .iter()
            .filter(|field| !derived_column(field))
            .map(|field| Column::new(&table_name, field, catalog, enums, id_types))
            .chain(fulltexts.iter().map(|def| Column::new_fulltext(def)))
            .collect::<Result<Vec<Column>, StoreError>>()?;
        let is_account_like =
            ACCOUNT_TABLES.contains(&format!("{}.{}", catalog.schema, table_name));
        let table = Table {
            object: defn.name.clone(),
            name: table_name.clone(),
            qualified_name: SqlName::qualified_name(&catalog.schema, &table_name),
            is_account_like,
            columns,
            position,
        };
        Ok(table)
    }

    /// Find the column `name` in this table. The name must be in snake case,
    /// i.e., use SQL conventions
    pub fn column(&self, name: &SqlName) -> Option<&Column> {
        self.columns
            .iter()
            .filter(|column| match column.column_type {
                ColumnType::TSVector(_) => false,
                _ => true,
            })
            .find(|column| &column.name == name)
    }

    /// Find the column for `field` in this table. The name must be the
    /// GraphQL name of an entity field
    pub fn column_for_field(&self, field: &str) -> Result<&Column, StoreError> {
        self.columns
            .iter()
            .find(|column| &column.field == field)
            .ok_or_else(|| StoreError::UnknownField(field.to_string()))
    }

    fn can_copy_from(&self, source: &Self) -> Vec<String> {
        self.columns
            .iter()
            .filter_map(|dcol| match source.column(&dcol.name) {
                Some(scol) => dcol.is_assignable_from(scol, &self.object),
                None => {
                    if !dcol.is_nullable() {
                        Some(format!(
                            "The attribute {}.{} is non-nullable, \
                         but there is no such attribute in the source",
                            self.object, dcol.field
                        ))
                    } else {
                        None
                    }
                }
            })
            .collect()
    }

    pub fn primary_key(&self) -> &Column {
        self.columns
            .iter()
            .find(|column| column.is_primary_key())
            .expect("every table has a primary key")
    }

    /// Generate the DDL for one table, i.e. one `create table` statement
    /// and all `create index` statements for the table's columns
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    fn as_ddl(&self, out: &mut String, layout: &Layout) -> fmt::Result {
        writeln!(
            out,
            "create table {}.{} (",
            layout.catalog.schema,
            self.name.quoted()
        )?;
        for column in self.columns.iter() {
            write!(out, "    ")?;
            column.as_ddl(out)?;
            writeln!(out, ",")?;
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

        // Add a BRIN index on the block_range bounds to exploit the fact
        // that block ranges closely correlate with where in a table an
        // entity appears physically. This index is incredibly efficient for
        // reverts where we look for very recent blocks, so that this index
        // is highly selective. See https://github.com/graphprotocol/graph-node/issues/1415#issuecomment-630520713
        // for details on one experiment.
        //
        // We do not index the `block_range` as a whole, but rather the lower
        // and upper bound separately, since experimentation has shown that
        // Postgres will not use the index on `block_range` for clauses like
        // `block_range @> $block` but rather falls back to a full table scan.
        //
        // We also make sure that we do not put `NULL` in the index for
        // the upper bound since nulls can not be compared to anything and
        // will make the index less effective.
        //
        // To make the index usable, queries need to have clauses using
        // `lower(block_range)` and `coalesce(..)` verbatim.
        //
        // We also index `vid` as that correlates with the order in which
        // entities are stored.
        write!(out,"create index brin_{table_name}\n    \
                    on {schema_name}.{table_name}\n \
                       using brin(lower(block_range), coalesce(upper(block_range), {block_max}), vid);\n",
            table_name = self.name,
            schema_name = layout.catalog.schema,
            block_max = BLOCK_NUMBER_MAX)?;

        // Add a BTree index that helps with the `RevertClampQuery` by making
        // it faster to find entity versions that have been modified
        write!(
            out,
            "create index {table_name}_block_range_closed\n    \
                     on {schema_name}.{table_name}(coalesce(upper(block_range), {block_max}))\n \
                     where coalesce(upper(block_range), {block_max}) < {block_max};\n",
            table_name = self.name,
            schema_name = layout.catalog.schema,
            block_max = BLOCK_NUMBER_MAX
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
            let (method, index_expr) = if column.is_reference() && !column.is_list() {
                // For foreign keys, index the key together with the block range
                // since we almost always also have a block_range clause in
                // queries that look for specific foreign keys
                let index_expr = format!("{}, {}", column.name.quoted(), BLOCK_RANGE_COLUMN);
                ("gist", index_expr)
            } else {
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

                (method, index_expr)
            };
            write!(
                out,
                "create index attr_{table_index}_{column_index}_{table_name}_{column_name}\n    on {schema_name}.\"{table_name}\" using {method}({index_expr});\n",
                table_index = self.position,
                table_name = self.name,
                column_index = i,
                column_name = column.name,
                schema_name = layout.catalog.schema,
                method = method,
                index_expr = index_expr,
            )?;
        }
        writeln!(out)
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

    !enums.contains_key(&*name) && !ValueType::is_scalar(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID_TYPE: ColumnType = ColumnType::String;

    fn test_layout(gql: &str) -> Layout {
        let subgraph = SubgraphDeploymentId::new("subgraph").unwrap();
        let schema = Schema::parse(gql, subgraph).expect("Test schema invalid");
        let catalog = Catalog::make_empty("rel".to_owned()).expect("Can not create catalog");
        Layout::new(&schema, catalog, false).expect("Failed to construct Layout")
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
        assert!(table.column(&bad_sql_name).is_none());
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

        let layout = test_layout(FORWARD_ENUM_GQL);
        let sql = layout.as_ddl().expect("Failed to generate DDL");
        assert_eq!(FORWARD_ENUM_SQL, sql);
    }

    #[test]
    fn forward_enum() {
        let layout = test_layout(FORWARD_ENUM_GQL);
        let table = layout
            .table(&SqlName::from("thing"))
            .expect("thing table exists");
        let column = table
            .column(&SqlName::from("orientation"))
            .expect("orientation column exists");
        assert!(column.is_enum());
    }

    #[test]
    fn can_copy_from() {
        let source = test_layout(THING_GQL);
        // We can always copy from an identical layout
        assert!(source.can_copy_from(&source).is_empty());

        // We allow leaving out and adding types, and leaving out attributes
        // of existing types
        let dest = test_layout("type Scalar { id: ID } type Other { id: ID, int: Int! }");
        assert!(dest.can_copy_from(&source).is_empty());

        // We allow making a non-nullable attribute nullable
        let dest = test_layout("type Thing { id: ID! }");
        assert!(dest.can_copy_from(&source).is_empty());

        // We can not turn a non-nullable attribute into a nullable attribute
        let dest = test_layout("type Scalar { id: ID! }");
        assert_eq!(
            vec![
                "The attribute Scalar.id is non-nullable, but the \
                 corresponding attribute in the source is nullable"
            ],
            dest.can_copy_from(&source)
        );

        // We can not change a scalar field to an array
        let dest = test_layout("type Scalar { id: ID, string: [String] }");
        assert_eq!(
            vec![
                "The attribute Scalar.string has type [String], \
                 but its type in the source is String"
            ],
            dest.can_copy_from(&source)
        );
        // We can not change an array field to a scalar
        assert_eq!(
            vec![
                "The attribute Scalar.string has type String, \
                 but its type in the source is [String]"
            ],
            source.can_copy_from(&dest)
        );
        // We can not change the underlying type of a field
        let dest = test_layout("type Scalar { id: ID, color: Int }");
        assert_eq!(
            vec![
                "The attribute Scalar.color has type Int, but \
                 its type in the source is Color"
            ],
            dest.can_copy_from(&source)
        );
        // We can not change the underlying type of a field in arrays
        let source = test_layout("type Scalar { id: ID, color: [Int!]! }");
        let dest = test_layout("type Scalar { id: ID, color: [String!]! }");
        assert_eq!(
            vec![
                "The attribute Scalar.color has type [String!]!, but \
                 its type in the source is [Int!]!"
            ],
            dest.can_copy_from(&source)
        );
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
    as enum ('BLUE', 'red', 'yellow');
create type rel.\"size\"
    as enum (\'large\', \'medium\', \'small\');
create table rel.\"thing\" (
        \"id\"                 text not null,
        \"big_thing\"          text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_thing
    on rel.thing
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index thing_block_range_closed
    on rel.thing(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_thing_id
    on rel.\"thing\" using btree(\"id\");
create index attr_0_1_thing_big_thing
    on rel.\"thing\" using gist(\"big_thing\", block_range);

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
create index brin_scalar
    on rel.scalar
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index scalar_block_range_closed
    on rel.scalar(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
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
create index brin_musician
    on rel.musician
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index musician_block_range_closed
    on rel.musician(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_musician_id
    on rel.\"musician\" using btree(\"id\");
create index attr_0_1_musician_name
    on rel.\"musician\" using btree(left(\"name\", 256));
create index attr_0_2_musician_main_band
    on rel.\"musician\" using gist(\"main_band\", block_range);
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
create index brin_band
    on rel.band
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index band_block_range_closed
    on rel.band(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
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
create index brin_song
    on rel.song
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index song_block_range_closed
    on rel.song(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_song_id
    on rel.\"song\" using btree(\"id\");
create index attr_2_1_song_title
    on rel.\"song\" using btree(left(\"title\", 256));
create index attr_2_2_song_written_by
    on rel.\"song\" using gist(\"written_by\", block_range);

create table rel.\"song_stat\" (
        \"id\"                 text not null,
        \"played\"             integer not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_song_stat
    on rel.song_stat
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index song_stat_block_range_closed
    on rel.song_stat(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
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
create index brin_animal
    on rel.animal
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index animal_block_range_closed
    on rel.animal(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_animal_id
    on rel.\"animal\" using btree(\"id\");
create index attr_0_1_animal_forest
    on rel.\"animal\" using gist(\"forest\", block_range);

create table rel.\"forest\" (
        \"id\"                 text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_forest
    on rel.forest
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index forest_block_range_closed
    on rel.forest(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
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
create index brin_habitat
    on rel.habitat
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index habitat_block_range_closed
    on rel.habitat(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_habitat_id
    on rel.\"habitat\" using btree(\"id\");
create index attr_2_1_habitat_most_common
    on rel.\"habitat\" using gist(\"most_common\", block_range);
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
create index brin_animal
    on rel.animal
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index animal_block_range_closed
    on rel.animal(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_animal_id
    on rel.\"animal\" using btree(\"id\");
create index attr_0_1_animal_name
    on rel.\"animal\" using btree(left(\"name\", 256));
create index attr_0_2_animal_species
    on rel.\"animal\" using btree(left(\"species\", 256));
create index attr_0_3_animal_forest
    on rel.\"animal\" using gist(\"forest\", block_range);
create index attr_0_4_animal_search
    on rel.\"animal\" using gin(\"search\");

create table rel.\"forest\" (
        \"id\"                 text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_forest
    on rel.forest
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index forest_block_range_closed
    on rel.forest(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
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
create index brin_habitat
    on rel.habitat
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index habitat_block_range_closed
    on rel.habitat(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_habitat_id
    on rel.\"habitat\" using btree(\"id\");
create index attr_2_1_habitat_most_common
    on rel.\"habitat\" using gist(\"most_common\", block_range);
create index attr_2_2_habitat_dwellers
    on rel.\"habitat\" using gin(\"dwellers\");

";

    const FORWARD_ENUM_GQL: &str = "
type Thing @entity  {
    id: ID!,
    orientation: Orientation!
}

enum Orientation {
    UP, DOWN
}
";

    const FORWARD_ENUM_SQL: &str = "create type rel.\"orientation\"
    as enum (\'DOWN\', \'UP\');
create table rel.\"thing\" (
        \"id\"                 text not null,
        \"orientation\"        \"rel\".\"orientation\" not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_thing
    on rel.thing
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index thing_block_range_closed
    on rel.thing(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_thing_id
    on rel.\"thing\" using btree(\"id\");
create index attr_0_1_thing_orientation
    on rel.\"thing\" using btree(\"orientation\");

";
}
