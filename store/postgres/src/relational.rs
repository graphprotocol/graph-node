//! Support for storing the entities of a subgraph in a relational schema,
//! i.e., one where each entity type gets its own table, and the table
//! structure follows the structure of the GraphQL type, using the
//! native SQL types that most appropriately map to the corresponding
//! GraphQL types
//!
//! The pivotal struct in this module is the `Layout` which handles all the
//! information about mapping a GraphQL schema to database tables
use diesel::{connection::SimpleConnection, Connection};
use diesel::{debug_query, OptionalExtension, PgConnection, RunQueryDsl};
use graph::cheap_clone::CheapClone;
use graph::data::graphql::TypeExt as _;
use graph::prelude::{q, s, StopwatchMetrics};
use graph::slog::warn;
use inflector::Inflector;
use lazy_static::lazy_static;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::{From, TryFrom};
use std::env;
use std::fmt::{self, Write};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{
    primary::{Namespace, Site},
    relational_queries::{
        ClampRangeQuery, ConflictingEntityQuery, EntityData, FilterCollection, FilterQuery,
        FindManyQuery, FindQuery, InsertQuery, RevertClampQuery, RevertRemoveQuery,
    },
};
use graph::components::store::EntityType;
use graph::data::graphql::ext::{DirectiveFinder, DocumentExt, ObjectTypeExt};
use graph::data::schema::{FulltextConfig, FulltextDefinition, Schema, SCHEMA_TYPE_NAME};
use graph::data::store::BYTES_SCALAR;
use graph::data::subgraph::schema::{POI_OBJECT, POI_TABLE};
use graph::prelude::{
    anyhow, info, BlockNumber, DeploymentHash, Entity, EntityChange, EntityCollection,
    EntityFilter, EntityKey, EntityOrder, EntityRange, Logger, QueryExecutionError, StoreError,
    StoreEvent, ValueType, BLOCK_NUMBER_MAX,
};

use crate::block_range::BLOCK_RANGE_COLUMN;
pub use crate::catalog::Catalog;
use crate::connection_pool::ForeignServer;
use crate::{catalog, deployment};

const POSTGRES_MAX_PARAMETERS: usize = u16::MAX as usize; // 65535
const DELETE_OPERATION_CHUNK_SIZE: usize = 1_000;

/// The size of string prefixes that we index. This is chosen so that we
/// will index strings that people will do string comparisons like
/// `=` or `!=` on; if text longer than this is stored in a String attribute
/// it is highly unlikely that they will be used for exact string operations.
/// This also makes sure that we do not put strings into a BTree index that's
/// bigger than Postgres' limit on such strings which is about 2k
pub const STRING_PREFIX_SIZE: usize = 256;

lazy_static! {
    /// Deprecated; use 'graphman stats account-like' instead. A list of
    /// fully qualified table names that contain entities that are like
    /// accounts in that they have a relatively small number of entities,
    /// with a large number of change for each entity. It is useful to treat
    /// such tables special in queries by changing the clause that selects
    /// for a specific block range in a way that makes the BRIN index on
    /// block_range usable
    ///
    /// Example: GRAPH_ACCOUNT_TABLES=sgd21902.pair,sgd1708.things
    static ref ACCOUNT_TABLES: HashSet<String> = {
            // Transform the entries in the form `schema.table` into
            // `"schema"."table"` so that we can compare to a table's
            // qualified name
            env::var("GRAPH_ACCOUNT_TABLES")
                .ok()
                .map(|v| v.split(',').map(|s| format!("\"{}\"", s.replace(".", "\".\""))).collect())
                .unwrap_or(HashSet::new())
    };

    /// `GRAPH_SQL_STATEMENT_TIMEOUT` is the timeout for queries in seconds.
    /// If it is not set, no statement timeout will be enforced. The statement
    /// timeout is local, i.e., can only be used within a transaction and
    /// will be cleared at the end of the transaction
    static ref STATEMENT_TIMEOUT: Option<String> = {
        env::var("GRAPH_SQL_STATEMENT_TIMEOUT")
        .ok()
        .map(|s| {
            u64::from_str(&s).unwrap_or_else(|_| {
                panic!("GRAPH_SQL_STATEMENT_TIMEOUT must be a number, but is `{}`", s)
            })
        }).map(|timeout| format!("set local statement_timeout={}", timeout * 1000))
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

    pub fn qualified_name(namespace: &Namespace, name: &SqlName) -> Self {
        SqlName(format!("\"{}\".\"{}\"", namespace, name.as_str()))
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
            _ => Err(anyhow!(
                "The `id` field has type `{}` but only `String`, `Bytes`, and `ID` are allowed",
                &name
            )
            .into()),
        }
    }
}

type IdTypeMap = HashMap<EntityType, IdType>;

type EnumMap = BTreeMap<String, Arc<BTreeSet<String>>>;

#[derive(Debug, Clone)]
pub struct Layout {
    /// Details of where the subgraph is stored
    pub site: Arc<Site>,
    /// Maps the GraphQL name of a type to the relational table
    pub tables: HashMap<EntityType, Arc<Table>>,
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
        site: Arc<Site>,
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
                        Err(anyhow!(
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
            .map(|obj_type| IdType::try_from(*obj_type).map(|t| (EntityType::from(*obj_type), t)))
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
                    Schema::entity_fulltext_definitions(&obj_type.name, &schema.document)
                        .map_err(|_| StoreError::FulltextSearchNonDeterministic)?,
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
                    "select count(*) from \"{}\".\"{}\" where block_range @> {}",
                    &catalog.site.namespace, table.name, BLOCK_NUMBER_MAX
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
            site,
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
            qualified_name: SqlName::qualified_name(&catalog.site.namespace, &table_name),
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

    pub fn supports_proof_of_indexing(&self) -> bool {
        self.tables.contains_key(&*POI_OBJECT)
    }

    pub fn create_relational_schema(
        conn: &PgConnection,
        site: Arc<Site>,
        schema: &Schema,
    ) -> Result<Layout, StoreError> {
        let catalog = Catalog::new(conn, site.clone())?;
        let layout = Self::new(site, schema, catalog, true)?;
        let sql = layout
            .as_ddl()
            .map_err(|_| StoreError::Unknown(anyhow!("failed to generate DDL for layout")))?;
        conn.batch_execute(&sql)?;
        Ok(layout)
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

    fn write_enum_ddl(&self, out: &mut dyn Write) -> Result<(), fmt::Error> {
        for (name, values) in &self.enums {
            let mut sep = "";
            let name = SqlName::from(name.as_str());
            write!(
                out,
                "create type {}.{}\n    as enum (",
                self.catalog.site.namespace,
                name.quoted()
            )?;
            for value in values.iter() {
                write!(out, "{}'{}'", sep, value)?;
                sep = ", "
            }
            writeln!(out, ");")?;
        }
        Ok(())
    }

    /// Generate the DDL for the entire layout, i.e., all `create table`
    /// and `create index` etc. statements needed in the database schema
    ///
    /// See the unit tests at the end of this file for the actual DDL that
    /// gets generated
    pub fn as_ddl(&self) -> Result<String, fmt::Error> {
        let mut out = String::new();

        // Output enums first so table definitions can reference them
        self.write_enum_ddl(&mut out)?;

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

    /// Import the database schema for this layout from its own database
    /// shard (in `self.site.shard`) into the database represented by `conn`
    /// if the schema for this layout does not exist yet
    pub fn import_schema(&self, conn: &PgConnection) -> Result<(), StoreError> {
        let make_query = || -> Result<String, fmt::Error> {
            let nsp = self.site.namespace.as_str();
            let srvname = ForeignServer::name(&self.site.shard);

            let mut query = String::new();
            writeln!(query, "create schema {};", nsp)?;
            // Postgres does not import enums. We recreate them in the target
            // database, otherwise importing tables that use them fails
            self.write_enum_ddl(&mut query)?;
            writeln!(
                query,
                "import foreign schema {nsp} from server {srvname} into {nsp}",
                nsp = nsp,
                srvname = srvname
            )?;
            Ok(query)
        };

        if !catalog::has_namespace(conn, &self.site.namespace)? {
            let query = make_query().map_err(|_| {
                StoreError::Unknown(anyhow!(
                    "failed to generate SQL to import foreign schema {}",
                    self.site.namespace
                ))
            })?;

            conn.batch_execute(&query)?;
        }
        Ok(())
    }

    /// Find the table with the provided `name`. The name must exactly match
    /// the name of an existing table. No conversions of the name are done
    pub fn table(&self, name: &SqlName) -> Option<&Table> {
        self.tables
            .values()
            .find(|table| &table.name == name)
            .map(|rc| rc.as_ref())
    }

    pub fn table_for_entity(&self, entity: &EntityType) -> Result<&Arc<Table>, StoreError> {
        self.tables
            .get(entity)
            .ok_or_else(|| StoreError::UnknownTable(entity.to_string()))
    }

    pub fn find(
        &self,
        conn: &PgConnection,
        entity: &EntityType,
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

    pub fn find_many<'a>(
        &self,
        conn: &PgConnection,
        ids_for_type: &BTreeMap<&EntityType, Vec<&str>>,
        block: BlockNumber,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        if ids_for_type.is_empty() {
            return Ok(BTreeMap::new());
        }

        let mut tables = Vec::new();
        for entity_type in ids_for_type.keys() {
            tables.push(self.table_for_entity(entity_type)?.as_ref());
        }
        let query = FindManyQuery {
            _namespace: &self.catalog.site.namespace,
            ids_for_type,
            tables,
            block,
        };
        let mut entities_for_type: BTreeMap<EntityType, Vec<Entity>> = BTreeMap::new();
        for data in query.load::<EntityData>(conn)? {
            entities_for_type
                .entry(data.entity_type())
                .or_default()
                .push(data.deserialize_with_layout(self)?);
        }
        Ok(entities_for_type)
    }

    pub fn insert<'a>(
        &'a self,
        conn: &PgConnection,
        entity_type: &'a EntityType,
        entities: &'a mut [(&'a EntityKey, Cow<'a, Entity>)],
        block: BlockNumber,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(entity_type)?;
        let _section = stopwatch.start_section("insert_modification_insert_query");
        let mut count = 0;
        // Each operation must respect the maximum number of bindings allowed in PostgreSQL queries,
        // so we need to act in chunks whose size is defined by the number of entities times the
        // number of attributes each entity type has.
        // We add 1 to account for the `block_range` bind parameter
        let chunk_size = POSTGRES_MAX_PARAMETERS / (table.columns.len() + 1);
        for chunk in entities.chunks_mut(chunk_size) {
            count += InsertQuery::new(table, chunk, block)?
                .get_results(conn)
                .map(|ids| ids.len())?
        }
        Ok(count)
    }

    pub fn conflicting_entity(
        &self,
        conn: &PgConnection,
        entity_id: &str,
        entities: Vec<EntityType>,
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

            let mut text = debug_query(&query).to_string().replace("\n", "\t");
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
        let values = conn
            .transaction(|| {
                if let Some(ref timeout_sql) = *STATEMENT_TIMEOUT {
                    conn.batch_execute(timeout_sql)?;
                }
                query.load::<EntityData>(conn)
            })
            .map_err(|e| match e {
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::__Unknown,
                    ref info,
                ) if info.message().starts_with("syntax error in tsquery") => {
                    QueryExecutionError::FulltextQueryInvalidSyntax(info.message().to_string())
                }
                _ => QueryExecutionError::ResolveEntitiesError(format!(
                    "{}, query = {:?}",
                    e,
                    debug_query(&query_clone).to_string()
                )),
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

    pub fn update<'a>(
        &'a self,
        conn: &PgConnection,
        entity_type: &'a EntityType,
        entities: &'a mut [(&'a EntityKey, Cow<'a, Entity>)],
        block: BlockNumber,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&entity_type)?;
        let entity_keys: Vec<&str> = entities
            .iter()
            .map(|(key, _)| key.entity_id.as_str())
            .collect();

        let section = stopwatch.start_section("update_modification_clamp_range_query");
        ClampRangeQuery::new(table, &entity_type, &entity_keys, block).execute(conn)?;
        section.end();

        let _section = stopwatch.start_section("update_modification_insert_query");
        let mut count = 0;

        // Each operation must respect the maximum number of bindings allowed in PostgreSQL queries,
        // so we need to act in chunks whose size is defined by the number of entities times the
        // number of attributes each entity type has.
        // We add 1 to account for the `block_range` bind parameter
        let chunk_size = POSTGRES_MAX_PARAMETERS / (table.columns.len() + 1);
        for chunk in entities.chunks_mut(chunk_size) {
            count += InsertQuery::new(table, chunk, block)?.execute(conn)?;
        }
        Ok(count)
    }

    pub fn delete(
        &self,
        conn: &PgConnection,
        entity_type: &EntityType,
        entity_ids: &[&str],
        block: BlockNumber,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&entity_type)?;
        let _section = stopwatch.start_section("delete_modification_clamp_range_query");
        let mut count = 0;
        for chunk in entity_ids.chunks(DELETE_OPERATION_CHUNK_SIZE) {
            count += ClampRangeQuery::new(table, &entity_type, chunk, block).execute(conn)?
        }
        Ok(count)
    }

    pub fn revert_block(
        &self,
        conn: &PgConnection,
        subgraph_id: &DeploymentHash,
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
                .map(|_| EntityChange::Data {
                    subgraph_id: subgraph_id.clone(),
                    entity_type: table.object.clone(),
                });
            changes.extend(deleted);
            // EntityChange for versions that we just updated or inserted
            let set = unclamped.into_iter().map(|_| EntityChange::Data {
                subgraph_id: subgraph_id.clone(),
                entity_type: table.object.clone(),
            });
            changes.extend(set);
        }
        Ok((StoreEvent::new(changes), count))
    }

    /// Revert the metadata (dynamic data sources and related entities) for
    /// the given `subgraph`.
    ///
    /// For metadata, reversion always means deletion since the metadata that
    /// is subject to reversion is only ever created but never updated
    pub fn revert_metadata(
        conn: &PgConnection,
        subgraph: &DeploymentHash,
        block: BlockNumber,
    ) -> Result<(), StoreError> {
        crate::dynds::revert(conn, &subgraph, block)?;
        crate::deployment::revert_subgraph_errors(conn, &subgraph, block)?;

        Ok(())
    }

    pub fn is_cacheable(&self) -> bool {
        // This would be false if we still needed to migrate the Layout, but
        // since there are no migrations in the code right now, it is always
        // safe to cache a Layout
        true
    }

    /// Update the layout with the latest information from the database; for
    /// now, an update only changes the `is_account_like` flag for tables or
    /// the layout's site. If no update is needed, just return `self`.
    pub fn refresh(
        self: Arc<Self>,
        conn: &PgConnection,
        site: Arc<Site>,
    ) -> Result<Arc<Self>, StoreError> {
        let account_like = crate::catalog::account_like(conn, &self.site)?;
        let is_account_like = {
            |table: &Table| {
                ACCOUNT_TABLES.contains(table.qualified_name.as_str())
                    || account_like.contains(table.name.as_str())
            }
        };

        let changed_tables: Vec<_> = self
            .tables
            .values()
            .filter(|table| table.is_account_like != is_account_like(table.as_ref()))
            .collect();
        if changed_tables.is_empty() && site == self.site {
            return Ok(self);
        }
        let mut layout = (*self).clone();
        for table in changed_tables.into_iter() {
            let mut table = (*table.as_ref()).clone();
            table.is_account_like = is_account_like(&table);
            layout.tables.insert(table.object.clone(), Arc::new(table));
        }
        layout.site = site;
        Ok(Arc::new(layout))
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
        if let Some(id_type) = id_types.get(&EntityType::new(name.to_string())) {
            return Ok((*id_type).into());
        }

        // Check if it's an enum, and if it is, return an appropriate
        // ColumnType::Enum
        if let Some(values) = enums.get(&*name) {
            // We do things this convoluted way to make sure field_type gets
            // snakecased, but the `.` must stay a `.`
            let name = SqlName::qualified_name(&catalog.site.namespace, &SqlName::from(name));
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
        self.field_type.is_list()
    }

    pub fn is_enum(&self) -> bool {
        matches!(self.column_type, ColumnType::Enum(_))
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

    pub fn is_assignable_from(&self, source: &Self, object: &EntityType) -> Option<String> {
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

#[derive(Debug, Clone)]
pub struct Table {
    /// The name of the GraphQL object type ('Thing')
    pub object: EntityType,
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
            .filter(|field| !field.is_derived())
            .map(|field| Column::new(&table_name, field, catalog, enums, id_types))
            .chain(fulltexts.iter().map(|def| Column::new_fulltext(def)))
            .collect::<Result<Vec<Column>, StoreError>>()?;
        let qualified_name = SqlName::qualified_name(&catalog.site.namespace, &table_name);
        let is_account_like = ACCOUNT_TABLES.contains(qualified_name.as_str());
        let table = Table {
            object: EntityType::from(defn),
            name: table_name.clone(),
            qualified_name,
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
            layout.catalog.site.namespace,
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
            schema_name = layout.catalog.site.namespace,
            block_max = BLOCK_NUMBER_MAX)?;

        // Add a BTree index that helps with the `RevertClampQuery` by making
        // it faster to find entity versions that have been modified
        write!(
            out,
            "create index {table_name}_block_range_closed\n    \
                     on {schema_name}.{table_name}(coalesce(upper(block_range), {block_max}))\n \
                     where coalesce(upper(block_range), {block_max}) < {block_max};\n",
            table_name = self.name,
            schema_name = layout.catalog.site.namespace,
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
                schema_name = layout.catalog.site.namespace,
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

fn is_object_type(field_type: &q::Type, enums: &EnumMap) -> bool {
    let name = named_type(field_type);

    !enums.contains_key(&*name) && !ValueType::is_scalar(name)
}

#[derive(Clone)]
struct CacheEntry {
    value: Arc<Layout>,
    expires: Instant,
}

/// Cache layouts for some time and refresh them when they expire.
/// Refreshing happens one at a time, and the cache makes sure we minimize
/// blocking while a refresh happens, favoring using an expired layout over
/// a refreshed one.
pub struct LayoutCache {
    entries: Mutex<HashMap<DeploymentHash, CacheEntry>>,
    ttl: Duration,
    /// Use this so that we only refresh one layout at any given time to
    /// avoid refreshing the same layout multiple times
    refresh: Mutex<()>,
}

impl LayoutCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
            refresh: Mutex::new(()),
        }
    }

    fn load(conn: &PgConnection, site: Arc<Site>) -> Result<Arc<Layout>, StoreError> {
        let subgraph_schema = deployment::schema(conn, site.as_ref())?;
        let has_poi = crate::catalog::supports_proof_of_indexing(conn, &site.namespace)?;
        let catalog = Catalog::new(conn, site.clone())?;
        let layout = Arc::new(Layout::new(
            site.clone(),
            &subgraph_schema,
            catalog,
            has_poi,
        )?);
        layout.refresh(conn, site)
    }

    fn cache(&self, layout: Arc<Layout>) {
        if layout.is_cacheable() {
            let deployment = layout.site.deployment.clone();
            let entry = CacheEntry {
                expires: Instant::now() + self.ttl,
                value: layout,
            };
            self.entries.lock().unwrap().insert(deployment, entry);
        }
    }

    /// Return the corresponding layout if we have one in cache already, and
    /// ignore expiration information
    pub(crate) fn find(&self, site: &Site) -> Option<Arc<Layout>> {
        self.entries
            .lock()
            .unwrap()
            .get(&site.deployment)
            .map(|CacheEntry { value, expires: _ }| value.clone())
    }

    /// Get the layout for `site`. If it's not in cache, load it. If it is
    /// expired, try to refresh it if there isn't another refresh happening
    /// already
    pub fn get(
        &self,
        logger: &Logger,
        conn: &PgConnection,
        site: Arc<Site>,
    ) -> Result<Arc<Layout>, StoreError> {
        let now = Instant::now();
        let entry = {
            let lock = self.entries.lock().unwrap();
            lock.get(&site.deployment).cloned()
        };
        match entry {
            Some(CacheEntry { value, expires }) => {
                if now <= expires {
                    // Entry is not expired; use it
                    Ok(value)
                } else {
                    // Only do a cache refresh once; we don't want to have
                    // multiple threads refreshing the same layout
                    // simultaneously. It's easiest to refresh at most one
                    // layout globally
                    let refresh = self.refresh.try_lock();
                    if let Err(_) = refresh {
                        return Ok(value.clone());
                    }
                    match value.cheap_clone().refresh(conn, site) {
                        Err(e) => {
                            warn!(
                                logger,
                                "failed to refresh statistics. Continuing with old statistics";
                                "deployment" => &value.site.deployment,
                                "error" => e.to_string()
                            );
                            // Update the timestamp so we don't retry
                            // refreshing too often
                            self.cache(value.cheap_clone());
                            Ok(value)
                        }
                        Ok(layout) => {
                            self.cache(layout.cheap_clone());
                            Ok(layout)
                        }
                    }
                }
            }
            None => {
                let layout = Self::load(conn, site)?;
                self.cache(layout.cheap_clone());
                Ok(layout)
            }
        }
    }

    // Only needed for tests
    #[cfg(debug_assertions)]
    pub(crate) fn clear(&self) {
        self.entries.lock().unwrap().clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::layout_for_tests::make_dummy_site;

    const ID_TYPE: ColumnType = ColumnType::String;

    fn test_layout(gql: &str) -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = Schema::parse(gql, subgraph.clone()).expect("Test schema invalid");
        let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let catalog = Catalog::make_empty(site.clone()).expect("Can not create catalog");
        Layout::new(site, &schema, catalog, false).expect("Failed to construct Layout")
    }

    #[test]
    fn table_is_sane() {
        let layout = test_layout(THING_GQL);
        let table = layout
            .table(&"thing".into())
            .expect("failed to get 'thing' table");
        assert_eq!(SqlName::from("thing"), table.name);
        assert_eq!("Thing", table.object.as_str());

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

    const THING_DDL: &str = "create type sgd0815.\"color\"
    as enum ('BLUE', 'red', 'yellow');
create type sgd0815.\"size\"
    as enum (\'large\', \'medium\', \'small\');
create table sgd0815.\"thing\" (
        \"id\"                 text not null,
        \"big_thing\"          text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_thing
    on sgd0815.thing
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index thing_block_range_closed
    on sgd0815.thing(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_thing_id
    on sgd0815.\"thing\" using btree(\"id\");
create index attr_0_1_thing_big_thing
    on sgd0815.\"thing\" using gist(\"big_thing\", block_range);

create table sgd0815.\"scalar\" (
        \"id\"                 text not null,
        \"bool\"               boolean,
        \"int\"                integer,
        \"big_decimal\"        numeric,
        \"string\"             text,
        \"bytes\"              bytea,
        \"big_int\"            numeric,
        \"color\"              \"sgd0815\".\"color\",

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_scalar
    on sgd0815.scalar
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index scalar_block_range_closed
    on sgd0815.scalar(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_scalar_id
    on sgd0815.\"scalar\" using btree(\"id\");
create index attr_1_1_scalar_bool
    on sgd0815.\"scalar\" using btree(\"bool\");
create index attr_1_2_scalar_int
    on sgd0815.\"scalar\" using btree(\"int\");
create index attr_1_3_scalar_big_decimal
    on sgd0815.\"scalar\" using btree(\"big_decimal\");
create index attr_1_4_scalar_string
    on sgd0815.\"scalar\" using btree(left(\"string\", 256));
create index attr_1_5_scalar_bytes
    on sgd0815.\"scalar\" using btree(\"bytes\");
create index attr_1_6_scalar_big_int
    on sgd0815.\"scalar\" using btree(\"big_int\");
create index attr_1_7_scalar_color
    on sgd0815.\"scalar\" using btree(\"color\");

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
    const MUSIC_DDL: &str = "create table sgd0815.\"musician\" (
        \"id\"                 text not null,
        \"name\"               text not null,
        \"main_band\"          text,
        \"bands\"              text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_musician
    on sgd0815.musician
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index musician_block_range_closed
    on sgd0815.musician(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_musician_id
    on sgd0815.\"musician\" using btree(\"id\");
create index attr_0_1_musician_name
    on sgd0815.\"musician\" using btree(left(\"name\", 256));
create index attr_0_2_musician_main_band
    on sgd0815.\"musician\" using gist(\"main_band\", block_range);
create index attr_0_3_musician_bands
    on sgd0815.\"musician\" using gin(\"bands\");

create table sgd0815.\"band\" (
        \"id\"                 text not null,
        \"name\"               text not null,
        \"original_songs\"     text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_band
    on sgd0815.band
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index band_block_range_closed
    on sgd0815.band(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_band_id
    on sgd0815.\"band\" using btree(\"id\");
create index attr_1_1_band_name
    on sgd0815.\"band\" using btree(left(\"name\", 256));
create index attr_1_2_band_original_songs
    on sgd0815.\"band\" using gin(\"original_songs\");

create table sgd0815.\"song\" (
        \"id\"                 text not null,
        \"title\"              text not null,
        \"written_by\"         text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_song
    on sgd0815.song
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index song_block_range_closed
    on sgd0815.song(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_song_id
    on sgd0815.\"song\" using btree(\"id\");
create index attr_2_1_song_title
    on sgd0815.\"song\" using btree(left(\"title\", 256));
create index attr_2_2_song_written_by
    on sgd0815.\"song\" using gist(\"written_by\", block_range);

create table sgd0815.\"song_stat\" (
        \"id\"                 text not null,
        \"played\"             integer not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_song_stat
    on sgd0815.song_stat
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index song_stat_block_range_closed
    on sgd0815.song_stat(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_3_0_song_stat_id
    on sgd0815.\"song_stat\" using btree(\"id\");
create index attr_3_1_song_stat_played
    on sgd0815.\"song_stat\" using btree(\"played\");

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

    const FOREST_DDL: &str = "create table sgd0815.\"animal\" (
        \"id\"                 text not null,
        \"forest\"             text,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_animal
    on sgd0815.animal
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index animal_block_range_closed
    on sgd0815.animal(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_animal_id
    on sgd0815.\"animal\" using btree(\"id\");
create index attr_0_1_animal_forest
    on sgd0815.\"animal\" using gist(\"forest\", block_range);

create table sgd0815.\"forest\" (
        \"id\"                 text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_forest
    on sgd0815.forest
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index forest_block_range_closed
    on sgd0815.forest(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_forest_id
    on sgd0815.\"forest\" using btree(\"id\");

create table sgd0815.\"habitat\" (
        \"id\"                 text not null,
        \"most_common\"        text not null,
        \"dwellers\"           text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_habitat
    on sgd0815.habitat
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index habitat_block_range_closed
    on sgd0815.habitat(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_habitat_id
    on sgd0815.\"habitat\" using btree(\"id\");
create index attr_2_1_habitat_most_common
    on sgd0815.\"habitat\" using gist(\"most_common\", block_range);
create index attr_2_2_habitat_dwellers
    on sgd0815.\"habitat\" using gin(\"dwellers\");

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

    const FULLTEXT_DDL: &str = "create table sgd0815.\"animal\" (
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
    on sgd0815.animal
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index animal_block_range_closed
    on sgd0815.animal(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_animal_id
    on sgd0815.\"animal\" using btree(\"id\");
create index attr_0_1_animal_name
    on sgd0815.\"animal\" using btree(left(\"name\", 256));
create index attr_0_2_animal_species
    on sgd0815.\"animal\" using btree(left(\"species\", 256));
create index attr_0_3_animal_forest
    on sgd0815.\"animal\" using gist(\"forest\", block_range);
create index attr_0_4_animal_search
    on sgd0815.\"animal\" using gin(\"search\");

create table sgd0815.\"forest\" (
        \"id\"                 text not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_forest
    on sgd0815.forest
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index forest_block_range_closed
    on sgd0815.forest(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_forest_id
    on sgd0815.\"forest\" using btree(\"id\");

create table sgd0815.\"habitat\" (
        \"id\"                 text not null,
        \"most_common\"        text not null,
        \"dwellers\"           text[] not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_habitat
    on sgd0815.habitat
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index habitat_block_range_closed
    on sgd0815.habitat(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_habitat_id
    on sgd0815.\"habitat\" using btree(\"id\");
create index attr_2_1_habitat_most_common
    on sgd0815.\"habitat\" using gist(\"most_common\", block_range);
create index attr_2_2_habitat_dwellers
    on sgd0815.\"habitat\" using gin(\"dwellers\");

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

    const FORWARD_ENUM_SQL: &str = "create type sgd0815.\"orientation\"
    as enum (\'DOWN\', \'UP\');
create table sgd0815.\"thing\" (
        \"id\"                 text not null,
        \"orientation\"        \"sgd0815\".\"orientation\" not null,

        vid                  bigserial primary key,
        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index brin_thing
    on sgd0815.thing
 using brin(lower(block_range), coalesce(upper(block_range), 2147483647), vid);
create index thing_block_range_closed
    on sgd0815.thing(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_thing_id
    on sgd0815.\"thing\" using btree(\"id\");
create index attr_0_1_thing_orientation
    on sgd0815.\"thing\" using btree(\"orientation\");

";
}
