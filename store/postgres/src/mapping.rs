use diesel::connection::SimpleConnection;
use diesel::{debug_query, PgConnection, RunQueryDsl};
use graphql_parser::query as q;
use graphql_parser::schema as s;
use inflector::Inflector;
use std::cmp::PartialOrd;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::str::FromStr;

use crate::mapping_sql::{
    DeleteQuery, ConflictingEntityQuery, EntityData, FilterQuery, FindQuery, InsertQuery, UpdateQuery,
};
use graph::prelude::{
    format_err, Entity, EntityFilter, EntityKey, QueryExecutionError, StoreError, SubgraphDeploymentId, ValueType,
};

use crate::entities::STRING_PREFIX_SIZE;

trait AsDdl {
    fn fmt(&self, f: &mut dyn fmt::Write, mapping: &Mapping) -> Result<(), fmt::Error>;

    fn as_ddl(&self, mapping: &Mapping) -> Result<String, fmt::Error> {
        let mut output = String::new();
        self.fmt(&mut output, mapping)?;
        Ok(output)
    }
}

/// A string we use as a SQL name for a table or column. The important thing
/// is that SQL names are snake cased. Using this type makes it easier to
/// spot cases where we use a GraphQL name like 'bigThing' when we should
/// really use the SQL version 'big_thing'
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct SqlName(String);

impl SqlName {
    pub fn as_str(&self) -> &str {
        &self.0
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
    /// A fake table that mirrors the Query type for the schema
    root_table: Table,
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
        let mut position = 0;

        for defn in &document.definitions {
            match defn {
                TypeDefinition(Object(obj_type)) => {
                    position += 1;
                    let table = Table::new(obj_type, &mut interfaces, id_type, position)?;
                    tables.push(table);
                }
                TypeDefinition(Interface(interface_type)) => {
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

        // Resolve references
        for table in tables.iter_mut() {
            for column in table.columns.iter_mut() {
                let named_type = named_type(&column.field_type);
                if is_scalar_type(named_type) {
                    // We only care about references
                    continue;
                }

                let references = match interfaces.get(named_type) {
                    Some(table_names) => {
                        // sql_type is an interface; add each table that contains
                        // a type that implements the interface into the references
                        table_names
                            .iter()
                            .map(|table_name| Reference::to_column(table_name.clone(), column))
                            .collect()
                    }
                    None => {
                        // sql_type is an object; add a single reference to the target
                        // table and column
                        let other_table_name = Table::collection_name(named_type);
                        let reference = Reference::to_column(other_table_name, column);
                        vec![reference]
                    }
                };
                column.references = references;
            }
        }

        let root_table = Mapping::make_root_table(&tables, id_type);

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
            .map(|table| format!("select count(*) from \"{}\".\"{}\"", schema, table.name))
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
            root_table,
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
            crate::mapping::Mapping::new(document, IdType::String, subgraph, schema_name)?;
        let sql = mapping
            .as_ddl()
            .map_err(|_| StoreError::Unknown(format_err!("failed to generate DDL for mapping")))?;
        conn.batch_execute(&sql)?;
        Ok(mapping)
    }

    pub fn as_ddl(&self) -> Result<String, fmt::Error> {
        (self as &dyn AsDdl).as_ddl(self)
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

    #[allow(dead_code)]
    pub fn column(&self, reference: &Reference) -> Result<&Column, StoreError> {
        self.table(&reference.table)?.column(&reference.column)
    }

    /// Construct a fake root table that has an attribute for each table
    /// we actually support
    fn make_root_table(tables: &Vec<Table>, id_type: IdType) -> Table {
        let mut columns = Vec::new();

        for table in tables {
            let objects = Column {
                name: table.name.clone(),
                field: table.object.clone(),
                column_type: ColumnType::from(id_type),
                field_type: q::Type::NamedType(table.object.clone()),
                derived: None,
                references: vec![table.primary_key()],
            };
            columns.push(objects);

            let object = Column {
                name: SqlName::from(&*table.singular_name),
                field: table.object.clone(),
                column_type: ColumnType::from(id_type),
                field_type: q::Type::NamedType(table.object.clone()),
                derived: None,
                references: vec![table.primary_key()],
            };
            columns.push(object);
        }
        Table {
            object: "$Root".to_owned(),
            name: "$roots".into(),
            singular_name: "$root".to_owned(),
            columns,
            position: 0,
        }
    }

    #[allow(dead_code)]
    pub fn root_table(&self) -> &Table {
        &self.root_table
    }

    pub fn find(
        &self,
        conn: &PgConnection,
        entity: &str,
        id: &str,
    ) -> Result<Option<Entity>, StoreError> {
        let mut values = FindQuery::new(self, entity, id).load::<EntityData>(conn)?;
        values
            .pop()
            .map(|entity_data| entity_data.to_entity(self))
            .transpose()
    }

    pub fn insert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: Entity,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        let query = InsertQuery::new(&self.schema, table, key, entity);
        Ok(query.execute(conn)?)
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

        let query = FilterQuery::new(&self.schema, tables, filter, order, first, skip);
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
        entity: Entity,
        overwrite: bool,
        guard: Option<EntityFilter>,
    ) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        let query = UpdateQuery::new(&self.schema, table, key, entity, overwrite, guard);
        Ok(query.execute(conn)?)
    }

    pub fn delete(&self, conn: &PgConnection, key: &EntityKey) -> Result<usize, StoreError> {
        let table = self.table_for_entity(&key.entity_type)?;
        let query = DeleteQuery::new(&self.schema, table, key);
        Ok(query.execute(conn)?)
    }
}

impl AsDdl for Mapping {
    fn fmt(&self, f: &mut dyn fmt::Write, mapping: &Mapping) -> fmt::Result {
        // We sort tables here solely because the unit tests rely on
        // 'create table' statements appearing in a fixed order
        let mut tables = self.tables.values().collect::<Vec<_>>();
        tables.sort_by(|a, b| (&a.position).partial_cmp(&b.position).unwrap());
        // Output 'create table' statements for all tables
        for table in tables {
            table.fmt(f, mapping)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Reference {
    pub table: SqlName,
    pub column: SqlName,
}

impl Reference {
    fn to_column(table: SqlName, column: &Column) -> Reference {
        let column = match &column.derived {
            Some(col) => col,
            None => PRIMARY_KEY_COLUMN,
        }
        .into();

        Reference { table, column }
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
    pub derived: Option<String>,
    pub references: Vec<Reference>,
}

#[allow(dead_code)]
impl Column {
    fn new(field: &s::Field, id_type: IdType) -> Result<Column, StoreError> {
        let sql_name = SqlName::from(&*field.name);
        Ok(Column {
            name: sql_name,
            field: field.name.clone(),
            column_type: ColumnType::from_value_type(base_type(&field.field_type), id_type)?,
            field_type: field.field_type.clone(),
            derived: derived_column(field)?,
            references: vec![],
        })
    }

    pub fn is_reference(&self) -> bool {
        !self.references.is_empty()
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

    pub fn is_derived(&self) -> bool {
        self.derived.is_some()
    }
}

impl AsDdl for Column {
    fn fmt(&self, f: &mut dyn fmt::Write, _: &Mapping) -> fmt::Result {
        write!(f, "    ")?;
        if self.derived.is_some() {
            write!(f, " -- ")?;
        }
        write!(f, "{:20} {}", self.name, self.sql_type())?;
        if self.is_list() {
            write!(f, "[]")?;
        }
        if self.name.0 == PRIMARY_KEY_COLUMN || !self.is_nullable() {
            write!(f, " not null")?;
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
    /// The name of the database table for this type ('things')
    pub name: SqlName,
    /// The name for a single object of elements of this type in GraphQL
    /// queries ('thing')
    pub singular_name: String,

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
        let table_name = Table::collection_name(&defn.name);
        let columns = defn
            .fields
            .iter()
            .map(|field| Column::new(field, id_type))
            .collect::<Result<Vec<_>, _>>()?;
        let table = Table {
            object: defn.name.clone(),
            name: table_name.clone(),
            singular_name: Table::object_name(&defn.name),
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

    pub fn primary_key(&self) -> Reference {
        Reference {
            table: self.name.clone(),
            column: PRIMARY_KEY_COLUMN.into(),
        }
    }

    #[allow(dead_code)]
    pub fn reference(&self, name: &SqlName) -> Result<Reference, StoreError> {
        let attr = self.column(name)?;
        Ok(Reference {
            table: self.name.clone(),
            column: attr.name.clone(),
        })
    }

    pub fn collection_name(gql_type_name: &str) -> SqlName {
        SqlName::from(gql_type_name.to_snake_case().to_plural())
    }

    pub fn object_name(gql_type_name: &str) -> String {
        gql_type_name.to_snake_case()
    }

    pub fn insert(
        &self,
        conn: &PgConnection,
        key: &EntityKey,
        entity: Entity,
        schema_name: &str,
    ) -> Result<usize, StoreError> {
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for column in &self.columns {
            if let Some(value) = entity.get(&column.field) {
                columns.push(&column.name);
                values.push(value);
            } else {
                if !column.is_nullable() {
                    return Err(StoreError::Unknown(format_err!(
                        "can not insert entity {}[{}] since value for {} is missing",
                        key.entity_type,
                        key.entity_id,
                        column.field
                    )));
                }
            }
        }
        let query = InsertQuery::new(schema_name, self, key, entity);
        Ok(query.execute(conn)?)
    }
}

impl AsDdl for Table {
    fn fmt(&self, f: &mut dyn fmt::Write, mapping: &Mapping) -> fmt::Result {
        write!(f, "create table {}.{} (\n", mapping.schema, self.name)?;
        for column in self.columns.iter().filter(|col| !col.is_derived()) {
            write!(f, "    ")?;
            column.fmt(f, mapping)?;
            write!(f, ",\n")?;
        }
        // Add block_range column and constraint
        write!(
            f,
            "\n        {block_range}          int4range not null,
        exclude using gist   (id with =, {block_range} with &&)\n",
            block_range = BLOCK_RANGE
        )?;
        if self.columns.iter().any(|col| col.is_derived()) {
            write!(f, "\n     -- derived fields (not stored in this table)\n")?;
            for column in self.columns.iter().filter(|col| col.is_derived()) {
                column.fmt(f, mapping)?;
                for reference in &column.references {
                    write!(f, " references {}({})", reference.table, reference.column)?;
                }
                write!(f, "\n")?;
            }
        }
        write!(f, ");\n")?;

        for (i, column) in self
            .columns
            .iter()
            .filter(|col| !col.is_derived())
            .enumerate()
        {
            if !column.is_primary_key()
                && !column.is_reference()
                && !column.is_list()
                && column.column_type == ColumnType::String
            {
                write!(
                f,
                "create index attr_{table_index}_{column_index}_{table_name}_{column_name}\n    on {schema_name}.{table_name} using btree(left({column_name}, {prefix_size}));\n",
                table_index=self.position,
                table_name=self.name,
                column_index=i + 1,
                column_name=column.name,
                schema_name=mapping.schema,
                prefix_size=STRING_PREFIX_SIZE,
            )?;
            } else {
                let method = if column.is_list() { "gin" } else { "btree" };
                write!(
                f,
                "create index attr_{table_index}_{column_index}_{table_name}_{column_name}\n    on {schema_name}.{table_name} using {method}({column_name});\n",
                table_index=self.position,
                table_name=self.name,
                column_index=i + 1,
                column_name=column.name,
                schema_name=mapping.schema,
                method=method
            )?;
            }
        }
        write!(f, "\n")
    }
}

/// Return `true` if `named_type` is one of the builtin scalar types, i.e.,
/// does not refer to another object
fn is_scalar_type(named_type: &str) -> bool {
    // This is pretty adhoc, and solely based on the example schema
    return named_type == "Boolean"
        || named_type == "BigDecimal"
        || named_type == "BigInt"
        || named_type == "Bytes"
        || named_type == "ID"
        || named_type == "Int"
        || named_type == "String";
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

fn derived_column(field: &s::Field) -> Result<Option<String>, StoreError> {
    match field
        .directives
        .iter()
        .find(|dir| dir.name == s::Name::from("derivedFrom"))
    {
        Some(dir) => Ok(Some(
            dir.arguments
                .iter()
                .find(|arg| arg.0 == "field")
                .map(|arg| {
                    arg.1
                        .to_string()
                        .trim_start_matches('"')
                        .trim_end_matches('"')
                        .to_owned()
                })
                .ok_or(StoreError::MalformedDirective(
                    "derivedFrom requires a 'field' argument".to_owned(),
                ))?,
        )),
        None => Ok(None),
    }
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
        let mapping = test_mapping(THINGS_GQL);
        let table = mapping
            .table(&"things".into())
            .expect("failed to get 'things' table");
        assert_eq!(SqlName::from("things"), table.name);
        assert_eq!("Thing", table.object);
        assert_eq!("thing", table.singular_name);

        let id = table
            .column(&PRIMARY_KEY_COLUMN.into())
            .expect("failed to get 'id' column for 'things' table");
        assert_eq!(ID_TYPE, id.column_type);
        assert!(!id.is_nullable());
        assert!(!id.is_list());
        assert!(id.derived.is_none());
        assert!(!id.is_reference());

        let big_thing = table
            .column(&"big_thing".into())
            .expect("failed to get 'big_thing' column for 'things' table");
        assert_eq!(ID_TYPE, big_thing.column_type);
        assert!(!big_thing.is_nullable());
        assert!(big_thing.derived.is_none());
        assert_eq!(
            vec![Reference {
                table: "things".into(),
                column: PRIMARY_KEY_COLUMN.into()
            }],
            big_thing.references
        );
        // Field lookup happens by the SQL name, not the GraphQL name
        let bad_sql_name = SqlName("bigThing".to_owned());
        assert!(table.column(&bad_sql_name).is_err());
    }

    #[test]
    fn generate_ddl() {
        let mapping = test_mapping(THINGS_GQL);
        let sql = mapping.as_ddl().expect("Failed to generate DDL");
        assert_eq!(THINGS_DDL, sql);

        let mapping = test_mapping(MUSIC_GQL);
        let sql = mapping.as_ddl().expect("Failed to generate DDL");
        assert_eq!(MUSIC_DDL, sql);

        let mapping = test_mapping(FOREST_GQL);
        let sql = mapping.as_ddl().expect("Failed to generate DDL");
        assert_eq!(FOREST_DDL, sql);
    }

    const THINGS_GQL: &str = "
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

    const THINGS_DDL: &str = "create table rel.things (
        id                   text not null,
        big_thing            text not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_1_things_id
    on rel.things using btree(id);
create index attr_1_2_things_big_thing
    on rel.things using btree(big_thing);

create table rel.scalars (
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
create index attr_2_1_scalars_id
    on rel.scalars using btree(id);
create index attr_2_2_scalars_bool
    on rel.scalars using btree(bool);
create index attr_2_3_scalars_int
    on rel.scalars using btree(int);
create index attr_2_4_scalars_big_decimal
    on rel.scalars using btree(big_decimal);
create index attr_2_5_scalars_string
    on rel.scalars using btree(left(string, 2048));
create index attr_2_6_scalars_bytes
    on rel.scalars using btree(bytes);
create index attr_2_7_scalars_big_int
    on rel.scalars using btree(big_int);

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
    const MUSIC_DDL: &str = "create table rel.musicians (
        id                   text not null,
        name                 text not null,
        main_band            text,
        bands                text[] not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)

     -- derived fields (not stored in this table)
     -- written_songs        text[] not null references songs(written_by)
);
create index attr_1_1_musicians_id
    on rel.musicians using btree(id);
create index attr_1_2_musicians_name
    on rel.musicians using btree(left(name, 2048));
create index attr_1_3_musicians_main_band
    on rel.musicians using btree(main_band);
create index attr_1_4_musicians_bands
    on rel.musicians using gin(bands);

create table rel.bands (
        id                   text not null,
        name                 text not null,
        original_songs       text[] not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)

     -- derived fields (not stored in this table)
     -- members              text[] not null references musicians(bands)
);
create index attr_2_1_bands_id
    on rel.bands using btree(id);
create index attr_2_2_bands_name
    on rel.bands using btree(left(name, 2048));
create index attr_2_3_bands_original_songs
    on rel.bands using gin(original_songs);

create table rel.songs (
        id                   text not null,
        title                text not null,
        written_by           text not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)

     -- derived fields (not stored in this table)
     -- band                 text references bands(original_songs)
);
create index attr_3_1_songs_id
    on rel.songs using btree(id);
create index attr_3_2_songs_title
    on rel.songs using btree(left(title, 2048));
create index attr_3_3_songs_written_by
    on rel.songs using btree(written_by);

create table rel.song_stats (
        id                   text not null,
        played               integer not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)

     -- derived fields (not stored in this table)
     -- song                 text references songs(id)
);
create index attr_4_1_song_stats_id
    on rel.song_stats using btree(id);
create index attr_4_2_song_stats_played
    on rel.song_stats using btree(played);

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

    const FOREST_DDL: &str = "create table rel.animals (
        id                   text not null,
        forest               text,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_1_1_animals_id
    on rel.animals using btree(id);
create index attr_1_2_animals_forest
    on rel.animals using btree(forest);

create table rel.forests (
        id                   text not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)

     -- derived fields (not stored in this table)
     -- dwellers             text[] not null references animals(forest)
);
create index attr_2_1_forests_id
    on rel.forests using btree(id);

create table rel.habitats (
        id                   text not null,
        most_common          text not null,
        dwellers             text[] not null,

        block_range          int4range not null,
        exclude using gist   (id with =, block_range with &&)
);
create index attr_3_1_habitats_id
    on rel.habitats using btree(id);
create index attr_3_2_habitats_most_common
    on rel.habitats using btree(most_common);
create index attr_3_3_habitats_dwellers
    on rel.habitats using gin(dwellers);

";

}
