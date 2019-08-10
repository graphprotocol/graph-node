use graphql_parser::query as q;
use graphql_parser::schema as s;
use inflector::Inflector;
use std::cmp::PartialOrd;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::str::FromStr;

use graph::prelude::{format_err, StoreError, ValueType};

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

impl IdType {
    fn sql_type(&self) -> &'static str {
        match self {
            IdType::String => "text",
            IdType::Bytes => "bytea",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Mapping {
    /// The SQL type for columns with GraphQL type `ID`
    id_type: IdType,
    /// Maps the GraphQL name of a type to the relational table
    tables: HashMap<String, Rc<Table>>,
    /// A fake table that mirrors the Query type for the schema
    root_table: Table,
    /// The subgraph id
    pub subgraph: String,
    /// The database schema for this subgraph
    pub schema: String,
    /// Map the entity names of interfaces to the list of
    /// database tables that contain entities implementing
    /// that interface
    pub interfaces: HashMap<String, Vec<Rc<Table>>>,
}

impl Mapping {
    /// Generate a mapping for a relational schema for entities in the
    /// GraphQL schema `document`. Attributes of type `ID` will use the
    /// SQL type `id_type`. The subgraph ID is passed in `subgraph`, and
    /// the name of the database schema in which the subgraph's tables live
    /// is in `schema`.
    pub fn new<U, V>(
        document: &s::Document,
        id_type: IdType,
        subgraph: U,
        schema: V,
    ) -> Result<Mapping, StoreError>
    where
        U: Into<String>,
        V: Into<String>,
    {
        use s::Definition::*;
        use s::TypeDefinition::*;

        let subgraph = subgraph.into();
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

        for defn in &document.definitions {
            match defn {
                TypeDefinition(Object(obj_type)) => {
                    let table = Table::new(obj_type, &mut interfaces)?;
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
                if scalar_sql_type(&column.field_type, id_type).is_some() {
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

        let root_table = Mapping::make_root_table(&tables);

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
        })
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

    #[allow(dead_code)]
    pub fn column(&self, reference: &Reference) -> Result<&Column, StoreError> {
        self.table(&reference.table)?.field(&reference.column)
    }

    /// Construct a fake root table that has an attribute for each table
    /// we actually support
    fn make_root_table(tables: &Vec<Table>) -> Table {
        let mut columns = Vec::new();

        for table in tables {
            let objects = Column {
                name: table.name.clone(),
                field: table.object.clone(),
                field_type: q::Type::NamedType(table.object.clone()),
                derived: None,
                references: vec![table.primary_key()],
            };
            columns.push(objects);

            let object = Column {
                name: SqlName::from(&*table.singular_name),
                field: table.object.clone(),
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
        }
    }

    #[allow(dead_code)]
    pub fn root_table(&self) -> &Table {
        &self.root_table
    }
}

impl AsDdl for Mapping {
    fn fmt(&self, f: &mut dyn fmt::Write, mapping: &Mapping) -> fmt::Result {
        // We sort tables here solely because the unit tests rely on
        // 'create table' statements appearing in a fixed order
        let mut tables = self.tables.values().collect::<Vec<_>>();
        tables.sort_by(|a, b| (&a.name).partial_cmp(&b.name).unwrap());
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

#[derive(Clone, Debug)]
pub struct Column {
    pub name: SqlName,
    pub field: String,
    pub field_type: q::Type,
    pub derived: Option<String>,
    pub references: Vec<Reference>,
}

#[allow(dead_code)]
impl Column {
    fn new(field: &s::Field) -> Result<Column, StoreError> {
        let sql_name = SqlName::from(&*field.name);
        Ok(Column {
            name: sql_name,
            field: field.name.clone(),
            field_type: field.field_type.clone(),
            derived: derived_column(field)?,
            references: vec![],
        })
    }

    pub fn is_reference(&self) -> bool {
        !self.references.is_empty()
    }

    fn sql_type(&self, id_type: IdType) -> &str {
        match base_type(&self.field_type) {
            ValueType::Boolean => "boolean",
            ValueType::BigDecimal => "numeric",
            ValueType::BigInt => "numeric",
            ValueType::Bytes => "bytea",
            ValueType::ID => id_type.sql_type(),
            ValueType::Int => "integer",
            ValueType::String => "text",
            ValueType::List => unreachable!(),
        }
    }
}

impl AsDdl for Column {
    fn fmt(&self, f: &mut dyn fmt::Write, mapping: &Mapping) -> fmt::Result {
        write!(f, "    ")?;
        if self.derived.is_some() {
            write!(f, "-- ")?;
        }
        write!(f, "{:20} {}", self.name, self.sql_type(mapping.id_type))?;
        if is_list(&self.field_type) {
            write!(f, "[]")?;
        }
        if self.name.0 == PRIMARY_KEY_COLUMN {
            write!(f, " primary key")?;
        } else if !is_nullable(&self.field_type) {
            write!(f, " not null")?;
        }
        Ok(())
    }
}

/// The name for the primary key column of a table; hardcoded for now
const PRIMARY_KEY_COLUMN: &str = "id";

#[allow(dead_code)]
pub(crate) fn is_primary_key_column(col: &SqlName) -> bool {
    PRIMARY_KEY_COLUMN == col.0
}

#[derive(Clone, Debug)]
pub struct Table {
    /// The name of the GraphQL object type ('Thing')
    pub object: s::Name,
    /// The name of the database table for this type ('things')
    pub name: SqlName,
    /// The name for a single object of elements of this type in GraphQL
    /// queries ('thing')
    singular_name: String,

    columns: Vec<Column>,
}

impl Table {
    fn new(
        defn: &s::ObjectType,
        interfaces: &mut HashMap<String, Vec<SqlName>>,
    ) -> Result<Table, StoreError> {
        let table_name = Table::collection_name(&defn.name);
        let columns = defn
            .fields
            .iter()
            .map(|field| Column::new(field))
            .collect::<Result<Vec<_>, _>>()?;
        let table = Table {
            object: defn.name.clone(),
            name: table_name.clone(),
            singular_name: Table::object_name(&defn.name),
            columns,
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
    /// Find the field `name` in this table. The name must be in snake case,
    /// i.e., use SQL conventions
    pub fn field(&self, name: &SqlName) -> Result<&Column, StoreError> {
        self.columns
            .iter()
            .find(|column| &column.name == name)
            .ok_or_else(|| StoreError::UnknownField(name.to_string()))
    }

    pub fn primary_key(&self) -> Reference {
        Reference {
            table: self.name.clone(),
            column: PRIMARY_KEY_COLUMN.into(),
        }
    }

    #[allow(dead_code)]
    pub fn reference(&self, name: &SqlName) -> Result<Reference, StoreError> {
        let attr = self.field(name)?;
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
}

impl AsDdl for Table {
    fn fmt(&self, f: &mut dyn fmt::Write, mapping: &Mapping) -> fmt::Result {
        write!(f, "create table {}.{} (\n", mapping.schema, self.name)?;
        let mut first = true;
        for column in self.columns.iter().filter(|col| col.derived.is_none()) {
            if !first {
                write!(f, ",\n")?;
            }
            first = false;
            write!(f, "    ")?;
            column.fmt(f, mapping)?;
        }
        if self.columns.iter().any(|col| col.derived.is_some()) {
            write!(f, "\n     -- derived fields (not stored in this table)")?;
            for column in self.columns.iter().filter(|col| col.derived.is_some()) {
                write!(f, "\n ")?;
                column.fmt(f, mapping)?;
                for reference in &column.references {
                    write!(f, " references {}({})", reference.table, reference.column)?;
                }
            }
        }
        write!(f, "\n);\n")
    }
}

fn is_nullable(field_type: &q::Type) -> bool {
    match field_type {
        q::Type::NonNullType(_) => false,
        _ => true,
    }
}

fn is_list(field_type: &q::Type) -> bool {
    use q::Type::*;

    match field_type {
        ListType(_) => true,
        NonNullType(inner) => is_list(inner),
        NamedType(_) => false,
    }
}

/// Return the SQL type that corresponds to the scalar GraphQL type
/// `name`. If `name` does not denote a known SQL type, return `None`
fn scalar_sql_type(field_type: &q::Type, id_type: IdType) -> Option<&str> {
    // This is pretty adhoc, and solely based on the example schema
    match field_type {
        q::Type::NamedType(name) => match name.as_str() {
            "Boolean" => Some("boolean"),
            "BigDecimal" => Some("numeric"),
            "BigInt" => Some("numeric"),
            "Bytes" => Some("bytea"),
            "ID" => Some(id_type.sql_type()),
            "Int" => Some("integer"),
            "String" => Some("text"),
            _ => None,
        },
        q::Type::ListType(inner) => scalar_sql_type(inner, id_type),
        q::Type::NonNullType(inner) => scalar_sql_type(inner, id_type),
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

    const ID_TYPE: IdType = IdType::String;

    fn test_mapping(gql: &str) -> Mapping {
        let schema = parse_schema(gql).expect("Test schema invalid");

        Mapping::new(&schema, ID_TYPE, "subgraph", "rel").expect("Failed to construct Mapping")
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
            .field(&PRIMARY_KEY_COLUMN.into())
            .expect("failed to get 'id' column for 'things' table");
        assert_eq!(ID_TYPE.sql_type(), id.sql_type(mapping.id_type));
        assert!(!is_nullable(&id.field_type));
        assert!(!is_list(&id.field_type));
        assert!(id.derived.is_none());
        assert!(!id.is_reference());

        let big_thing = table
            .field(&"big_thing".into())
            .expect("failed to get 'big_thing' column for 'things' table");
        assert_eq!(ID_TYPE.sql_type(), big_thing.sql_type(mapping.id_type));
        assert!(!is_nullable(&big_thing.field_type));
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
        assert!(table.field(&bad_sql_name).is_err());
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

    const THINGS_DDL: &str = "create table rel.scalars (
        id                   text primary key,
        bool                 boolean,
        int                  integer,
        big_decimal          numeric,
        string               text,
        bytes                bytea,
        big_int              numeric
);
create table rel.things (
        id                   text primary key,
        big_thing            text not null
);
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
    const MUSIC_DDL: &str = "create table rel.bands (
        id                   text primary key,
        name                 text not null,
        original_songs       text[] not null
     -- derived fields (not stored in this table)
     -- members              text[] not null references musicians(bands)
);
create table rel.musicians (
        id                   text primary key,
        name                 text not null,
        main_band            text,
        bands                text[] not null
     -- derived fields (not stored in this table)
     -- written_songs        text[] not null references songs(written_by)
);
create table rel.song_stats (
        id                   text primary key,
        played               integer not null
     -- derived fields (not stored in this table)
     -- song                 text references songs(id)
);
create table rel.songs (
        id                   text primary key,
        title                text not null,
        written_by           text not null
     -- derived fields (not stored in this table)
     -- band                 text references bands(original_songs)
);
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
        id                   text primary key,
        forest               text
);
create table rel.forests (
        id                   text primary key
     -- derived fields (not stored in this table)
     -- dwellers             text[] not null references animals(forest)
);
create table rel.habitats (
        id                   text primary key,
        most_common          text not null,
        dwellers             text[] not null
);
";

}
