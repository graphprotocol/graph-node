use graphql_parser::query as q;
use graphql_parser::schema as s;
use std::fmt;

use inflector::Inflector;

use graph::prelude::{format_err, StoreError};

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
#[derive(Clone, Debug, PartialEq)]
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

#[derive(Debug)]
pub struct Mapping {
    /// The SQL type for columns with GRaphQL type `ID`
    id_type: IdType,
    /// Maps the GraphQL name of a type to the relational table
    tables: Vec<Table>,
    /// A fake table that mirrors the Query type for the schema
    root_table: Table,
    /// The subgraph id
    pub subgraph: String,
    /// The database schema for this subgraph
    pub schema: String,
    pub interfaces: Vec<Interface>,
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
        let mut mapping = Mapping {
            id_type,
            subgraph,
            schema,
            tables: Vec::new(),
            root_table: Table {
                object: "$Root".to_owned(),
                name: "$roots".into(),
                singular_name: "$root".to_owned(),
                primary_key: "none".into(),
                columns: Vec::new(),
            },
            interfaces: Vec::new(),
        };

        for defn in &document.definitions {
            match defn {
                TypeDefinition(type_def) => match type_def {
                    Object(obj_type) => {
                        mapping.add_table(obj_type)?;
                    }
                    Interface(interface_type) => {
                        mapping.add_interface(interface_type)?;
                    }
                    other => {
                        return Err(StoreError::Unknown(format_err!(
                            "can not handle {:?}",
                            other
                        )))
                    }
                },
                other => {
                    return Err(StoreError::Unknown(format_err!(
                        "can not handle {:?}",
                        other
                    )))
                }
            }
        }
        mapping.resolve_references()?;
        mapping.fill_root_table();
        Ok(mapping)
    }

    pub fn as_ddl(&self) -> Result<String, fmt::Error> {
        (self as &dyn AsDdl).as_ddl(self)
    }

    #[allow(dead_code)]
    pub fn table_for_field(&self, field: &q::Field) -> Option<&Table> {
        // FIXME: We really need to consult GraphQL schema information here
        let name = SqlName::from(&*field.name);
        self.tables
            .iter()
            .find(|table| table.name == name || table.singular_name == field.name)
    }

    /// Find the table with the provided `name`. The name must exactly match
    /// the name of an existing table. No conversions of the name are done
    pub fn table(&self, name: &SqlName) -> Result<&Table, StoreError> {
        self.tables
            .iter()
            .find(|table| &table.name == name)
            .ok_or_else(|| StoreError::UnknownTable(name.to_string()))
    }

    #[allow(dead_code)]
    pub fn column(&self, reference: &Reference) -> Result<&Column, StoreError> {
        self.table(&reference.table)?.field(&reference.column)
    }

    fn add_table(&mut self, defn: &s::ObjectType) -> Result<(), StoreError> {
        let table_name = Table::collection_name(&defn.name);
        let mut table = Table {
            object: defn.name.clone(),
            name: table_name.clone(),
            singular_name: Table::object_name(&defn.name),
            primary_key: PRIMARY_KEY_COLUMN.into(),
            columns: Vec::new(),
        };
        for field in &defn.fields {
            let sql_name = SqlName::from(&*field.name);
            let column = Column {
                name: sql_name,
                // Leave the GraphQL type here for now, we'll deal with it
                // in resolve_references
                sql_type: base_type(&field.field_type).to_owned(),
                nullable: is_nullable(&field.field_type),
                list: is_list(&field.field_type),
                derived: derived_column(field)?,
                references: vec![],
            };
            table.columns.push(column);
        }
        self.tables.push(table);
        for interface_name in &defn.implements_interfaces {
            match self
                .interfaces
                .iter_mut()
                .find(|interface| &interface.name == interface_name)
            {
                Some(ref mut interface) => interface.tables.push(table_name.clone()),
                None => {
                    return Err(StoreError::Unknown(format_err!(
                        "unknown interface {}",
                        interface_name
                    )))
                }
            }
        }
        Ok(())
    }

    fn add_interface(&mut self, defn: &s::InterfaceType) -> Result<(), StoreError> {
        self.interfaces.push(Interface {
            name: defn.name.clone(),
            tables: vec![],
        });
        Ok(())
    }

    fn resolve_references(&mut self) -> Result<(), StoreError> {
        for t in 0..self.tables.len() {
            for c in 0..self.tables[t].columns.len() {
                let column = &self.tables[t].columns[c];

                let (sql_type, references) =
                    if let Some(sql_type) = self.scalar_sql_type(&column.sql_type) {
                        // sql_type is a scalar
                        (sql_type.to_owned(), vec![])
                    } else if let Some(interface) = self
                        .interfaces
                        .iter()
                        .find(|interface| interface.name == column.sql_type)
                    {
                        // sql_type is an interface; add each table that contains
                        // a type that implements the interface into the references
                        let references: Vec<_> = interface
                            .tables
                            .iter()
                            .map(|table_name| self.table(table_name))
                            .collect::<Result<Vec<_>, _>>()?
                            .iter()
                            .map(|table| Reference::to_column(table, column))
                            .collect();
                        (self.id_type.sql_type().to_owned(), references)
                    } else {
                        // sql_type is an object; add a single reference to the target
                        // table and column
                        let other_table_name = Table::collection_name(&column.sql_type);
                        let other_table = self.table(&other_table_name)?;
                        let reference = Reference::to_column(other_table, column);
                        (self.id_type.sql_type().to_owned(), vec![reference])
                    };
                let column = &mut self.tables[t].columns[c];
                column.references = references;
                column.sql_type = sql_type;
            }
        }
        Ok(())
    }

    /// Construct a fake root table that has an attribute for each table
    /// we actually support
    fn fill_root_table(&mut self) {
        for table in &self.tables {
            let objects = Column {
                name: table.name.clone(),
                sql_type: table.object.clone(),
                nullable: false,
                list: true,
                derived: None,
                references: vec![table.primary_key()],
            };
            self.root_table.columns.push(objects);

            let object = Column {
                name: SqlName::from(&*table.singular_name),
                sql_type: table.object.clone(),
                nullable: false,
                list: false,
                derived: None,
                references: vec![table.primary_key()],
            };
            self.root_table.columns.push(object);
        }
    }

    /// Return the SQL type that corresponds to the scalar GraphQL type
    /// `name`. If `name` does not denote a known SQL type, return `None`
    fn scalar_sql_type(&self, name: &str) -> Option<&str> {
        // This is pretty adhoc, and solely based on the example schema
        match name {
            "Boolean" => Some("boolean"),
            "BigDecimal" => Some("numeric"),
            "BigInt" => Some("numeric"),
            "Bytes" => Some("bytea"),
            "ID" => Some(&self.id_type.sql_type()),
            "Int" => Some("integer"),
            "String" => Some("text"),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn root_table(&self) -> &Table {
        &self.root_table
    }
}

impl AsDdl for Mapping {
    fn fmt(&self, f: &mut dyn fmt::Write, mapping: &Mapping) -> fmt::Result {
        // Output 'create table' statements for all tables
        for table in &self.tables {
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
    fn to_column(table: &Table, column: &Column) -> Reference {
        let column = match &column.derived {
            Some(col) => col,
            None => &*table.primary_key.0,
        }
        .into();

        Reference {
            table: table.name.clone(),
            column,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: SqlName,
    /// The SQL type for this column. For lists, this only provides the type
    /// for the elements of the list; i.e., for a list of String, this is
    /// 'text' rather than 'text[]'
    pub sql_type: String,
    pub nullable: bool,
    pub list: bool,
    pub derived: Option<String>,
    pub references: Vec<Reference>,
}

#[allow(dead_code)]
impl Column {
    pub fn is_reference(&self) -> bool {
        !self.references.is_empty()
    }
}

impl AsDdl for Column {
    fn fmt(&self, f: &mut dyn fmt::Write, _: &Mapping) -> fmt::Result {
        write!(f, "    ")?;
        if self.derived.is_some() {
            write!(f, "-- ")?;
        }
        write!(f, "{:20} {}", self.name, self.sql_type)?;
        if self.list {
            write!(f, "[]")?;
        }
        if self.name.0 == PRIMARY_KEY_COLUMN {
            write!(f, " primary key")?;
        } else if !self.nullable {
            write!(f, " not null")?;
        }
        Ok(())
    }
}

/// The name for the primary key column of a table; hardcoded for now
#[allow(dead_code)]
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

    /// The name of the primary key column. This is only the name for the
    /// 'id' column; for storage schemes with composite primary key, other
    /// parts like the subgraph and the entity type are taken from this
    /// table resp. the mapping the table belongs to
    primary_key: SqlName,
    columns: Vec<Column>,
}

impl Table {
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
            column: self.primary_key.clone(),
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

#[derive(Clone, Debug)]
pub struct Interface {
    pub name: String,
    pub tables: Vec<SqlName>,
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

/// Return the base type underlying the given field type, i.e., the type
/// after stripping List and NonNull
fn base_type(field_type: &q::Type) -> &str {
    match field_type {
        q::Type::NamedType(name) => name,
        q::Type::ListType(child) => base_type(child),
        q::Type::NonNullType(child) => base_type(child),
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
        assert!(is_primary_key_column(&table.primary_key));

        let id = table
            .field(&PRIMARY_KEY_COLUMN.into())
            .expect("failed to get 'id' column for 'things' table");
        assert_eq!(ID_TYPE.sql_type(), id.sql_type);
        assert!(!id.nullable);
        assert!(!id.list);
        assert!(id.derived.is_none());
        assert!(!id.is_reference());

        let big_thing = table
            .field(&"big_thing".into())
            .expect("failed to get 'big_thing' column for 'things table");
        assert_eq!(ID_TYPE.sql_type(), big_thing.sql_type);
        assert!(!big_thing.nullable);
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

    const THINGS_DDL: &str = "create table rel.things (
        id                   text primary key,
        big_thing            text not null
);
create table rel.scalars (
        id                   text primary key,
        bool                 boolean,
        int                  integer,
        big_decimal          numeric,
        string               text,
        bytes                bytea,
        big_int              numeric
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
    const MUSIC_DDL: &str = "create table rel.musicians (
        id                   text primary key,
        name                 text not null,
        main_band            text,
        bands                text[] not null
     -- derived fields (not stored in this table)
     -- written_songs        text[] not null references songs(written_by)
);
create table rel.bands (
        id                   text primary key,
        name                 text not null,
        original_songs       text[] not null
     -- derived fields (not stored in this table)
     -- members              text[] not null references musicians(bands)
);
create table rel.songs (
        id                   text primary key,
        title                text not null,
        written_by           text not null
     -- derived fields (not stored in this table)
     -- band                 text references bands(original_songs)
);
create table rel.song_stats (
        id                   text primary key,
        played               integer not null
     -- derived fields (not stored in this table)
     -- song                 text references songs(id)
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
        id                   varchar primary key,
        forest               varchar
);
create table rel.forests (
        id                   varchar primary key
     -- derived fields (not stored in this table)
     -- dwellers             varchar[] not null references animals(forest)
);
create table rel.habitats (
        id                   varchar primary key,
        most_common          varchar not null,
        dwellers             varchar[] not null
);
";

}
