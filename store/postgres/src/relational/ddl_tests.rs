use index::CreateIndex;
use itertools::Itertools;
use pretty_assertions::assert_eq;

use super::*;

use crate::{deployment_store::generate_index_creation_sql, layout_for_tests::make_dummy_site};

const ID_TYPE: ColumnType = ColumnType::String;

fn test_layout(gql: &str) -> Layout {
    let subgraph = DeploymentHash::new("subgraph").unwrap();
    let schema = InputSchema::parse_latest(gql, subgraph.clone()).expect("Test schema invalid");
    let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
    let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
    let ents = {
        match schema.entity_type("FileThing") {
            Ok(entity_type) => BTreeSet::from_iter(vec![entity_type]),
            Err(_) => BTreeSet::new(),
        }
    };
    let catalog = Catalog::for_tests(site.clone(), ents).expect("Can not create catalog");
    Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
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

// Check that the two strings are the same after replacing runs of
// whitespace with a single space
#[track_caller]
fn check_eqv(left: &str, right: &str) {
    let left_s = left.split_whitespace().join(" ");
    let right_s = right.split_whitespace().join(" ");
    assert_eq!(left_s, right_s);
}

#[test]
fn test_manual_index_creation_ddl() {
    let layout = Arc::new(test_layout(BOOKS_GQL));

    #[track_caller]
    fn assert_generated_sql(
        layout: Arc<Layout>,
        entity_name: &str,
        field_names: Vec<String>,
        index_method: &str,
        expected_format: &str,
        after: Option<BlockNumber>,
    ) {
        let namespace = layout.site.namespace.clone();
        let expected = expected_format.replace("{namespace}", namespace.as_str());

        let (_, sql): (String, String) = generate_index_creation_sql(
            layout.clone(),
            entity_name,
            field_names,
            index::Method::from_str(index_method).unwrap(),
            after,
        )
        .unwrap();

        check_eqv(&expected, sql.trim());
    }

    const BTREE: &str = "btree"; // Assuming index::Method is the enum containing the BTree variant
    const GIST: &str = "gist";

    assert_generated_sql(
        layout.clone(),
        "Book",
        vec!["id".to_string()],
        BTREE,
        "create index concurrently if not exists manual_book_id on {namespace}.book using btree (\"id\")",
        None
    );

    assert_generated_sql(
        layout.clone(),
        "Book",
        vec!["content".to_string()],
        BTREE,
        "create index concurrently if not exists manual_book_content on {namespace}.book using btree (substring(\"content\", 1, 64))",
        None
    );

    assert_generated_sql(
        layout.clone(),
        "Book",
        vec!["title".to_string()],
        BTREE,
        "create index concurrently if not exists manual_book_title on {namespace}.book using btree (left(\"title\", 256))",
        None
    );

    assert_generated_sql(
        layout.clone(),
        "Book",
        vec!["page_count".to_string()],
        BTREE,
        "create index concurrently if not exists manual_book_page_count on {namespace}.book using btree (\"page_count\")",
        None
    );

    assert_generated_sql(
        layout.clone(),
        "Book",
        vec!["page_count".to_string(), "title".to_string()],
        BTREE,
        "create index concurrently if not exists manual_book_page_count_title on {namespace}.book using btree (\"page_count\", left(\"title\", 256))",
        None
    );

    assert_generated_sql(
        layout.clone(),
        "Book",
        vec!["content".to_string(), "block_range".to_string()], // Explicitly including 'block_range'
        GIST,
        "create index concurrently if not exists manual_book_content_block_range on {namespace}.book using gist (substring(\"content\", 1, 64), block_range)",
        None
    );

    assert_generated_sql(
        layout.clone(),
        "Book",
        vec!["page_count".to_string()],
        BTREE,
        "create index concurrently if not exists manual_book_page_count_12345 on sgd0815.book using btree (\"page_count\")  where coalesce(upper(block_range), 2147483647) > 12345",
        Some(12345)
    );
}

#[test]
fn generate_postponed_indexes() {
    let layout = test_layout(THING_GQL);
    let table = layout.table(&SqlName::from("Scalar")).unwrap();
    let skip_colums = vec!["id".to_string()];
    let query_vec = table.create_postponed_indexes(skip_colums);
    assert!(query_vec.len() == 7);
    let queries = query_vec.join(" ");
    check_eqv(THING_POSTPONED_INDEXES, &queries)
}
const THING_POSTPONED_INDEXES: &str = r#"
create index concurrently if not exists attr_1_1_scalar_bool
    on "sgd0815"."scalar" using btree("bool");
 create index concurrently if not exists attr_1_2_scalar_int
    on "sgd0815"."scalar" using btree("int");
 create index concurrently if not exists attr_1_3_scalar_big_decimal
    on "sgd0815"."scalar" using btree("big_decimal");
 create index concurrently if not exists attr_1_4_scalar_string
    on "sgd0815"."scalar" using btree(left("string", 256));
 create index concurrently if not exists attr_1_5_scalar_bytes
    on "sgd0815"."scalar" using btree(substring("bytes", 1, 64));
 create index concurrently if not exists attr_1_6_scalar_big_int
    on "sgd0815"."scalar" using btree("big_int");
 create index concurrently if not exists attr_1_7_scalar_color
    on "sgd0815"."scalar" using btree("color");
"#;

impl IndexList {
    fn mock_thing_index_list() -> Self {
        let mut indexes: HashMap<String, Vec<CreateIndex>> = HashMap::new();
        let v1 = vec![
            CreateIndex::parse(r#"create index thing_id_block_range_excl on sgd0815.thing using gist (id, block_range)"#.to_string()),
            CreateIndex::parse(r#"create index brin_thing on sgd0815."thing" using brin (lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops)"#.to_string()),
            // fixme: enable the index bellow once the parsing of statements is fixed, and BlockRangeUpper in particular (issue #5512)
            // CreateIndex::parse(r#"create index thing_block_range_closed on sgd0815."thing" using btree (coalesce(upper(block_range), 2147483647)) where coalesce((upper(block_range), 2147483647) < 2147483647)"#.to_string()),
            CreateIndex::parse(r#"create index attr_0_0_thing_id on sgd0815."thing" using btree (id)"#.to_string()),
            CreateIndex::parse(r#"create index attr_0_1_thing_big_thing on sgd0815."thing" using gist (big_thing, block_range)"#.to_string()),
        ];
        indexes.insert("thing".to_string(), v1);
        let v2 = vec![
            CreateIndex::parse(r#"create index attr_1_0_scalar_id on sgd0815."scalar" using btree (id)"#.to_string(),),
            CreateIndex::parse(r#"create index attr_1_1_scalar_bool on sgd0815."scalar" using btree (bool)"#.to_string(),),
            CreateIndex::parse(r#"create index attr_1_2_scalar_int on sgd0815."scalar" using btree (int)"#.to_string(),),
            CreateIndex::parse(r#"create index attr_1_3_scalar_big_decimal on sgd0815."scalar" using btree (big_decimal)"#.to_string()),
            CreateIndex::parse(r#"create index attr_1_4_scalar_string on sgd0815."scalar" using btree (left(string, 256))"#.to_string()),
            CreateIndex::parse(r#"create index attr_1_5_scalar_bytes on sgd0815."scalar" using btree (substring(bytes, 1, 64))"#.to_string()),
            CreateIndex::parse(r#"create index attr_1_6_scalar_big_int on sgd0815."scalar" using btree (big_int)"#.to_string()),
            CreateIndex::parse(r#"create index attr_1_7_scalar_color on sgd0815."scalar" using btree (color)"#.to_string()),
        ];
        indexes.insert("scalar".to_string(), v2);
        let v3 = vec![CreateIndex::parse(
            r#"create index attr_2_0_file_thing_id on sgd0815."file_thing" using btree (id)"#
                .to_string(),
        )];
        indexes.insert("file_thing".to_string(), v3);
        IndexList { indexes }
    }
}

#[test]
fn generate_ddl() {
    let layout = test_layout(THING_GQL);
    let sql = layout.as_ddl(None).expect("Failed to generate DDL");
    assert_eq!(THING_DDL, &sql); // Use `assert_eq!` to also test the formatting.

    let il = IndexList::mock_thing_index_list();
    let layout = test_layout(THING_GQL);
    let sql = layout.as_ddl(Some(il)).expect("Failed to generate DDL");
    println!("SQL: {}", sql);
    println!("THING_DDL_ON_COPY: {}", THING_DDL_ON_COPY);
    check_eqv(THING_DDL_ON_COPY, &sql);

    let layout = test_layout(MUSIC_GQL);
    let sql = layout.as_ddl(None).expect("Failed to generate DDL");
    check_eqv(MUSIC_DDL, &sql);

    let layout = test_layout(FOREST_GQL);
    let sql = layout.as_ddl(None).expect("Failed to generate DDL");
    check_eqv(FOREST_DDL, &sql);

    let layout = test_layout(FULLTEXT_GQL);
    let sql = layout.as_ddl(None).expect("Failed to generate DDL");
    check_eqv(FULLTEXT_DDL, &sql);

    let layout = test_layout(FORWARD_ENUM_GQL);
    let sql = layout.as_ddl(None).expect("Failed to generate DDL");
    check_eqv(FORWARD_ENUM_SQL, &sql);

    let layout = test_layout(TS_GQL);
    let sql = layout.as_ddl(None).expect("Failed to generate DDL");
    check_eqv(TS_SQL, &sql);

    let layout = test_layout(LIFETIME_GQL);
    let sql = layout.as_ddl(None).expect("Failed to generate DDL");
    check_eqv(LIFETIME_SQL, &sql);
}

#[test]
fn exlusion_ddl() {
    let layout = test_layout(THING_GQL);
    let table = layout
        .table_for_entity(&layout.input_schema.entity_type("Thing").unwrap())
        .unwrap();

    // When `as_constraint` is false, just create an index
    let mut out = String::new();
    table
        .exclusion_ddl_inner(&mut out, false)
        .expect("can write exclusion DDL");
    check_eqv(
        r#"create index thing_id_block_range_excl on "sgd0815"."thing" using gist (id, block_range);"#,
        out.trim(),
    );

    // When `as_constraint` is true, add an exclusion constraint
    let mut out = String::new();
    table
        .exclusion_ddl_inner(&mut out, true)
        .expect("can write exclusion DDL");
    check_eqv(
        r#"alter table "sgd0815"."thing" add constraint thing_id_block_range_excl exclude using gist (id with =, block_range with &&);"#,
        out.trim(),
    );
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
    let dest =
        test_layout("type Scalar @entity { id: ID } type Other @entity { id: ID, int: Int! }");
    assert!(dest.can_copy_from(&source).is_empty());

    // We allow making a non-nullable attribute nullable
    let dest = test_layout("type Thing @entity { id: ID! }");
    assert!(dest.can_copy_from(&source).is_empty());

    // We can not turn a non-nullable attribute into a nullable attribute
    let dest = test_layout("type Scalar @entity { id: ID! }");
    assert_eq!(
        vec![
            "The attribute Scalar.id is non-nullable, but the \
                 corresponding attribute in the source is nullable"
        ],
        dest.can_copy_from(&source)
    );

    // We can not change a scalar field to an array
    let dest = test_layout("type Scalar @entity { id: ID, string: [String] }");
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
    let dest = test_layout("type Scalar @entity { id: ID, color: Int }");
    assert_eq!(
        vec![
            "The attribute Scalar.color has type Int, but \
                 its type in the source is Color"
        ],
        dest.can_copy_from(&source)
    );
    // We can not change the underlying type of a field in arrays
    let source = test_layout("type Scalar @entity { id: ID, color: [Int!]! }");
    let dest = test_layout("type Scalar @entity { id: ID, color: [String!]! }");
    assert_eq!(
        vec![
            "The attribute Scalar.color has type [String!]!, but \
                 its type in the source is [Int!]!"
        ],
        dest.can_copy_from(&source)
    );
}

const THING_GQL: &str = r#"
        type Thing @entity {
            id: ID!
            bigThing: Thing!
        }

        enum Color { yellow, red, BLUE }

        enum Size { small, medium, large }

        type Scalar @entity {
            id: ID,
            bool: Boolean,
            int: Int,
            bigDecimal: BigDecimal,
            string: String,
            bytes: Bytes,
            bigInt: BigInt,
            color: Color,
        }

        type FileThing @entity {
            id: ID!
        }
        "#;

const THING_DDL: &str = r#"create type sgd0815."color"
    as enum ('BLUE', 'red', 'yellow');
create type sgd0815."size"
    as enum ('large', 'medium', 'small');

    create table "sgd0815"."thing" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "big_thing"          text not null
    );

    alter table "sgd0815"."thing"
        add constraint thing_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_thing
    on "sgd0815"."thing"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index thing_block_range_closed
    on "sgd0815"."thing"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_thing_id
    on "sgd0815"."thing" using btree("id");
create index attr_0_1_thing_big_thing
    on "sgd0815"."thing" using gist("big_thing", block_range);


    create table "sgd0815"."scalar" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "bool"               boolean,
        "int"                int4,
        "big_decimal"        numeric,
        "string"             text,
        "bytes"              bytea,
        "big_int"            numeric,
        "color"              "sgd0815"."color"
    );

    alter table "sgd0815"."scalar"
        add constraint scalar_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_scalar
    on "sgd0815"."scalar"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index scalar_block_range_closed
    on "sgd0815"."scalar"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_scalar_id
    on "sgd0815"."scalar" using btree("id");
create index attr_1_1_scalar_bool
    on "sgd0815"."scalar" using btree("bool");
create index attr_1_2_scalar_int
    on "sgd0815"."scalar" using btree("int");
create index attr_1_3_scalar_big_decimal
    on "sgd0815"."scalar" using btree("big_decimal");
create index attr_1_4_scalar_string
    on "sgd0815"."scalar" using btree(left("string", 256));
create index attr_1_5_scalar_bytes
    on "sgd0815"."scalar" using btree(substring("bytes", 1, 64));
create index attr_1_6_scalar_big_int
    on "sgd0815"."scalar" using btree("big_int");
create index attr_1_7_scalar_color
    on "sgd0815"."scalar" using btree("color");


    create table "sgd0815"."file_thing" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        causality_region     int not null,
        "id"                 text not null
    );

    alter table "sgd0815"."file_thing"
        add constraint file_thing_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_file_thing
    on "sgd0815"."file_thing"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index file_thing_block_range_closed
    on "sgd0815"."file_thing"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_file_thing_id
    on "sgd0815"."file_thing" using btree("id");

"#;

const THING_DDL_ON_COPY: &str = r#"create type sgd0815."color"
    as enum ('BLUE', 'red', 'yellow');
create type sgd0815."size"
    as enum ('large', 'medium', 'small');

    create table "sgd0815"."thing" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "big_thing"          text not null
    );

    alter table "sgd0815"."thing"
        add constraint thing_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_thing
    on "sgd0815"."thing"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index thing_block_range_closed
    on "sgd0815"."thing"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_thing_id
    on sgd0815."thing" using btree (id);
create index attr_0_1_thing_big_thing
    on sgd0815."thing" using gist (big_thing, block_range);


    create table "sgd0815"."scalar" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "bool"               boolean,
        "int"                int4,
        "big_decimal"        numeric,
        "string"             text,
        "bytes"              bytea,
        "big_int"            numeric,
        "color"              "sgd0815"."color"
    );

    alter table "sgd0815"."scalar"
        add constraint scalar_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_scalar
    on "sgd0815"."scalar"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index scalar_block_range_closed
    on "sgd0815"."scalar"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_scalar_id
    on sgd0815."scalar" using btree (id);


    create table "sgd0815"."file_thing" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        causality_region     int not null,
        "id"                 text not null
    );

    alter table "sgd0815"."file_thing"
        add constraint file_thing_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_file_thing
    on "sgd0815"."file_thing"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index file_thing_block_range_closed
    on "sgd0815"."file_thing"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_file_thing_id
    on sgd0815."file_thing" using btree (id);
"#;

const BOOKS_GQL: &str = r#"type Author @entity {
    id: ID!
    name: String!
    books: [Book!]! @derivedFrom(field: "author")
}

type Book @entity {
    id: ID!
    title: String!
    content: Bytes!
    pageCount: BigInt!
    author: Author!
}"#;

const MUSIC_GQL: &str = r#"type Musician @entity {
    id: ID!
    name: String!
    mainBand: Band
    bands: [Band!]!
    writtenSongs: [Song]! @derivedFrom(field: "writtenBy")
}

type Band @entity {
    id: ID!
    name: String!
    members: [Musician!]! @derivedFrom(field: "bands")
    originalSongs: [Song!]!
}

type Song @entity(immutable: true) {
    id: ID!
    title: String!
    writtenBy: Musician!
    band: Band @derivedFrom(field: "originalSongs")
}

type SongStat @entity {
    id: ID!
    song: Song @derivedFrom(field: "id")
    played: Int!
}"#;
const MUSIC_DDL: &str = r#"create table "sgd0815"."musician" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "name"               text not null,
        "main_band"          text,
        "bands"              text[] not null
);
alter table "sgd0815"."musician"
  add constraint musician_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_musician
    on "sgd0815"."musician"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index musician_block_range_closed
    on "sgd0815"."musician"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_musician_id
    on "sgd0815"."musician" using btree("id");
create index attr_0_1_musician_name
    on "sgd0815"."musician" using btree(left("name", 256));
create index attr_0_2_musician_main_band
    on "sgd0815"."musician" using gist("main_band", block_range);

create table "sgd0815"."band" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "name"               text not null,
        "original_songs"     text[] not null
);
alter table "sgd0815"."band"
  add constraint band_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_band
    on "sgd0815"."band"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index band_block_range_closed
    on "sgd0815"."band"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_band_id
    on "sgd0815"."band" using btree("id");
create index attr_1_1_band_name
    on "sgd0815"."band" using btree(left("name", 256));

create table "sgd0815"."song" (
        vid                    bigserial primary key,
        block$                 int not null,
        "id"                 text not null,
        "title"              text not null,
        "written_by"         text not null,

        unique(id)
);
create index song_block
    on "sgd0815"."song"(block$);
create index attr_2_0_song_title
    on "sgd0815"."song" using btree(left("title", 256));
create index attr_2_1_song_written_by
    on "sgd0815"."song" using btree("written_by", block$);

create table "sgd0815"."song_stat" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "played"             int4 not null
);
alter table "sgd0815"."song_stat"
  add constraint song_stat_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_song_stat
    on "sgd0815"."song_stat"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index song_stat_block_range_closed
    on "sgd0815"."song_stat"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_3_0_song_stat_id
    on "sgd0815"."song_stat" using btree("id");
create index attr_3_1_song_stat_played
    on "sgd0815"."song_stat" using btree("played");

"#;

const FOREST_GQL: &str = r#"
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
    dwellers: [ForestDweller!]! @derivedFrom(field: "forest")
}
type Habitat @entity {
    id: ID!,
    # Use interface as direct reference
    most_common: ForestDweller!,
    dwellers: [ForestDweller!]!
}"#;

const FOREST_DDL: &str = r#"create table "sgd0815"."animal" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "forest"             text
);
alter table "sgd0815"."animal"
  add constraint animal_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_animal
    on "sgd0815"."animal"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index animal_block_range_closed
    on "sgd0815"."animal"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_animal_id
    on "sgd0815"."animal" using btree("id");
create index attr_0_1_animal_forest
    on "sgd0815"."animal" using gist("forest", block_range);

create table "sgd0815"."forest" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"               text not null
);
alter table "sgd0815"."forest"
  add constraint forest_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_forest
    on "sgd0815"."forest"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index forest_block_range_closed
    on "sgd0815"."forest"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_forest_id
    on "sgd0815"."forest" using btree("id");

create table "sgd0815"."habitat" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "most_common"        text not null,
        "dwellers"           text[] not null
);
alter table "sgd0815"."habitat"
  add constraint habitat_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_habitat
    on "sgd0815"."habitat"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index habitat_block_range_closed
    on "sgd0815"."habitat"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_habitat_id
    on "sgd0815"."habitat" using btree("id");
create index attr_2_1_habitat_most_common
    on "sgd0815"."habitat" using gist("most_common", block_range);

"#;
const FULLTEXT_GQL: &str = r#"
type _Schema_ @fulltext(
    name: "search"
    language: en
    algorithm: rank
    include: [
        {
            entity: "Animal",
            fields: [
                {name: "name"},
                {name: "species"}
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
    dwellers: [Animal!]! @derivedFrom(field: "forest")
}
type Habitat @entity {
    id: ID!,
    most_common: Animal!,
    dwellers: [Animal!]!
}"#;

const FULLTEXT_DDL: &str = r#"create table "sgd0815"."animal" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "name"               text not null,
        "species"            text not null,
        "forest"             text,
        "search"             tsvector
);
alter table "sgd0815"."animal"
  add constraint animal_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_animal
    on "sgd0815"."animal"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index animal_block_range_closed
    on "sgd0815"."animal"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_animal_id
    on "sgd0815"."animal" using btree("id");
create index attr_0_1_animal_name
    on "sgd0815"."animal" using btree(left("name", 256));
create index attr_0_2_animal_species
    on "sgd0815"."animal" using btree(left("species", 256));
create index attr_0_3_animal_forest
    on "sgd0815"."animal" using gist("forest", block_range);
create index attr_0_4_animal_search
    on "sgd0815"."animal" using gin("search");

create table "sgd0815"."forest" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null
);
alter table "sgd0815"."forest"
  add constraint forest_id_block_range_excl exclude using gist (id with =, block_range with &&);

create index brin_forest
    on "sgd0815"."forest"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index forest_block_range_closed
    on "sgd0815"."forest"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_1_0_forest_id
    on "sgd0815"."forest" using btree("id");

create table "sgd0815"."habitat" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "most_common"        text not null,
        "dwellers"           text[] not null
);
alter table "sgd0815"."habitat"
  add constraint habitat_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_habitat
    on "sgd0815"."habitat"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index habitat_block_range_closed
    on "sgd0815"."habitat"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_2_0_habitat_id
    on "sgd0815"."habitat" using btree("id");
create index attr_2_1_habitat_most_common
    on "sgd0815"."habitat" using gist("most_common", block_range);

"#;

const FORWARD_ENUM_GQL: &str = r#"
type Thing @entity  {
    id: ID!,
    orientation: Orientation!
}

enum Orientation {
    UP, DOWN
}
"#;

const FORWARD_ENUM_SQL: &str = r#"create type sgd0815."orientation"
    as enum ('DOWN', 'UP');
create table "sgd0815"."thing" (
        vid                  bigserial primary key,
        block_range          int4range not null,
        "id"                 text not null,
        "orientation"        "sgd0815"."orientation" not null
);
alter table "sgd0815"."thing"
  add constraint thing_id_block_range_excl exclude using gist (id with =, block_range with &&);
create index brin_thing
    on "sgd0815"."thing"
 using brin(lower(block_range) int4_minmax_ops, coalesce(upper(block_range), 2147483647) int4_minmax_ops, vid int8_minmax_ops);
create index thing_block_range_closed
    on "sgd0815"."thing"(coalesce(upper(block_range), 2147483647))
 where coalesce(upper(block_range), 2147483647) < 2147483647;
create index attr_0_0_thing_id
    on "sgd0815"."thing" using btree("id");
create index attr_0_1_thing_orientation
    on "sgd0815"."thing" using btree("orientation");

"#;

const TS_GQL: &str = r#"
type Data @entity(timeseries: true) {
    id: Int8!
    timestamp: Timestamp!
    amount: BigDecimal!
}

type Stats @aggregation(intervals: ["hour", "day"], source: "Data") {
    id:        Int8!
    timestamp: Timestamp!
    volume:    BigDecimal! @aggregate(fn: "sum", arg: "amount")
    maxPrice:  BigDecimal! @aggregate(fn: "max", arg: "amount")
}
"#;

const TS_SQL: &str = r#"
create table "sgd0815"."data" (
    vid                  bigserial primary key,
    block$                int not null,
    "id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "amount"             numeric not null,
    unique(id)
);
create index data_block
    on "sgd0815"."data"(block$);
create index attr_0_0_data_timestamp
    on "sgd0815"."data" using btree("timestamp");
create index attr_0_1_data_amount
    on "sgd0815"."data" using btree("amount");

create table "sgd0815"."stats_hour" (
    vid                  bigserial primary key,
    block$                int not null,
    "id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "volume"             numeric not null,
    "max_price"          numeric not null,
    unique(id)
);
create index stats_hour_block
    on "sgd0815"."stats_hour"(block$);
create index attr_1_0_stats_hour_timestamp
    on "sgd0815"."stats_hour" using btree("timestamp");
create index attr_1_1_stats_hour_volume
    on "sgd0815"."stats_hour" using btree("volume");
create index attr_1_2_stats_hour_max_price
    on "sgd0815"."stats_hour" using btree("max_price");

create table "sgd0815"."stats_day" (
    vid                  bigserial primary key,
    block$               int not null,
    "id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "volume"             numeric not null,
    "max_price"          numeric not null,
    unique(id)
);
create index stats_day_block
    on "sgd0815"."stats_day"(block$);
create index attr_2_0_stats_day_timestamp
    on "sgd0815"."stats_day" using btree("timestamp");
create index attr_2_1_stats_day_volume
    on "sgd0815"."stats_day" using btree("volume");
create index attr_2_2_stats_day_max_price
    on "sgd0815"."stats_day" using btree("max_price");"#;

const LIFETIME_GQL: &str = r#"
    type Data @entity(timeseries: true) {
        id: Int8!
        timestamp: Timestamp!
        group1: Int!
        group2: Int!
        amount: BigDecimal!
    }

    type Stats1 @aggregation(intervals: ["hour", "day"], source: "Data") {
        id:        Int8!
        timestamp: Timestamp!
        volume:    BigDecimal! @aggregate(fn: "sum", arg: "amount", cumulative: true)
    }

    type Stats2 @aggregation(intervals: ["hour", "day"], source: "Data") {
        id:        Int8!
        timestamp: Timestamp!
        group1: Int!
        volume:    BigDecimal! @aggregate(fn: "sum", arg: "amount", cumulative: true)
    }

    type Stats3 @aggregation(intervals: ["hour", "day"], source: "Data") {
        id:        Int8!
        timestamp: Timestamp!
        group2: Int!
        group1: Int!
        volume:    BigDecimal! @aggregate(fn: "sum", arg: "amount", cumulative: true)
    }

    type Stats2 @aggregation(intervals: ["hour", "day"], source: "Data") {
        id:        Int8!
        timestamp: Timestamp!
        group1: Int!
        group2: Int!
        volume:    BigDecimal! @aggregate(fn: "sum", arg: "amount", cumulative: true)
    }
    "#;

const LIFETIME_SQL: &str = r#"
create table "sgd0815"."data" (
    vid                  bigserial primary key,
    block$                int not null,
"id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "group_1"            int4 not null,
    "group_2"            int4 not null,
    "amount"             numeric not null,
    unique(id)
);
create index data_block
on "sgd0815"."data"(block$);
create index attr_0_0_data_timestamp
on "sgd0815"."data" using btree("timestamp");
create index attr_0_1_data_group_1
on "sgd0815"."data" using btree("group_1");
create index attr_0_2_data_group_2
on "sgd0815"."data" using btree("group_2");
create index attr_0_3_data_amount
on "sgd0815"."data" using btree("amount");

create table "sgd0815"."stats_1_hour" (
    vid                  bigserial primary key,
    block$                int not null,
"id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "volume"             numeric not null,
    unique(id)
);
create index stats_1_hour_block
on "sgd0815"."stats_1_hour"(block$);
create index attr_1_0_stats_1_hour_timestamp
on "sgd0815"."stats_1_hour" using btree("timestamp");
create index attr_1_1_stats_1_hour_volume
on "sgd0815"."stats_1_hour" using btree("volume");


create table "sgd0815"."stats_1_day" (
    vid                  bigserial primary key,
    block$                int not null,
"id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "volume"             numeric not null,
    unique(id)
);
create index stats_1_day_block
on "sgd0815"."stats_1_day"(block$);
create index attr_2_0_stats_1_day_timestamp
on "sgd0815"."stats_1_day" using btree("timestamp");
create index attr_2_1_stats_1_day_volume
on "sgd0815"."stats_1_day" using btree("volume");


create table "sgd0815"."stats_2_hour" (
    vid                  bigserial primary key,
    block$                int not null,
"id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "group_1"            int4 not null,
    "volume"             numeric not null,
    unique(id)
);
create index stats_2_hour_block
on "sgd0815"."stats_2_hour"(block$);
create index attr_5_0_stats_2_hour_timestamp
on "sgd0815"."stats_2_hour" using btree("timestamp");
create index attr_5_1_stats_2_hour_group_1
on "sgd0815"."stats_2_hour" using btree("group_1");
create index attr_5_2_stats_2_hour_volume
on "sgd0815"."stats_2_hour" using btree("volume");
create index stats_2_hour_dims
on "sgd0815"."stats_2_hour"(group_1, timestamp);

create table "sgd0815"."stats_2_day" (
    vid                  bigserial primary key,
    block$                int not null,
"id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "group_1"            int4 not null,
    "volume"             numeric not null,
    unique(id)
);
create index stats_2_day_block
on "sgd0815"."stats_2_day"(block$);
create index attr_6_0_stats_2_day_timestamp
on "sgd0815"."stats_2_day" using btree("timestamp");
create index attr_6_1_stats_2_day_group_1
on "sgd0815"."stats_2_day" using btree("group_1");
create index attr_6_2_stats_2_day_volume
on "sgd0815"."stats_2_day" using btree("volume");
create index stats_2_day_dims
on "sgd0815"."stats_2_day"(group_1, timestamp);

create table "sgd0815"."stats_3_hour" (
    vid                  bigserial primary key,
    block$                int not null,
"id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "group_2"            int4 not null,
    "group_1"            int4 not null,
    "volume"             numeric not null,
    unique(id)
);
create index stats_3_hour_block
on "sgd0815"."stats_3_hour"(block$);
create index attr_7_0_stats_3_hour_timestamp
on "sgd0815"."stats_3_hour" using btree("timestamp");
create index attr_7_1_stats_3_hour_group_2
on "sgd0815"."stats_3_hour" using btree("group_2");
create index attr_7_2_stats_3_hour_group_1
on "sgd0815"."stats_3_hour" using btree("group_1");
create index attr_7_3_stats_3_hour_volume
on "sgd0815"."stats_3_hour" using btree("volume");
create index stats_3_hour_dims
on "sgd0815"."stats_3_hour"(group_2, group_1, timestamp);

create table "sgd0815"."stats_3_day" (
    vid                  bigserial primary key,
    block$                int not null,
"id"                 int8 not null,
    "timestamp"          timestamptz not null,
    "group_2"            int4 not null,
    "group_1"            int4 not null,
    "volume"             numeric not null,
    unique(id)
);
create index stats_3_day_block
on "sgd0815"."stats_3_day"(block$);
create index attr_8_0_stats_3_day_timestamp
on "sgd0815"."stats_3_day" using btree("timestamp");
create index attr_8_1_stats_3_day_group_2
on "sgd0815"."stats_3_day" using btree("group_2");
create index attr_8_2_stats_3_day_group_1
on "sgd0815"."stats_3_day" using btree("group_1");
create index attr_8_3_stats_3_day_volume
on "sgd0815"."stats_3_day" using btree("volume");
create index stats_3_day_dims
on "sgd0815"."stats_3_day"(group_2, group_1, timestamp);
"#;
