use std::{collections::BTreeSet, sync::Arc};

use diesel::{debug_query, pg::Pg};
use graph::{
    data_source::CausalityRegion,
    prelude::{r, serde_json as json, DeploymentHash, EntityFilter},
    schema::InputSchema,
};

use crate::{
    block_range::BoundSide,
    layout_for_tests::{make_dummy_site, Namespace},
    relational::{Catalog, ColumnType, Layout},
    relational_queries::{FindRangeQuery, FromColumnValue},
};

use crate::relational_queries::Filter;

#[test]
fn gql_value_from_bytes() {
    const EXP: &str = "0xdeadbeef";

    let exp = r::Value::String(EXP.to_string());
    for s in ["deadbeef", "\\xdeadbeef", "0xdeadbeef"] {
        let act =
            r::Value::from_column_value(&ColumnType::Bytes, json::Value::String(s.to_string()))
                .unwrap();

        assert_eq!(exp, act);
    }
}

fn test_layout(gql: &str) -> Layout {
    let subgraph = DeploymentHash::new("subgraph").unwrap();
    let schema = InputSchema::parse_latest(gql, subgraph.clone()).expect("Test schema invalid");
    let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
    let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
    let catalog =
        Catalog::for_tests(site.clone(), BTreeSet::new()).expect("Can not create catalog");
    Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
}

#[track_caller]
fn filter_contains(filter: EntityFilter, sql: &str) {
    const SCHEMA: &str = "
    type Thing @entity {
        id: Bytes!,
        address: Bytes!,
        name: String
    }";
    let layout = test_layout(SCHEMA);
    let table = layout
        .table_for_entity(&layout.input_schema.entity_type("Thing").unwrap())
        .unwrap()
        .dsl_table();
    let filter = Filter::main(&layout, table, &filter, Default::default()).unwrap();
    let query = debug_query::<Pg, _>(&filter);
    assert!(
        query.to_string().contains(sql),
        "Expected query /{}/ to contain /{}/",
        query,
        sql
    );
}

#[test]
fn prefix() {
    // String prefixes
    let filter = EntityFilter::Equal("name".to_string(), "Bibi".into());
    filter_contains(
        filter,
        r#"left(c."name", 256) = left($1, 256) -- binds: ["Bibi"]"#,
    );

    let filter = EntityFilter::In("name".to_string(), vec!["Bibi".into(), "Julian".into()]);
    filter_contains(
        filter,
        r#"left(c."name", 256) in ($1, $2) -- binds: ["Bibi", "Julian"]"#,
    );

    // Bytes prefixes
    let filter = EntityFilter::Equal("address".to_string(), "0xbeef".into());
    filter_contains(
        filter,
        r#"substring(c."address", 1, 64) = substring($1, 1, 64)"#,
    );

    let filter = EntityFilter::In("address".to_string(), vec!["0xbeef".into()]);
    filter_contains(filter, r#"substring(c."address", 1, 64) in ($1)"#);
}

#[test]
fn find_range_query_id_type_casting() {
    let string_schema = "
    type StringEntity @entity {
        id: String!,
        name: String
    }";

    let bytes_schema = "
    type BytesEntity @entity {
        id: Bytes!,
        address: Bytes
    }";

    let int8_schema = "
    type Int8Entity @entity {
        id: Int8!,
        value: Int8
    }";

    let string_layout = test_layout(string_schema);
    let bytes_layout = test_layout(bytes_schema);
    let int8_layout = test_layout(int8_schema);

    let string_table = string_layout
        .table_for_entity(
            &string_layout
                .input_schema
                .entity_type("StringEntity")
                .unwrap(),
        )
        .unwrap();
    let bytes_table = bytes_layout
        .table_for_entity(
            &bytes_layout
                .input_schema
                .entity_type("BytesEntity")
                .unwrap(),
        )
        .unwrap();
    let int8_table = int8_layout
        .table_for_entity(&int8_layout.input_schema.entity_type("Int8Entity").unwrap())
        .unwrap();

    let causality_region = CausalityRegion::ONCHAIN;
    let bound_side = BoundSide::Lower;
    let block_range = 100..200;

    test_id_type_casting(
        string_table.as_ref(),
        "id::bytea",
        "String ID should be cast to bytea",
    );
    test_id_type_casting(bytes_table.as_ref(), "id", "Bytes ID should remain as id");
    test_id_type_casting(
        int8_table.as_ref(),
        "id::text::bytea",
        "Int8 ID should be cast to text then bytea",
    );

    let tables = vec![
        string_table.as_ref(),
        bytes_table.as_ref(),
        int8_table.as_ref(),
    ];
    let query = FindRangeQuery::new(&tables, causality_region, bound_side, block_range);
    let sql = debug_query::<Pg, _>(&query).to_string();

    assert!(
        sql.contains("id::bytea"),
        "String entity ID casting should be present in UNION query"
    );
    assert!(
        sql.contains("id as id"),
        "Bytes entity ID should be present in UNION query"
    );
    assert!(
        sql.contains("id::text::bytea"),
        "Int8 entity ID casting should be present in UNION query"
    );

    assert!(
        sql.contains("union all"),
        "Multiple tables should generate UNION ALL queries"
    );
    assert!(
        sql.contains("order by block_number, entity, id"),
        "Query should end with proper ordering"
    );
}

fn test_id_type_casting(table: &crate::relational::Table, expected_cast: &str, test_name: &str) {
    let causality_region = CausalityRegion::ONCHAIN;
    let bound_side = BoundSide::Lower;
    let block_range = 100..200;

    let tables = vec![table];
    let query = FindRangeQuery::new(&tables, causality_region, bound_side, block_range);
    let sql = debug_query::<Pg, _>(&query).to_string();

    assert!(
        sql.contains(expected_cast),
        "{}: Expected '{}' in SQL, got: {}",
        test_name,
        expected_cast,
        sql
    );
}
