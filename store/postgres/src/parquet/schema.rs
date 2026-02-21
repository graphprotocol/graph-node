use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::relational::{ColumnType, Table};

/// Map a relational `Table` to an Arrow `Schema`.
///
/// System columns are emitted first (`vid`, then block tracking, then
/// `causality_region`), followed by data columns in declaration order.
/// Fulltext (TSVector) columns are skipped — they are generated and
/// rebuilt on restore.
pub fn arrow_schema(table: &Table) -> Schema {
    let mut fields = Vec::new();

    // vid — always present, non-nullable
    fields.push(Field::new("vid", DataType::Int64, false));

    // Block tracking
    if table.immutable {
        fields.push(Field::new("block$", DataType::Int32, false));
    } else {
        fields.push(Field::new("block_range_start", DataType::Int32, false));
        fields.push(Field::new("block_range_end", DataType::Int32, true));
    }

    // Causality region
    if table.has_causality_region {
        fields.push(Field::new("causality_region", DataType::Int32, false));
    }

    // Data columns
    for col in &table.columns {
        if col.is_fulltext() {
            continue;
        }
        let base_type = column_type_to_arrow(&col.column_type);
        let dt = if col.is_list() {
            DataType::List(Arc::new(Field::new("item", base_type, true)))
        } else {
            base_type
        };
        fields.push(Field::new(col.name.as_str(), dt, col.is_nullable()));
    }

    Schema::new(fields)
}

/// Fixed Arrow schema for the `data_sources$` table.
pub fn data_sources_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("vid", DataType::Int64, false),
        Field::new("block_range_start", DataType::Int32, false),
        Field::new("block_range_end", DataType::Int32, true),
        Field::new("causality_region", DataType::Int32, false),
        Field::new("manifest_idx", DataType::Int32, false),
        Field::new("parent", DataType::Int32, true),
        Field::new("id", DataType::Binary, true),
        Field::new("param", DataType::Binary, true),
        Field::new("context", DataType::Utf8, true),
        Field::new("done_at", DataType::Int32, true),
    ])
}

fn column_type_to_arrow(ct: &ColumnType) -> DataType {
    match ct {
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Int => DataType::Int32,
        ColumnType::Int8 => DataType::Int64,
        ColumnType::Bytes => DataType::Binary,
        ColumnType::BigInt | ColumnType::BigDecimal => DataType::Utf8,
        ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnType::String | ColumnType::Enum(_) => DataType::Utf8,
        ColumnType::TSVector(_) => {
            unreachable!("TSVector columns should be skipped before calling column_type_to_arrow")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};
    use graph::prelude::DeploymentHash;
    use graph::schema::InputSchema;

    use crate::layout_for_tests::{make_dummy_site, Catalog, Layout, Namespace};

    use super::*;

    fn test_layout(gql: &str) -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = InputSchema::parse_latest(gql, subgraph.clone()).expect("Test schema invalid");
        let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let catalog =
            Catalog::for_tests(site.clone(), BTreeSet::new()).expect("Can not create catalog");
        Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
    }

    fn test_layout_with_causality(gql: &str, entity_name: &str) -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = InputSchema::parse_latest(gql, subgraph.clone()).expect("Test schema invalid");
        let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let entity_type = schema.entity_type(entity_name).unwrap();
        let ents = BTreeSet::from_iter(vec![entity_type]);
        let catalog = Catalog::for_tests(site.clone(), ents).expect("Can not create catalog");
        Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
    }

    #[test]
    fn immutable_table_schema() {
        let layout = test_layout(
            "type Token @entity(immutable: true) { id: ID!, name: String!, decimals: Int! }",
        );
        let table = layout.table("token").unwrap();
        let schema = arrow_schema(table);

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, &["vid", "block$", "id", "name", "decimals"]);

        assert_eq!(
            schema.field_with_name("vid").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(!schema.field_with_name("vid").unwrap().is_nullable());

        assert_eq!(
            schema.field_with_name("block$").unwrap().data_type(),
            &DataType::Int32
        );
        assert!(!schema.field_with_name("block$").unwrap().is_nullable());

        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(!schema.field_with_name("id").unwrap().is_nullable());

        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("decimals").unwrap().data_type(),
            &DataType::Int32
        );
    }

    #[test]
    fn mutable_table_with_causality_region() {
        let layout = test_layout_with_causality(
            "type Transfer @entity { id: ID!, amount: BigInt! }",
            "Transfer",
        );
        let table = layout.table("transfer").unwrap();
        let schema = arrow_schema(table);

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            &[
                "vid",
                "block_range_start",
                "block_range_end",
                "causality_region",
                "id",
                "amount"
            ]
        );

        assert_eq!(
            schema
                .field_with_name("block_range_start")
                .unwrap()
                .data_type(),
            &DataType::Int32
        );
        assert!(!schema
            .field_with_name("block_range_start")
            .unwrap()
            .is_nullable());

        assert_eq!(
            schema
                .field_with_name("block_range_end")
                .unwrap()
                .data_type(),
            &DataType::Int32
        );
        assert!(schema
            .field_with_name("block_range_end")
            .unwrap()
            .is_nullable());

        assert_eq!(
            schema
                .field_with_name("causality_region")
                .unwrap()
                .data_type(),
            &DataType::Int32
        );

        assert_eq!(
            schema.field_with_name("amount").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn mutable_table_without_causality_region() {
        let layout = test_layout("type Pair @entity { id: ID!, price: BigDecimal! }");
        let table = layout.table("pair").unwrap();
        let schema = arrow_schema(table);

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            &["vid", "block_range_start", "block_range_end", "id", "price"]
        );

        // No causality_region field
        assert!(schema.field_with_name("causality_region").is_err());
    }

    #[test]
    fn nullable_and_list_columns() {
        let layout = test_layout(
            "type Pool @entity(immutable: true) { id: ID!, tags: [String!]!, description: String }",
        );
        let table = layout.table("pool").unwrap();
        let schema = arrow_schema(table);

        // tags is a non-nullable list of strings
        let tags = schema.field_with_name("tags").unwrap();
        assert!(!tags.is_nullable());
        match tags.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Utf8);
            }
            other => panic!("expected List, got {:?}", other),
        }

        // description is nullable
        let desc = schema.field_with_name("description").unwrap();
        assert!(desc.is_nullable());
        assert_eq!(desc.data_type(), &DataType::Utf8);
    }

    #[test]
    fn all_scalar_column_types() {
        let layout = test_layout(
            "type Everything @entity(immutable: true) { \
                id: ID!, \
                flag: Boolean!, \
                small: Int!, \
                big: Int8!, \
                amount: BigInt!, \
                price: BigDecimal!, \
                data: Bytes!, \
                ts: Timestamp!, \
                label: String! \
            }",
        );
        let table = layout.table("everything").unwrap();
        let schema = arrow_schema(table);

        assert_eq!(
            schema.field_with_name("flag").unwrap().data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            schema.field_with_name("small").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema.field_with_name("big").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("amount").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("price").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("data").unwrap().data_type(),
            &DataType::Binary
        );
        assert_eq!(
            schema.field_with_name("ts").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field_with_name("label").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn data_sources_schema() {
        let schema = data_sources_arrow_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            &[
                "vid",
                "block_range_start",
                "block_range_end",
                "causality_region",
                "manifest_idx",
                "parent",
                "id",
                "param",
                "context",
                "done_at"
            ]
        );

        // Check nullability
        assert!(!schema.field_with_name("vid").unwrap().is_nullable());
        assert!(!schema
            .field_with_name("block_range_start")
            .unwrap()
            .is_nullable());
        assert!(schema
            .field_with_name("block_range_end")
            .unwrap()
            .is_nullable());
        assert!(!schema
            .field_with_name("causality_region")
            .unwrap()
            .is_nullable());
        assert!(!schema
            .field_with_name("manifest_idx")
            .unwrap()
            .is_nullable());
        assert!(schema.field_with_name("parent").unwrap().is_nullable());
        assert!(schema.field_with_name("id").unwrap().is_nullable());
        assert!(schema.field_with_name("param").unwrap().is_nullable());
        assert!(schema.field_with_name("context").unwrap().is_nullable());
        assert!(schema.field_with_name("done_at").unwrap().is_nullable());

        // Check types
        assert_eq!(
            schema.field_with_name("vid").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("context").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("param").unwrap().data_type(),
            &DataType::Binary
        );
    }

    #[test]
    fn enum_columns_map_to_utf8() {
        let layout = test_layout(
            "enum Status { Active, Inactive } \
             type Item @entity(immutable: true) { id: ID!, status: Status! }",
        );
        let table = layout.table("item").unwrap();
        let schema = arrow_schema(table);

        assert_eq!(
            schema.field_with_name("status").unwrap().data_type(),
            &DataType::Utf8
        );
    }
}
