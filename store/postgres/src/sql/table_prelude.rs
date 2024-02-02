use itertools::Itertools;

use crate::relational::{ColumnType, Layout};

pub fn generate_table_prelude_from_layout(layout: &Layout) -> String {
    let schema = &layout.catalog.site.namespace;
    let ctes = layout
        .tables
        .iter()
        .filter(|(entity, _)| !entity.is_poi())
        .map(|(_, table)| {
            let table_name = table.name.as_str();

            let columns = table
                .columns
                .iter()
                .map(|col| match col.column_type {
                    ColumnType::Bytes => format!("concat('0x', encode({}, 'hex')) AS {}", col.name.as_str(), col.name.as_str()),
                    _ => col.name.to_string(),
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{table_name} AS (SELECT {columns} FROM {schema}.{table_name})",)
        })
        .sorted()
        .collect::<Vec<_>>()
        .join(",\n");
    format!("WITH {ctes}")
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use graph::{data::subgraph::DeploymentHash, schema::InputSchema};

    use crate::{
        layout_for_tests::{make_dummy_site, Namespace},
        relational::{Catalog, Layout},
    };

    const THINGS_GQL: &str = r#"
    type _Schema_ @fulltext(
        name: "userSearch"
        language: en
        algorithm: rank
        include: [
            {
                entity: "User",
                fields: [
                    { name: "name"},
                    { name: "email"},
                ]
            }
        ]
    ) @fulltext(
        name: "userSearch2"
        language: en
        algorithm: rank
        include: [
            {
                entity: "User",
                fields: [
                    { name: "name"},
                    { name: "email"},
                ]
            }
        ]
    ) @fulltext(
        name: "nullableStringsSearch"
        language: en
        algorithm: rank
        include: [
            {
                entity: "NullableStrings",
                fields: [
                    { name: "name"},
                    { name: "description"},
                    { name: "test"},
                ]
            }
        ]
    )

    type Thing @entity {
        id: ID!
        bigThing: Thing!
    }

    enum Color { yellow, red, BLUE }

    type Scalar @entity {
        id: ID,
        bool: Boolean,
        int: Int,
        bigDecimal: BigDecimal,
        bigDecimalArray: [BigDecimal!]!
        string: String,
        strings: [String!],
        bytes: Bytes,
        byteArray: [Bytes!],
        bigInt: BigInt,
        bigIntArray: [BigInt!]!
        color: Color,
        int8: Int8,
    }

    interface Pet {
        id: ID!,
        name: String!
    }

    type Cat implements Pet @entity {
        id: ID!,
        name: String!
    }

    type Dog implements Pet @entity {
        id: ID!,
        name: String!
    }

    type Ferret implements Pet @entity {
        id: ID!,
        name: String!
    }

    type Mink @entity(immutable: true) {
        id: ID!,
        order: Int,
    }

    type User @entity {
        id: ID!,
        name: String!,
        bin_name: Bytes!,
        email: String!,
        age: Int!,
        visits: Int8!
        seconds_age: BigInt!,
        weight: BigDecimal!,
        coffee: Boolean!,
        favorite_color: Color,
        drinks: [String!]
    }

    type NullableStrings @entity {
        id: ID!,
        name: String,
        description: String,
        test: String
    }

    interface BytePet {
        id: Bytes!,
        name: String!
    }

    type ByteCat implements BytePet @entity {
        id: Bytes!,
        name: String!
    }

    type ByteDog implements BytePet @entity {
        id: Bytes!,
        name: String!
    }

    type ByteFerret implements BytePet @entity {
        id: Bytes!,
        name: String!
    }
"#;

    fn test_layout(gql: &str) -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = InputSchema::parse(gql, subgraph.clone()).expect("Test schema invalid");
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
    fn test_generate_table_prelude_from_layout() {
        let layout = test_layout(THINGS_GQL);
        let result = super::generate_table_prelude_from_layout(&layout);
        assert_eq!(
            result,
            r#"WITH byte_cat AS (SELECT concat('0x', encode(id, 'hex')) AS id, name FROM sgd0815.byte_cat),
byte_dog AS (SELECT concat('0x', encode(id, 'hex')) AS id, name FROM sgd0815.byte_dog),
byte_ferret AS (SELECT concat('0x', encode(id, 'hex')) AS id, name FROM sgd0815.byte_ferret),
cat AS (SELECT id, name FROM sgd0815.cat),
dog AS (SELECT id, name FROM sgd0815.dog),
ferret AS (SELECT id, name FROM sgd0815.ferret),
mink AS (SELECT id, order FROM sgd0815.mink),
nullable_strings AS (SELECT id, name, description, test, nullable_strings_search FROM sgd0815.nullable_strings),
scalar AS (SELECT id, bool, int, big_decimal, big_decimal_array, string, strings, concat('0x', encode(bytes, 'hex')) AS bytes, concat('0x', encode(byte_array, 'hex')) AS byte_array, big_int, big_int_array, color, int_8 FROM sgd0815.scalar),
thing AS (SELECT id, big_thing FROM sgd0815.thing),
user AS (SELECT id, name, concat('0x', encode(bin_name, 'hex')) AS bin_name, email, age, visits, seconds_age, weight, coffee, favorite_color, drinks, user_search, user_search_2 FROM sgd0815.user)"#
        );
    }
}
