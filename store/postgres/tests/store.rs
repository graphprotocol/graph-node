extern crate diesel;
extern crate futures;
#[macro_use]
extern crate lazy_static;
extern crate graph;
extern crate graph_store_postgres;
extern crate hex;

use diesel::pg::PgConnection;
use diesel::*;
use std::collections::HashSet;
use std::fmt::Debug;
use std::iter::FromIterator;
use std::str::FromStr;
use std::sync::Mutex;

use graph::components::store::{EntityFilter, EntityKey, EntityOrder, EntityQuery, EntityRange};
use graph::data::store::scalar;
use graph::prelude::*;
use graph::web3::types::H256;
use graph_store_postgres::{db_schema, Store as DieselStore, StoreConfig};

/// Helper function to ensure and obtain the Postgres URL to use for testing.
fn postgres_test_url() -> String {
    std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("The THEGRAPH_STORE_POSTGRES_DIESEL_URL environment variable is not set")
        .into_string()
        .unwrap()
}

lazy_static! {
    static ref TEST_MUTEX: Mutex<()> = Mutex::new(());
    static ref TEST_SUBGRAPH_ID: SubgraphId = SubgraphId::new("testsubgraph").unwrap();
    static ref TEST_BLOCK_0_PTR: EthereumBlockPointer = (
        H256::from("0xbd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f"),
        0u64
    )
        .into();
    static ref TEST_BLOCK_1_PTR: EthereumBlockPointer = (
        H256::from("0x8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13"),
        1u64
    )
        .into();
    static ref TEST_BLOCK_2_PTR: EthereumBlockPointer = (
        H256::from("0xb98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1"),
        2u64
    )
        .into();
    static ref TEST_BLOCK_3_PTR: EthereumBlockPointer = (
        H256::from("0x977c084229c72a0fa377cae304eda9099b6a2cb5d83b25cdf0f0969b69874255"),
        3u64
    )
        .into();
    static ref TEST_BLOCK_3A_PTR: EthereumBlockPointer = (
        H256::from("0xd163aec0592c7cb00c2700ab65dcaac93289f5d250b3b889b39198b07e1fbe4a"),
        3u64
    )
        .into();
    static ref TEST_BLOCK_4_PTR: EthereumBlockPointer = (
        H256::from("0x007a03cdf635ebb66f5e79ae66cc90ca23d98031665649db056ff9c6aac2d74d"),
        4u64
    )
        .into();
    static ref TEST_BLOCK_4A_PTR: EthereumBlockPointer = (
        H256::from("0x8fab27e9e9285b0a39110f4d9877f05d0f43d2effa157e55f4dcc49c3cf8cbd7"),
        4u64
    )
        .into();
    static ref TEST_BLOCK_5_PTR: EthereumBlockPointer = (
        H256::from("0xe8b3b02b936c4a4a331ac691ac9a86e197fb7731f14e3108602c87d4dac55160"),
        5u64
    )
        .into();
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(Arc<DieselStore>) -> R + Send + 'static,
    R: IntoFuture + Send + 'static,
    R::Item: Send,
    R::Error: Send + Debug,
    R::Future: Send,
{
    // Lock regardless of poisoning.
    let _test_lock = match TEST_MUTEX.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(future::lazy(move || {
            // Set up Store
            let logger = Logger::root(slog::Discard, o!());
            let postgres_url = postgres_test_url();
            let net_identifiers = EthereumNetworkIdentifier {
                net_version: "graph test suite".to_owned(),
                genesis_block_hash: TEST_BLOCK_0_PTR.hash,
            };
            let network_name = "fake_network".to_owned();
            let store = Arc::new(DieselStore::new(
                StoreConfig {
                    postgres_url,
                    network_name,
                },
                &logger,
                net_identifiers,
            ));

            // Reset state before starting
            remove_test_data();

            // Seed database with test data
            insert_test_data(store.clone());

            // Run test
            test(store.clone())
        }))
        .expect("Failed to run Store test");
}

/// Inserts test data into the store.
///
/// Inserts data in test blocks 1, 2, and 3, leaving test blocks 3A, 4, and 4A for the tests to
/// use.
fn insert_test_data(store: Arc<DieselStore>) {
    store
        .add_subgraph_if_missing(TEST_SUBGRAPH_ID.clone(), *TEST_BLOCK_0_PTR)
        .unwrap();

    let test_entity_1 = create_test_entity(
        "1",
        "user",
        "Johnton",
        "tonofjohn@email.com",
        67 as i32,
        184.4 as f32,
        false,
    );
    store
        .transact_block_operations(
            TEST_SUBGRAPH_ID.clone(),
            *TEST_BLOCK_0_PTR,
            *TEST_BLOCK_1_PTR,
            vec![test_entity_1],
        )
        .unwrap();

    let test_entity_2 = create_test_entity(
        "2",
        "user",
        "Cindini",
        "dinici@email.com",
        43 as i32,
        159.1 as f32,
        true,
    );
    let test_entity_3_1 = create_test_entity(
        "3",
        "user",
        "Shaqueeena",
        "queensha@email.com",
        28 as i32,
        111.7 as f32,
        false,
    );
    store
        .transact_block_operations(
            TEST_SUBGRAPH_ID.clone(),
            *TEST_BLOCK_1_PTR,
            *TEST_BLOCK_2_PTR,
            vec![test_entity_2, test_entity_3_1],
        )
        .unwrap();

    let test_entity_3_2 = create_test_entity(
        "3",
        "user",
        "Shaqueeena",
        "teeko@email.com",
        28 as i32,
        111.7 as f32,
        false,
    );
    store
        .transact_block_operations(
            TEST_SUBGRAPH_ID.clone(),
            *TEST_BLOCK_2_PTR,
            *TEST_BLOCK_3_PTR,
            vec![test_entity_3_2],
        )
        .unwrap();
}

/// Creates a test entity.
fn create_test_entity(
    id: &str,
    entity_type: &str,
    name: &str,
    email: &str,
    age: i32,
    weight: f32,
    coffee: bool,
) -> EntityOperation {
    let mut test_entity = Entity::new();

    test_entity.insert("id".to_owned(), Value::String(id.to_owned()));
    test_entity.insert("name".to_owned(), Value::String(name.to_owned()));
    let bin_name = scalar::Bytes::from_str(&hex::encode(name)).unwrap();
    test_entity.insert("bin_name".to_owned(), Value::Bytes(bin_name));
    test_entity.insert("email".to_owned(), Value::String(email.to_owned()));
    test_entity.insert("age".to_owned(), Value::Int(age));
    test_entity.insert("weight".to_owned(), Value::Float(weight));
    test_entity.insert("coffee".to_owned(), Value::Bool(coffee));

    EntityOperation::Set {
        key: EntityKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: entity_type.to_owned(),
            entity_id: id.to_owned(),
        },
        data: test_entity,
    }
}

/// Removes test data from the database behind the store.
fn remove_test_data() {
    use db_schema::entities;
    use db_schema::subgraph_deployments;
    use db_schema::subgraphs;

    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");
    delete(entities::table)
        .execute(&conn)
        .expect("Failed to remove entity test data");
    delete(subgraphs::table)
        .execute(&conn)
        .expect("Failed to remove subgraph test data");
    delete(subgraph_deployments::table)
        .execute(&conn)
        .expect("Failed to remove subgraph test data");
}

#[test]
fn delete_entity() {
    run_test(|store| -> Result<(), ()> {
        use db_schema::entities::dsl::*;

        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![EntityOperation::Remove {
                    key: EntityKey {
                        subgraph_id: TEST_SUBGRAPH_ID.clone(),
                        entity_type: "user".to_owned(),
                        entity_id: "3".to_owned(),
                    },
                }],
            )
            .unwrap();

        // Get all ids in table
        let all_ids = entities
            .select(id)
            .load::<String>(&*store.conn.lock().unwrap())
            .unwrap();

        // Check that that the deleted entity id is not present
        assert!(!all_ids.contains(&"3".to_owned()));

        Ok(())
    })
}

#[test]
fn get_entity() {
    run_test(|store| -> Result<(), ()> {
        let key = EntityKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            entity_id: "1".to_owned(),
        };
        let result = store.get(key).unwrap();

        let mut expected_entity = Entity::new();

        expected_entity.insert("id".to_owned(), "1".into());
        expected_entity.insert("name".to_owned(), "Johnton".into());
        expected_entity.insert(
            "bin_name".to_owned(),
            Value::Bytes("Johnton".as_bytes().into()),
        );
        expected_entity.insert("email".to_owned(), "tonofjohn@email.com".into());
        expected_entity.insert("age".to_owned(), Value::Int(67 as i32));
        expected_entity.insert("weight".to_owned(), Value::Float(184.4 as f32));
        expected_entity.insert("coffee".to_owned(), Value::Bool(false));

        // Check that the expected entity was returned
        assert_eq!(result, Some(expected_entity));

        Ok(())
    })
}

#[test]
fn insert_entity() {
    run_test(|store| -> Result<(), ()> {
        use db_schema::entities::dsl::*;

        let test_entity = create_test_entity(
            "7",
            "user",
            "Wanjon",
            "wanawana@email.com",
            76 as i32,
            111.7 as f32,
            true,
        );
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![test_entity],
            )
            .unwrap();

        // Check that new record is in the store
        let all_ids = entities
            .select(id)
            .load::<String>(&*store.conn.lock().unwrap())
            .unwrap();
        assert!(all_ids.iter().any(|x| x == &"7".to_owned()));

        Ok(())
    })
}

#[test]
fn update_existing() {
    run_test(|store| -> Result<(), ()> {
        let entity_key = EntityKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            entity_id: "1".to_owned(),
        };

        let op = create_test_entity(
            "1",
            "user",
            "Wanjon",
            "wanawana@email.com",
            76 as i32,
            111.7 as f32,
            true,
        );
        let mut new_data = match op {
            EntityOperation::Set { ref data, .. } => data.clone(),
            _ => unreachable!(),
        };

        // Verify that the entity before updating is different from what we expect afterwards
        assert_ne!(store.get(entity_key.clone()).unwrap().unwrap(), new_data);

        // Set test entity; as the entity already exists an update should be performed
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![op],
            )
            .unwrap();

        // Verify that the entity in the store has changed to what we have set.
        let bin_name = match new_data.get("bin_name") {
            Some(Value::Bytes(bytes)) => bytes.clone(),
            _ => unreachable!(),
        };
        new_data.insert("bin_name".to_owned(), Value::Bytes(bin_name));
        assert_eq!(store.get(entity_key).unwrap(), Some(new_data));

        Ok(())
    })
}

#[test]
fn partially_update_existing() {
    run_test(|store| -> Result<(), ()> {
        let entity_key = EntityKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            entity_id: "1".to_owned(),
        };

        let partial_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny Boy")),
            ("email", Value::Null),
        ]);

        let original_entity = store
            .get(entity_key.clone())
            .unwrap()
            .expect("entity not found");

        // Set test entity; as the entity already exists an update should be performed
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![EntityOperation::Set {
                    key: entity_key.clone(),
                    data: partial_entity.clone(),
                }],
            )
            .unwrap();

        // Obtain the updated entity from the store
        let updated_entity = store.get(entity_key).unwrap().expect("entity not found");

        // Verify that the values of all attributes we have set were either unset
        // (in the case of Value::Null) or updated to the new values
        assert_eq!(updated_entity.get("id"), partial_entity.get("id"));
        assert_eq!(updated_entity.get("user"), partial_entity.get("user"));
        assert_eq!(updated_entity.get("email"), None);

        // Verify that all attributes we have not set have remained at their old values
        assert_eq!(updated_entity.get("age"), original_entity.get("age"));
        assert_eq!(updated_entity.get("weight"), original_entity.get("weight"));
        assert_eq!(updated_entity.get("coffee"), original_entity.get("coffee"));

        Ok(())
    })
}

fn test_find(expected_entity_ids: Vec<&str>, query: EntityQuery) {
    let expected_entity_ids: Vec<String> =
        expected_entity_ids.into_iter().map(str::to_owned).collect();

    run_test(move |store| -> Result<(), ()> {
        let entities = store
            .find(query)
            .expect("store.find failed to execute query");

        let entity_ids: Vec<_> = entities
            .into_iter()
            .map(|entity| match entity.get("id") {
                Some(Value::String(id)) => id.to_owned(),
                Some(_) => panic!("store.find returned entity with non-string ID attribute"),
                None => panic!("store.find returned entity with no ID attribute"),
            })
            .collect();

        assert_eq!(entity_ids, expected_entity_ids);

        Ok(())
    })
}

#[test]
fn find_string_contains() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Contains(
                "name".into(),
                "%ind%".into(),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        },
    )
}

#[test]
fn find_string_equal() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "name".to_owned(),
                "Cindini".into(),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        },
    )
}

#[test]
fn find_string_not_equal() {
    test_find(
        vec!["1", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "name".to_owned(),
                "Cindini".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn find_string_greater_than() {
    test_find(
        vec!["3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterThan(
                "name".to_owned(),
                "Kundi".into(),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        },
    )
}

#[test]
fn find_string_less_than_order_by_asc() {
    test_find(
        vec!["2", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "name".to_owned(),
                "Kundi".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn find_string_less_than_order_by_desc() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "name".to_owned(),
                "Kundi".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_string_less_than_range() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "name".to_owned(),
                "ZZZ".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 1, skip: 1 }),
        },
    )
}

#[test]
fn find_string_multiple_and() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![
                EntityFilter::LessThan("name".to_owned(), "Cz".into()),
                EntityFilter::Equal("name".to_owned(), "Cindini".into()),
            ])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_string_ends_with() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::EndsWith(
                "name".to_owned(),
                "ini".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_string_not_ends_with() {
    test_find(
        vec!["3", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::NotEndsWith(
                "name".to_owned(),
                "ini".into(),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_string_in() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "name".to_owned(),
                vec!["Johnton".into()],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_string_not_in() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "name".to_owned(),
                vec!["Shaqueeena".into()],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_float_equal() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "weight".to_owned(),
                Value::Float(184.4 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        },
    )
}

#[test]
fn find_float_not_equal() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "weight".to_owned(),
                Value::Float(184.4 as f32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_float_greater_than() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterThan(
                "weight".to_owned(),
                Value::Float(160 as f32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        },
    )
}

#[test]
fn find_float_less_than() {
    test_find(
        vec!["2", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "weight".to_owned(),
                Value::Float(160 as f32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn find_float_less_than_order_by_desc() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "weight".to_owned(),
                Value::Float(160 as f32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_float_less_than_range() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "weight".to_owned(),
                Value::Float(161 as f32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 1, skip: 1 }),
        },
    )
}

#[test]
fn find_float_in() {
    test_find(
        vec!["3", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "weight".to_owned(),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 5, skip: 0 }),
        },
    )
}

#[test]
fn find_float_not_in() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "weight".to_owned(),
                vec![Value::Float(184.4 as f32), Value::Float(111.7 as f32)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 5, skip: 0 }),
        },
    )
}

#[test]
fn find_int_equal() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "age".to_owned(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_int_not_equal() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "age".to_owned(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_int_greater_than() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterThan(
                "age".to_owned(),
                Value::Int(43 as i32),
            )])),
            order_by: None,
            order_direction: None,
            range: None,
        },
    )
}

#[test]
fn find_int_greater_or_equal() {
    test_find(
        vec!["2", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::GreaterOrEqual(
                "age".to_owned(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn find_int_less_than() {
    test_find(
        vec!["2", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "age".to_owned(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn find_int_less_or_equal() {
    test_find(
        vec!["2", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessOrEqual(
                "age".to_owned(),
                Value::Int(43 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn find_int_less_than_order_by_desc() {
    test_find(
        vec!["3", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "age".to_owned(),
                Value::Int(50 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_int_less_than_range() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::LessThan(
                "age".to_owned(),
                Value::Int(67 as i32),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 1, skip: 1 }),
        },
    )
}

#[test]
fn find_int_in() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "age".to_owned(),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 5, skip: 0 }),
        },
    )
}

#[test]
fn find_int_not_in() {
    test_find(
        vec!["3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "age".to_owned(),
                vec![Value::Int(67 as i32), Value::Int(43 as i32)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 5, skip: 0 }),
        },
    )
}

#[test]
fn find_bool_equal() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "coffee".to_owned(),
                Value::Bool(true),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_bool_not_equal() {
    test_find(
        vec!["1", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Not(
                "coffee".to_owned(),
                Value::Bool(true),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn find_bool_in() {
    test_find(
        vec!["2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::In(
                "coffee".to_owned(),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 5, skip: 0 }),
        },
    )
}

#[test]
fn find_bool_not_in() {
    test_find(
        vec!["3", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::NotIn(
                "coffee".to_owned(),
                vec![Value::Bool(true)],
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: Some(EntityRange { first: 5, skip: 0 }),
        },
    )
}

#[test]
fn find_bytes_equal() {
    test_find(
        vec!["1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "bin_name".to_owned(),
                Value::Bytes("Johnton".as_bytes().into()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    )
}

#[test]
fn find_order_by_float() {
    test_find(
        vec!["3", "2", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("weight".to_owned(), ValueType::Float)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    );
    test_find(
        vec!["1", "2", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("weight".to_owned(), ValueType::Float)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    );
}

#[test]
fn find_order_by_id() {
    test_find(
        vec!["1", "2", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("id".to_owned(), ValueType::ID)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    );
    test_find(
        vec!["3", "2", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("id".to_owned(), ValueType::ID)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    );
}

#[test]
fn find_order_by_int() {
    test_find(
        vec!["3", "2", "1"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("age".to_owned(), ValueType::Int)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    );
    test_find(
        vec!["1", "2", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("age".to_owned(), ValueType::Int)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    );
}

#[test]
fn find_order_by_string() {
    test_find(
        vec!["2", "1", "3"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    );
    test_find(
        vec!["3", "1", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: None,
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        },
    );
}

#[test]
fn find_where_nested_and_or() {
    test_find(
        vec!["1", "2"],
        EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Or(vec![
                EntityFilter::Equal("id".to_owned(), Value::from("1")),
                EntityFilter::Equal("id".to_owned(), Value::from("2")),
            ])])),
            order_by: Some(("id".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Ascending),
            range: None,
        },
    )
}

#[test]
fn revert_block() {
    run_test(|store| -> Result<(), ()> {
        let this_query = EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "name".to_owned(),
                Value::String("Shaqueeena".to_owned()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        };

        // Revert block 3
        store
            .revert_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_2_PTR,
            )
            .unwrap();

        let returned_entities = store
            .find(this_query.clone())
            .expect("store.find operation failed");

        // There should be 1 user returned in results
        assert_eq!(1, returned_entities.len());

        // Check if the first user in the result vector has email "queensha@email.com"
        let returned_name = returned_entities[0].get(&"email".to_owned());
        let test_value = Value::String("queensha@email.com".to_owned());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
fn revert_block_with_delete() {
    run_test(|store| -> Result<(), ()> {
        let this_query = EntityQuery {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            filter: Some(EntityFilter::And(vec![EntityFilter::Equal(
                "name".to_owned(),
                Value::String("Cindini".to_owned()),
            )])),
            order_by: Some(("name".to_owned(), ValueType::String)),
            order_direction: Some(EntityOrder::Descending),
            range: None,
        };

        // Delete entity with id=2
        let del_key = EntityKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            entity_id: "2".to_owned(),
        };

        // Process deletion
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![EntityOperation::Remove { key: del_key }],
            )
            .unwrap();

        // Revert deletion
        store
            .revert_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_4_PTR,
                *TEST_BLOCK_3_PTR,
            )
            .unwrap();

        // Query after revert
        let returned_entities = store
            .find(this_query.clone())
            .expect("store.find operation failed");

        // There should be 1 entity returned in results
        assert_eq!(1, returned_entities.len());

        // Check if "dinici@email.com" is in result set
        let returned_name = returned_entities[0].get(&"email".to_owned());
        let test_value = Value::String("dinici@email.com".to_owned());
        assert!(returned_name.is_some());
        assert_eq!(&test_value, returned_name.unwrap());

        Ok(())
    })
}

#[test]
fn revert_block_with_partial_update() {
    run_test(|store| -> Result<(), ()> {
        let entity_key = EntityKey {
            subgraph_id: TEST_SUBGRAPH_ID.clone(),
            entity_type: "user".to_owned(),
            entity_id: "1".to_owned(),
        };

        let partial_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny Boy")),
            ("email", Value::Null),
        ]);

        let original_entity = store
            .get(entity_key.clone())
            .unwrap()
            .expect("missing entity");

        // Set test entity; as the entity already exists an update should be performed
        store
            .transact_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_3_PTR,
                *TEST_BLOCK_4_PTR,
                vec![EntityOperation::Set {
                    key: entity_key.clone(),
                    data: partial_entity.clone(),
                }],
            )
            .unwrap();

        // Perform revert operation, reversing the partial update
        store
            .revert_block_operations(
                TEST_SUBGRAPH_ID.clone(),
                *TEST_BLOCK_4_PTR,
                *TEST_BLOCK_3_PTR,
            )
            .unwrap();

        // Obtain the reverted entity from the store
        let reverted_entity = store
            .get(entity_key.clone())
            .unwrap()
            .expect("missing entity");

        // Verify that the entity has been returned to its original state
        assert_eq!(reverted_entity, original_entity);

        Ok(())
    })
}

// Disabled due to issue #332
#[test]
#[ignore]
fn entity_changes_are_fired_and_forwarded_to_subscriptions() {
    run_test(|store| {
        let subgraph_id = SubgraphId::new("EntityChangeTestSubgraph").unwrap();
        store
            .add_subgraph_if_missing(subgraph_id.clone(), *TEST_BLOCK_0_PTR)
            .unwrap();

        // Create a store subscription
        let subscription = store.subscribe(vec![(subgraph_id.clone(), "User".to_owned())]);

        // Add two entities to the store
        let added_entities = vec![
            (
                "1".to_owned(),
                Entity::from(vec![
                    ("id", Value::from("1")),
                    ("name", Value::from("Johnny Boy")),
                ]),
            ),
            (
                "2".to_owned(),
                Entity::from(vec![
                    ("id", Value::from("2")),
                    ("name", Value::from("Tessa")),
                ]),
            ),
        ];
        store
            .transact_block_operations(
                subgraph_id.clone(),
                *TEST_BLOCK_0_PTR,
                *TEST_BLOCK_1_PTR,
                added_entities
                    .iter()
                    .map(|(id, data)| EntityOperation::Set {
                        key: EntityKey {
                            subgraph_id: subgraph_id.clone(),
                            entity_type: "user".to_owned(),
                            entity_id: id.to_owned(),
                        },
                        data: data.to_owned(),
                    })
                    .collect(),
            )
            .unwrap();

        // Update an entity in the store
        let updated_entity = Entity::from(vec![
            ("id", Value::from("1")),
            ("name", Value::from("Johnny")),
        ]);
        let update_op = EntityOperation::Set {
            key: EntityKey {
                subgraph_id: subgraph_id.clone(),
                entity_type: "User".to_owned(),
                entity_id: "1".to_owned(),
            },
            data: updated_entity.clone(),
        };

        // Delete an entity in the store
        let delete_op = EntityOperation::Remove {
            key: EntityKey {
                subgraph_id: subgraph_id.clone(),
                entity_type: "User".to_owned(),
                entity_id: "2".to_owned(),
            },
        };

        // Commit update & delete ops
        store
            .transact_block_operations(
                subgraph_id.clone(),
                *TEST_BLOCK_1_PTR,
                *TEST_BLOCK_2_PTR,
                vec![update_op, delete_op],
            )
            .unwrap();

        // We're expecting four events to be written to the subscription stream
        subscription
            .take(4)
            .collect()
            .and_then(move |changes| {
                // Keep the store around until we're done reading from it; otherwise
                // it would be dropped too early and its entity change listener would
                // be terminated as well
                let _store = store;

                assert_eq!(
                    changes,
                    vec![
                        EntityChange {
                            subgraph_id: subgraph_id.clone(),
                            entity_type: "User".to_owned(),
                            entity_id: added_entities[0].clone().0,
                            operation: EntityChangeOperation::Added,
                        },
                        EntityChange {
                            subgraph_id: subgraph_id.clone(),
                            entity_type: "User".to_owned(),
                            entity_id: added_entities[1].clone().0,
                            operation: EntityChangeOperation::Added,
                        },
                        EntityChange {
                            subgraph_id: subgraph_id.clone(),
                            entity_type: "User".to_owned(),
                            entity_id: "1".to_owned(),
                            operation: EntityChangeOperation::Updated,
                        },
                        EntityChange {
                            subgraph_id: subgraph_id.clone(),
                            entity_type: "User".to_owned(),
                            entity_id: added_entities[1].clone().0,
                            operation: EntityChangeOperation::Removed,
                        },
                    ]
                );

                Ok(())
            })
            .and_then(|_| Ok(()))
    })
}

#[test]
fn write_and_read_deployments() {
    run_test(|store| -> Result<(), ()> {
        let name1 = SubgraphDeploymentName::new("name1").unwrap();
        let name2 = SubgraphDeploymentName::new("name2").unwrap();
        let subgraph_id1 = SubgraphId::new("mysubgraph").unwrap();
        let subgraph_id2 = SubgraphId::new("mysubgraph2").unwrap();
        let subgraph_id3 = SubgraphId::new("mysubgraph3").unwrap();
        let node_id = NodeId::new("thisnode").unwrap();

        store
            .write(name1.clone(), subgraph_id1.clone(), node_id.clone())
            .unwrap();
        store
            .write(name2.clone(), subgraph_id2.clone(), node_id.clone())
            .unwrap();
        assert_eq!(
            store.read(name1.clone()).unwrap(),
            Some((subgraph_id1.clone(), node_id.clone()))
        );
        assert_eq!(
            HashSet::from_iter(store.read_by_node_id(node_id.clone()).unwrap()),
            vec![
                (name1.clone(), subgraph_id1.clone()),
                (name2.clone(), subgraph_id2.clone()),
            ]
            .into_iter()
            .collect::<HashSet<_>>()
        );
        assert_eq!(
            store.read(name2.clone()).unwrap(),
            Some((subgraph_id2.clone(), node_id.clone()))
        );
        store
            .write(name1.clone(), subgraph_id3.clone(), node_id.clone())
            .unwrap();
        assert_eq!(
            store.read(name1.clone()).unwrap(),
            Some((subgraph_id3.clone(), node_id.clone()))
        );
        Ok(())
    })
}

#[test]
fn write_and_delete_deployments() {
    run_test(|store| -> Result<(), ()> {
        let name1 = SubgraphDeploymentName::new("name1").unwrap();
        let name2 = SubgraphDeploymentName::new("name2").unwrap();
        let subgraph_id1 = SubgraphId::new("mysubgraph").unwrap();
        let node_id = NodeId::new("thisnode").unwrap();

        assert_eq!(store.read(name1.clone()).unwrap(), None);

        store
            .write(name1.clone(), subgraph_id1.clone(), node_id.clone())
            .unwrap();

        assert_eq!(
            store.read(name1.clone()).unwrap(),
            Some((subgraph_id1.clone(), node_id.clone()))
        );

        store.remove(name1.clone()).unwrap();

        assert_eq!(
            HashSet::from_iter(store.read_by_node_id(node_id.clone()).unwrap()),
            vec![].into_iter().collect::<HashSet<_>>()
        );
        assert_eq!(store.read(name1.clone()).unwrap(), None);
        assert_eq!(store.read(name2.clone()).unwrap(), None);

        Ok(())
    })
}

#[test]
fn receive_deployment_events() {
    run_test(|store| {
        let name1 = SubgraphDeploymentName::new("name1").unwrap();
        let subgraph_id1 = SubgraphId::new("mysubgraph").unwrap();
        let node_id1 = NodeId::new("thisnode").unwrap();
        let node_id2 = NodeId::new("othernode").unwrap();

        let event_stream_node1 = store.deployment_events(node_id1.clone());
        let event_stream_node2 = store.deployment_events(node_id2.clone());

        store
            .write(name1.clone(), subgraph_id1.clone(), node_id1.clone())
            .unwrap();
        store
            .write(name1.clone(), subgraph_id1.clone(), node_id2.clone())
            .unwrap();
        store.remove(name1.clone()).unwrap();

        event_stream_node1
            .take(2)
            .collect()
            .and_then(move |events| {
                assert_eq!(
                    events,
                    vec![
                        DeploymentEvent::Add {
                            deployment_name: name1.clone(),
                            subgraph_id: subgraph_id1.clone(),
                            node_id: node_id1.clone(),
                        },
                        DeploymentEvent::Remove {
                            deployment_name: name1.clone(),
                            subgraph_id: subgraph_id1.clone(),
                            node_id: node_id1.clone(),
                        },
                    ]
                );

                event_stream_node2
                    .take(2)
                    .collect()
                    .and_then(move |events| {
                        assert_eq!(
                            events,
                            vec![
                                DeploymentEvent::Add {
                                    deployment_name: name1.clone(),
                                    subgraph_id: subgraph_id1.clone(),
                                    node_id: node_id2.clone(),
                                },
                                DeploymentEvent::Remove {
                                    deployment_name: name1.clone(),
                                    subgraph_id: subgraph_id1.clone(),
                                    node_id: node_id2.clone(),
                                },
                            ]
                        );

                        Ok(())
                    })
            })
    })
}
