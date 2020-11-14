use diesel::pg::PgConnection;
use diesel::*;
use hex_literal::hex;
use lazy_static::lazy_static;
use std::str::FromStr;
use test_store::*;

use graph::components::store::{EntityKey, EntityOrder, EntityQuery};
use graph::data::store::scalar;
use graph::data::subgraph::schema::*;
use graph::data::subgraph::*;
use graph::prelude::*;
use graph_store_postgres::NetworkStore as DieselStore;
use web3::types::H256;

const USER_GQL: &str = "
enum Color { yellow, red, blue, green }

interface ColorAndAge {
    id: ID!,
    age: Int,
    favorite_color: Color
}

type User implements ColorAndAge @entity {
    id: ID!,
    name: String,
    bin_name: Bytes,
    email: String,
    age: Int,
    seconds_age: BigInt,
    weight: BigDecimal,
    coffee: Boolean,
    favorite_color: Color
}

type Person implements ColorAndAge @entity {
    id: ID!,
    name: String,
    age: Int,
    favorite_color: Color
}
";

const GRAFT_GQL: &str = "
enum Color { yellow, red, blue, green }

type User @entity {
    id: ID!,
    name: String,
    email: String,
    age: Int,
    favorite_color: Color
}
";

const USER: &str = "User";

macro_rules! block_pointer {
    ($hash:expr, $number:expr) => {{
        EthereumBlockPointer::from((H256::from(hex!($hash)), $number as u64))
    }};
}

lazy_static! {
    static ref TEST_SUBGRAPH_ID: SubgraphDeploymentId =
        SubgraphDeploymentId::new("testsubgraph").unwrap();
    static ref TEST_SUBGRAPH_SCHEMA: Schema =
        Schema::parse(USER_GQL, TEST_SUBGRAPH_ID.clone()).expect("Failed to parse user schema");
    static ref BLOCKS: Vec<EthereumBlockPointer> = vec![
        block_pointer!(
            "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f",
            0
        ),
        block_pointer!(
            "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13",
            1
        ),
        block_pointer!(
            "b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1",
            2
        ),
    ];
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(Arc<DieselStore>) -> R + Send + 'static,
    R: IntoFuture<Item = ()> + Send + 'static,
    R::Error: Send + Debug,
    R::Future: Send,
{
    let store = STORE.clone();

    // Lock regardless of poisoning. This also forces sequential test execution.
    let mut runtime = match STORE_RUNTIME.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    runtime
        .block_on(async {
            // Reset state before starting
            remove_test_data(store.clone());

            // Seed database with test data
            insert_test_data(store.clone());

            // Run test
            test(store).into_future().compat().await
        })
        .unwrap_or_else(|e| panic!("Failed to run Store test: {:?}", e));
}

/// Inserts test data into the store.
///
/// Inserts data in test blocks 1, 2, and 3, leaving test blocks 3A, 4, and 4A for the tests to
/// use.
fn insert_test_data(store: Arc<DieselStore>) {
    let manifest = SubgraphManifest {
        id: TEST_SUBGRAPH_ID.clone(),
        location: "/ipfs/test".to_owned(),
        spec_version: "1".to_owned(),
        features: Default::default(),
        description: None,
        repository: None,
        schema: TEST_SUBGRAPH_SCHEMA.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
    };

    // Create SubgraphDeploymentEntity
    let deployment = SubgraphDeploymentEntity::new(&manifest, false, None);
    let name = SubgraphName::new("test/graft").unwrap();
    let node_id = NodeId::new("test").unwrap();
    store
        .create_subgraph_deployment(
            name,
            &TEST_SUBGRAPH_SCHEMA,
            deployment,
            node_id,
            SubgraphVersionSwitchingMode::Instant,
        )
        .unwrap();

    let test_entity_1 = create_test_entity(
        "1",
        USER,
        "Johnton",
        "tonofjohn@email.com",
        67 as i32,
        184.4,
        false,
        None,
    );
    transact_entity_operations(
        &store,
        TEST_SUBGRAPH_ID.clone(),
        BLOCKS[0],
        vec![test_entity_1],
    )
    .unwrap();

    let test_entity_2 = create_test_entity(
        "2",
        USER,
        "Cindini",
        "dinici@email.com",
        43 as i32,
        159.1,
        true,
        Some("red"),
    );
    let test_entity_3_1 = create_test_entity(
        "3",
        USER,
        "Shaqueeena",
        "queensha@email.com",
        28 as i32,
        111.7,
        false,
        Some("blue"),
    );
    transact_entity_operations(
        &store,
        TEST_SUBGRAPH_ID.clone(),
        BLOCKS[1],
        vec![test_entity_2, test_entity_3_1],
    )
    .unwrap();

    let test_entity_3_2 = create_test_entity(
        "3",
        USER,
        "Shaqueeena",
        "teeko@email.com",
        28 as i32,
        111.7,
        false,
        None,
    );
    transact_entity_operations(
        &store,
        TEST_SUBGRAPH_ID.clone(),
        BLOCKS[2],
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
    weight: f64,
    coffee: bool,
    favorite_color: Option<&str>,
) -> EntityOperation {
    let mut test_entity = Entity::new();

    test_entity.insert("id".to_owned(), Value::String(id.to_owned()));
    test_entity.insert("name".to_owned(), Value::String(name.to_owned()));
    let bin_name = scalar::Bytes::from_str(&hex::encode(name)).unwrap();
    test_entity.insert("bin_name".to_owned(), Value::Bytes(bin_name));
    test_entity.insert("email".to_owned(), Value::String(email.to_owned()));
    test_entity.insert("age".to_owned(), Value::Int(age));
    test_entity.insert(
        "seconds_age".to_owned(),
        Value::BigInt(BigInt::from(age) * 31557600.into()),
    );
    test_entity.insert("weight".to_owned(), Value::BigDecimal(weight.into()));
    test_entity.insert("coffee".to_owned(), Value::Bool(coffee));
    test_entity.insert(
        "favorite_color".to_owned(),
        favorite_color
            .map(|s| Value::String(s.to_owned()))
            .unwrap_or(Value::Null),
    );

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
fn remove_test_data(store: Arc<DieselStore>) {
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");
    graph_store_postgres::store::delete_all_entities_for_test_use_only(&store, &conn)
        .expect("Failed to remove entity test data");
}

#[test]
fn graft() {
    run_test(move |store| -> Result<(), ()> {
        const SUBGRAPH: &str = "grafted";
        let subgraph_id = SubgraphDeploymentId::new(SUBGRAPH).unwrap();
        let res = test_store::create_grafted_subgraph(
            &subgraph_id,
            GRAFT_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1],
        );

        assert!(res.is_ok());

        let query = EntityQuery::new(
            subgraph_id.clone(),
            BLOCK_NUMBER_MAX,
            EntityCollection::All(vec![USER.to_owned()]),
        )
        .order(EntityOrder::Descending(
            "name".to_string(),
            ValueType::String,
        ));

        let entities = store
            .find(query)
            .expect("store.find failed to execute query");

        let ids = entities
            .iter()
            .map(|entity| entity.id().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(vec!["3", "1", "2"], ids);

        // Make sure we caught Shqueena at block 1, before the change in
        // email address
        let mut shaq = entities.first().unwrap().to_owned();
        assert_eq!(Some(&Value::from("queensha@email.com")), shaq.get("email"));

        // Make our own entries for block 2
        shaq.set("email", "shaq@gmail.com");
        let op = EntityOperation::Set {
            key: EntityKey {
                subgraph_id: subgraph_id.clone(),
                entity_type: USER.to_owned(),
                entity_id: "3".to_owned(),
            },
            data: shaq,
        };
        transact_entity_operations(&store, subgraph_id.clone(), BLOCKS[2], vec![op]).unwrap();

        store
            .revert_block_operations(subgraph_id.clone(), BLOCKS[2], BLOCKS[1])
            .expect("We can revert a block we just created");

        let err = store
            .revert_block_operations(subgraph_id.clone(), BLOCKS[1], BLOCKS[0])
            .expect_err("Reverting past graft point is not allowed");

        assert!(err.to_string().contains("Can not revert subgraph"));

        Ok(())
    })
}
