use graph::blockchain::block_stream::FirehoseCursor;
use lazy_static::lazy_static;
use std::{marker::PhantomData, str::FromStr};
use test_store::*;

use graph::components::store::{
    DeploymentLocator, EntityKey, EntityOrder, EntityQuery, EntityType, PruneReporter,
};
use graph::data::store::scalar;
use graph::data::subgraph::schema::*;
use graph::data::subgraph::*;
use graph::prelude::*;
use graph::semver::Version;
use graph_store_postgres::SubgraphStore as DieselSubgraphStore;

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
    favorite_color: Color,
    # A column in the dst schema that does not exist in the src schema
    added: String,
}
";

const GRAFT_IMMUTABLE_GQL: &str = "
enum Color { yellow, red, blue, green }

type User @entity(immutable: true) {
    id: ID!,
    name: String,
    email: String,
    age: Int,
    favorite_color: Color,
    # A column in the dst schema that does not exist in the src schema
    added: String,
}
";

const USER: &str = "User";

lazy_static! {
    static ref TEST_SUBGRAPH_ID: DeploymentHash = DeploymentHash::new("testsubgraph").unwrap();
    static ref TEST_SUBGRAPH_SCHEMA: Schema =
        Schema::parse(USER_GQL, TEST_SUBGRAPH_ID.clone()).expect("Failed to parse user schema");
    static ref BLOCKS: Vec<BlockPtr> = vec![
        "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f",
        "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13",
        "b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1",
        "7347afe69254df06729e123610b00b8b11f15cfae3241f9366fb113aec07489c",
        "f8ccbd3877eb98c958614f395dd351211afb9abba187bfc1fb4ac414b099c4a6",
        "7b0ea919e258eb2b119eb32de56b85d12d50ac6a9f7c5909f843d6172c8ba196",
        "6b834521bb753c132fdcf0e1034803ed9068e324112f8750ba93580b393a986b",
        "7cce080f5a49c2997a6cc65fc1cee9910fd8fc3721b7010c0b5d0873e2ac785e"
    ]
    .iter()
    .enumerate()
    .map(|(idx, hash)| BlockPtr::try_from((*hash, idx as i64)).unwrap())
    .collect();
}

/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(Arc<DieselSubgraphStore>, DeploymentLocator) -> R + Send + 'static,
    R: std::future::Future<Output = Result<(), StoreError>> + Send + 'static,
{
    run_test_sequentially(|store| async move {
        let store = store.subgraph_store();

        // Reset state before starting
        remove_test_data(store.clone());

        // Seed database with test data
        let deployment = insert_test_data(store.clone()).await;

        flush(&deployment).await.unwrap();

        // Run test
        test(store.cheap_clone(), deployment.clone())
            .await
            .expect("graft test succeeds");

        store
            .cheap_clone()
            .writable(LOGGER.clone(), deployment.id)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
    })
}

/// Inserts test data into the store.
///
/// Inserts data in test blocks 1, 2, and 3, leaving test blocks 3A, 4, and 4A for the tests to
/// use.
async fn insert_test_data(store: Arc<DieselSubgraphStore>) -> DeploymentLocator {
    let manifest = SubgraphManifest::<graph_chain_ethereum::Chain> {
        id: TEST_SUBGRAPH_ID.clone(),
        spec_version: Version::new(1, 0, 0),
        features: Default::default(),
        description: None,
        repository: None,
        schema: TEST_SUBGRAPH_SCHEMA.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
        chain: PhantomData,
    };

    // Create SubgraphDeploymentEntity
    let deployment = DeploymentCreate::new(&manifest, None);
    let name = SubgraphName::new("test/graft").unwrap();
    let node_id = NodeId::new("test").unwrap();
    let deployment = store
        .create_subgraph_deployment(
            name,
            &TEST_SUBGRAPH_SCHEMA,
            deployment,
            node_id,
            "fake_network".to_string(),
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
    transact_entity_operations(&store, &deployment, BLOCKS[0].clone(), vec![test_entity_1])
        .await
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
        &deployment,
        BLOCKS[1].clone(),
        vec![test_entity_2, test_entity_3_1],
    )
    .await
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
        &deployment,
        BLOCKS[2].clone(),
        vec![test_entity_3_2],
    )
    .await
    .unwrap();

    deployment
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
            entity_type: EntityType::new(entity_type.to_string()),
            entity_id: id.into(),
        },
        data: test_entity,
    }
}

/// Removes test data from the database behind the store.
fn remove_test_data(store: Arc<DieselSubgraphStore>) {
    store
        .delete_all_entities_for_test_use_only()
        .expect("deleting test entities succeeds");
}

async fn create_grafted_subgraph(
    subgraph_id: &DeploymentHash,
    schema: &str,
    base_id: &str,
    base_block: BlockPtr,
) -> Result<DeploymentLocator, StoreError> {
    let base = Some((DeploymentHash::new(base_id).unwrap(), base_block));
    test_store::create_subgraph(subgraph_id, schema, base).await
}

fn find_entities(
    store: &DieselSubgraphStore,
    deployment: &DeploymentLocator,
) -> (Vec<Entity>, Vec<String>) {
    let query = EntityQuery::new(
        deployment.hash.clone(),
        BLOCK_NUMBER_MAX,
        EntityCollection::All(vec![(EntityType::from(USER), AttributeNames::All)]),
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
    (entities, ids)
}

async fn check_graft(
    store: Arc<DieselSubgraphStore>,
    deployment: DeploymentLocator,
) -> Result<(), StoreError> {
    let (entities, ids) = find_entities(store.as_ref(), &deployment);

    assert_eq!(vec!["3", "1", "2"], ids);

    // Make sure we caught Shaqueeena at block 1, before the change in
    // email address
    let mut shaq = entities.first().unwrap().to_owned();
    assert_eq!(Some(&Value::from("queensha@email.com")), shaq.get("email"));

    // Make our own entries for block 2
    shaq.set("email", "shaq@gmail.com");
    let op = EntityOperation::Set {
        key: EntityKey {
            entity_type: EntityType::new(USER.to_owned()),
            entity_id: "3".into(),
        },
        data: shaq,
    };
    transact_and_wait(&store, &deployment, BLOCKS[2].clone(), vec![op])
        .await
        .unwrap();

    let writable = store.writable(LOGGER.clone(), deployment.id).await?;
    writable
        .revert_block_operations(BLOCKS[1].clone(), FirehoseCursor::None)
        .await
        .expect("We can revert a block we just created");
    writable.flush().await.expect("we can revert to BLOCKS[1]");

    let err = {
        match writable
            .revert_block_operations(BLOCKS[0].clone(), FirehoseCursor::None)
            .await
        {
            Ok(()) => writable.flush().await,
            Err(e) => Err(e),
        }
    }
    .expect_err("Reverting past graft point is not allowed");

    assert!(err.to_string().contains("Can not revert subgraph"));

    Ok(())
}

#[test]
fn graft() {
    run_test(|store, _| async move {
        const SUBGRAPH: &str = "grafted";
        const SUBGRAPH_ERR: &str = "grafted_err";
        const SUBGRAPH_OK: &str = "grafted_ok";

        let subgraph_id = DeploymentHash::new(SUBGRAPH).unwrap();

        let deployment = create_grafted_subgraph(
            &subgraph_id,
            GRAFT_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1].clone(),
        )
        .await
        .expect("can create grafted subgraph");

        check_graft(store.clone(), deployment).await.unwrap();

        // The test data has an update for the entity with id 3 at block 1.
        // We can therefore graft immutably onto block 0, but grafting onto
        // block 1 fails because we see the deletion of the old version of
        // the entity
        let subgraph_id = DeploymentHash::new(SUBGRAPH_ERR).unwrap();

        let err = create_grafted_subgraph(
            &subgraph_id,
            GRAFT_IMMUTABLE_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1].clone(),
        )
        .await
        .expect_err("grafting onto block 1 fails");
        assert!(err.to_string().contains("can not be made immutable"));

        let subgraph_id = DeploymentHash::new(SUBGRAPH_OK).unwrap();
        let deployment = create_grafted_subgraph(
            &subgraph_id,
            GRAFT_IMMUTABLE_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[0].clone(),
        )
        .await
        .expect("grafting onto block 0 works");

        let (entities, ids) = find_entities(store.as_ref(), &deployment);
        assert_eq!(vec!["1"], ids);
        let shaq = entities.first().unwrap().to_owned();
        assert_eq!(Some(&Value::from("tonofjohn@email.com")), shaq.get("email"));
        Ok(())
    })
}

// This test will only do something if the test configuration uses at least
// two shards
#[test]
fn copy() {
    run_test(|store, src| async move {
        let src_shard = store.shard(&src)?;

        let dst_shard = match all_shards()
            .into_iter()
            .find(|shard| shard.as_str() != src_shard.as_str())
        {
            None => {
                // The tests are configured with just one shard, copying is not possible
                println!("skipping copy test since there is no shard to copy to");
                return Ok(());
            }
            Some(shard) => shard,
        };

        let deployment =
            store.copy_deployment(&src, dst_shard, NODE_ID.clone(), BLOCKS[1].clone())?;

        store
            .cheap_clone()
            .writable(LOGGER.clone(), deployment.id)
            .await?
            .start_subgraph_deployment(&*LOGGER)
            .await?;

        store.activate(&deployment)?;

        check_graft(store, deployment).await
    })
}

#[test]
fn prune() {
    fn check_at_block(
        store: &DieselSubgraphStore,
        src: &DeploymentLocator,
        block: BlockNumber,
        exp: Vec<&str>,
    ) {
        let query = EntityQuery::new(
            src.hash.clone(),
            block,
            EntityCollection::All(vec![(
                EntityType::new("User".to_string()),
                AttributeNames::All,
            )]),
        );

        let act: Vec<_> = store
            .find(query)
            .unwrap()
            .into_iter()
            .map(|entity| entity.id().unwrap())
            .collect();
        assert_eq!(act, exp);
    }

    async fn prune(
        store: &DieselSubgraphStore,
        src: &DeploymentLocator,
        earliest_block: BlockNumber,
    ) -> Result<(), StoreError> {
        struct Progress;
        impl PruneReporter for Progress {}
        let reporter = Box::new(Progress);

        store
            .prune(reporter, &src, earliest_block, 1, 1.1)
            .await
            .map(|_| ())
    }

    run_test(|store, src| async move {
        // The setup sets the subgraph pointer to block 2, we try to set
        // earliest block to 5
        prune(&store, &src, 5)
            .await
            .expect_err("setting earliest block later than latest does not work");

        // Latest block 2 minus reorg threshold 1 means we need to copy
        // final blocks from block 1, but want earliest as block 2, i.e. no
        // final blocks which won't work
        prune(&store, &src, 2)
            .await
            .expect_err("setting earliest block after last final block fails");

        // Add another version for user 2 at block 4
        let user2 = create_test_entity(
            "2",
            USER,
            "Cindini",
            "dinici@email.com",
            44 as i32,
            157.1,
            true,
            Some("red"),
        );
        transact_and_wait(&store, &src, BLOCKS[5].clone(), vec![user2])
            .await
            .unwrap();

        // Setup and the above addition create these user versions:
        // id | versions
        // ---+---------
        //  1 | [0,)
        //  2 | [1,5) [5,)
        //  3 | [1,2) [2,)

        // Forward block ptr to block 5
        transact_and_wait(&store, &src, BLOCKS[6].clone(), vec![])
            .await
            .unwrap();
        // Pruning only removes the [1,2) version of user 3
        prune(&store, &src, 3).await.expect("pruning works");

        // Check which versions exist at every block, even if they are
        // before the new earliest block, since we don't have a convenient
        // way to load all entity versions with their block range
        check_at_block(&store, &src, 0, vec!["1"]);
        check_at_block(&store, &src, 1, vec!["1", "2"]);
        for block in 2..=5 {
            check_at_block(&store, &src, block, vec!["1", "2", "3"]);
        }
        Ok(())
    })
}
