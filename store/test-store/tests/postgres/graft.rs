use graph::blockchain::block_stream::FirehoseCursor;
use graph::schema::InputSchema;
use graph_store_postgres::command_support::OnSync;
use lazy_static::lazy_static;
use std::{marker::PhantomData, str::FromStr};
use test_store::*;

use graph::components::store::{
    DeploymentLocator, EntityOrder, EntityQuery, PruneReporter, PruneRequest, PruningStrategy,
    VersionStats,
};
use graph::data::store::{Id, scalar};
use graph::data::subgraph::schema::*;
use graph::data::subgraph::*;
use graph::semver::Version;
use graph::{entity, prelude::*};
use graph_store_postgres::{Shard, SubgraphStore as DieselSubgraphStore};

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

const GRAFT_NEW_TABLE_GQL: &str = "
enum Color { yellow, red, blue, green }

type User @entity {
    id: ID!,
    name: String,
    email: String,
    age: Int,
    favorite_color: Color,
}

# An entity type in the dst schema that does not exist in the src schema
type Pet @entity {
    id: ID!,
    name: String,
    age: Int,
}
";

const USER: &str = "User";

lazy_static! {
    static ref TEST_SUBGRAPH_ID: DeploymentHash = DeploymentHash::new("testsubgraph").unwrap();
    static ref TEST_SUBGRAPH_SCHEMA: InputSchema =
        InputSchema::parse_latest(USER_GQL, TEST_SUBGRAPH_ID.clone())
            .expect("Failed to parse user schema");
    static ref BLOCKS: Vec<BlockPtr> = [
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
        remove_subgraphs().await;

        // Seed database with test data
        let deployment = insert_test_data(store.clone()).await;

        flush(&deployment).await.unwrap();

        // Run test
        test(store.cheap_clone(), deployment.clone())
            .await
            .expect("graft test succeeds");

        store
            .cheap_clone()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
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
        spec_version: Version::new(1, 3, 0),
        features: Default::default(),
        description: None,
        repository: None,
        schema: TEST_SUBGRAPH_SCHEMA.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
        chain: PhantomData,
        indexer_hints: None,
    };

    // Create SubgraphDeploymentEntity
    let mut yaml = serde_yaml::Mapping::new();
    yaml.insert("dataSources".into(), Vec::<serde_yaml::Value>::new().into());
    let yaml = serde_yaml::to_string(&yaml).unwrap();
    let deployment = DeploymentCreate::new(yaml, &manifest, None);
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
        .await
        .unwrap();

    let test_entity_1 = create_test_entity(
        "1",
        USER,
        "Johnton",
        "tonofjohn@email.com",
        67_i32,
        184.4,
        false,
        None,
        0,
    );
    transact_entity_operations(&store, &deployment, BLOCKS[0].clone(), vec![test_entity_1])
        .await
        .unwrap();

    let test_entity_2 = create_test_entity(
        "2",
        USER,
        "Cindini",
        "dinici@email.com",
        43_i32,
        159.1,
        true,
        Some("red"),
        1,
    );
    let test_entity_3_1 = create_test_entity(
        "3",
        USER,
        "Shaqueeena",
        "queensha@email.com",
        28_i32,
        111.7,
        false,
        Some("blue"),
        2,
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
        28_i32,
        111.7,
        false,
        None,
        3,
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
    vid: i64,
) -> EntityOperation {
    let bin_name = scalar::Bytes::from_str(&hex::encode(name)).unwrap();
    let test_entity = entity! { TEST_SUBGRAPH_SCHEMA =>
        id: id,
        name: name,
        bin_name: bin_name,
        email: email,
        age: age,
        seconds_age: age * 31557600,
        weight: Value::BigDecimal(weight.into()),
        coffee: coffee,
        favorite_color: favorite_color,
        vid: vid,
    };

    let entity_type = TEST_SUBGRAPH_SCHEMA.entity_type(entity_type).unwrap();
    EntityOperation::Set {
        key: entity_type.parse_key(id).unwrap(),
        data: test_entity,
    }
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

async fn find_entities(
    store: &DieselSubgraphStore,
    deployment: &DeploymentLocator,
) -> (Vec<Entity>, Vec<Id>) {
    let entity_type = TEST_SUBGRAPH_SCHEMA.entity_type(USER).unwrap();
    let query = EntityQuery::new(
        deployment.hash.clone(),
        BLOCK_NUMBER_MAX,
        EntityCollection::All(vec![(entity_type, AttributeNames::All)]),
    )
    .order(EntityOrder::Descending(
        "name".to_string(),
        ValueType::String,
    ));

    let entities = store
        .find(query)
        .await
        .expect("store.find failed to execute query");

    let ids = entities
        .iter()
        .map(|entity| entity.id())
        .collect::<Vec<_>>();
    (entities, ids)
}

async fn check_graft(
    store: Arc<DieselSubgraphStore>,
    deployment: DeploymentLocator,
) -> Result<(), StoreError> {
    let (entities, ids) = find_entities(store.as_ref(), &deployment).await;

    let ids_str = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    assert_eq!(vec!["3", "1", "2"], ids_str);

    // Make sure we caught Shaqueeena at block 1, before the change in
    // email address
    let mut shaq = entities.first().unwrap().clone();
    assert_eq!(Some(&Value::from("queensha@email.com")), shaq.get("email"));

    let schema = store.input_schema(&deployment.hash).await?;
    let user_type = schema.entity_type("User").unwrap();

    // Make our own entries for block 2
    shaq.set("email", "shaq@gmail.com").unwrap();
    let _ = shaq.set_vid(3);
    let op = EntityOperation::Set {
        key: user_type.parse_key("3").unwrap(),
        data: shaq,
    };
    transact_and_wait(&store, &deployment, BLOCKS[2].clone(), vec![op])
        .await
        .unwrap();

    let writable = store
        .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
        .await?;
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

        let (entities, ids) = find_entities(store.as_ref(), &deployment).await;
        let ids_str = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
        assert_eq!(vec!["1"], ids_str);
        let shaq = entities.first().unwrap().clone();
        assert_eq!(Some(&Value::from("tonofjohn@email.com")), shaq.get("email"));
        Ok(())
    })
}

async fn other_shard(
    store: &DieselSubgraphStore,
    src: &DeploymentLocator,
) -> Result<Option<Shard>, StoreError> {
    let src_shard = store.shard(src).await?;

    match all_shards()
        .into_iter()
        .find(|shard| shard.as_str() != src_shard.as_str())
    {
        None => {
            // The tests are configured with just one shard, copying is not possible
            println!("skipping copy test since there is no shard to copy to");
            Ok(None)
        }
        Some(shard) => Ok(Some(shard)),
    }
}

// This test will only do something if the test configuration uses at least
// two shards
#[test]
fn copy() {
    run_test(|store, src| async move {
        if let Some(dst_shard) = other_shard(&store, &src).await? {
            let deployment = store
                .copy_deployment(
                    &src,
                    dst_shard,
                    NODE_ID.clone(),
                    BLOCKS[1].clone(),
                    OnSync::None,
                )
                .await?;

            store
                .cheap_clone()
                .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
                .await?
                .start_subgraph_deployment(&LOGGER)
                .await?;

            store.activate(&deployment).await?;

            check_graft(store, deployment).await?;
        }
        Ok(())
    })
}

// Test that the on_sync behavior is correct when `deployment_synced` gets
// run. This test will only do something if the test configuration uses at
// least two shards
#[test]
fn on_sync() {
    for on_sync in [OnSync::None, OnSync::Activate, OnSync::Replace] {
        run_test(move |store, src| async move {
            if let Some(dst_shard) = other_shard(&store, &src).await? {
                let dst = store
                    .copy_deployment(&src, dst_shard, NODE_ID.clone(), BLOCKS[1].clone(), on_sync)
                    .await?;

                let writable = store
                    .cheap_clone()
                    .writable(LOGGER.clone(), dst.id, Arc::new(Vec::new()))
                    .await?;

                writable.start_subgraph_deployment(&LOGGER).await?;
                writable.deployment_synced(BLOCKS[0].clone()).await?;

                let mut primary = primary_connection().await;
                let src_site = primary.locate_site(src).await?.unwrap();
                let src_node = primary.assigned_node(&src_site).await?;
                let dst_site = primary.locate_site(dst).await?.unwrap();
                let dst_node = primary.assigned_node(&dst_site).await?;

                assert!(dst_node.is_some());
                match on_sync {
                    OnSync::None => {
                        assert!(src_node.is_some());
                        assert!(src_site.active);
                        assert!(!dst_site.active)
                    }
                    OnSync::Activate => {
                        assert!(src_node.is_some());
                        assert!(!src_site.active);
                        assert!(dst_site.active)
                    }
                    OnSync::Replace => {
                        assert!(src_node.is_none());
                        assert!(!src_site.active);
                        assert!(dst_site.active)
                    }
                }
            }
            Ok(())
        })
    }

    // Check that on_sync does not cause an error when the source of the
    // copy has vanished
    run_test(move |store, src| async move {
        if let Some(dst_shard) = other_shard(&store, &src).await? {
            let dst = store
                .copy_deployment(
                    &src,
                    dst_shard,
                    NODE_ID.clone(),
                    BLOCKS[1].clone(),
                    OnSync::Replace,
                )
                .await?;

            let writable = store
                .cheap_clone()
                .writable(LOGGER.clone(), dst.id, Arc::new(Vec::new()))
                .await?;

            // Perform the copy
            writable.start_subgraph_deployment(&LOGGER).await?;

            let mut primary = primary_connection().await;
            let src_site = primary.locate_site(src.clone()).await?.unwrap();
            primary.unassign_subgraph(&src_site).await?;
            store.activate(&dst).await?;
            store.remove_deployment(src.id.into()).await?;

            let res = writable.deployment_synced(BLOCKS[2].clone()).await;
            assert!(res.is_ok());
        }
        Ok(())
    })
}

#[test]
fn prune() {
    struct Progress;
    impl PruneReporter for Progress {}

    async fn check_at_block(
        store: &DieselSubgraphStore,
        src: &DeploymentLocator,
        strategy: PruningStrategy,
        block: BlockNumber,
        exp: Vec<&str>,
    ) {
        let user_type = TEST_SUBGRAPH_SCHEMA.entity_type("User").unwrap();
        let query = EntityQuery::new(
            src.hash.clone(),
            block,
            EntityCollection::All(vec![(user_type.clone(), AttributeNames::All)]),
        );

        let exp = exp
            .into_iter()
            .map(|id| user_type.parse_id(id).unwrap())
            .collect::<Vec<_>>();
        let act: Vec<_> = store
            .find(query)
            .await
            .unwrap()
            .into_iter()
            .map(|entity| entity.id())
            .collect();
        assert_eq!(
            act, exp,
            "different users visible at block {block} with {strategy}"
        );
    }

    for strategy in [PruningStrategy::Rebuild, PruningStrategy::Delete] {
        run_test(move |store, src| async move {
            store
                .set_history_blocks(&src, -3, 10)
                .await
                .expect_err("history_blocks can not be set to a negative number");

            store
                .set_history_blocks(&src, 10, 10)
                .await
                .expect_err("history_blocks must be bigger than reorg_threshold");

            // Add another version for user 2 at block 4
            let user2 = create_test_entity(
                "2",
                USER,
                "Cindini",
                "dinici@email.com",
                44_i32,
                157.1,
                true,
                Some("red"),
                4,
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

            // Forward block ptr to block 6
            transact_and_wait(&store, &src, BLOCKS[6].clone(), vec![])
                .await
                .unwrap();

            // Prune to 3 blocks of history, with a reorg threshold of 1 where
            // we have blocks from [0, 6]. That should only remove the [1,2)
            // version of user 3
            let mut req = PruneRequest::new(&src, 3, 1, 0, 6)?;
            // Change the thresholds so that we select the desired strategy
            match strategy {
                PruningStrategy::Rebuild => {
                    req.rebuild_threshold = 0.0;
                    req.delete_threshold = 0.0;
                }
                PruningStrategy::Delete => {
                    req.rebuild_threshold = 1.0;
                    req.delete_threshold = 0.0;
                }
            }
            // We have 5 versions for 3 entities
            let stats = VersionStats {
                entities: 3,
                versions: 5,
                tablename: USER.to_ascii_lowercase(),
                ratio: 3.0 / 5.0,
                last_pruned_block: None,
                block_range_upper: vec![],
            };
            assert_eq!(
                Some(strategy),
                req.strategy(&stats),
                "changing thresholds didn't yield desired strategy"
            );
            store
                .prune(Box::new(Progress), &src, req)
                .await
                .expect("pruning works");

            // Check which versions exist at every block, even if they are
            // before the new earliest block, since we don't have a convenient
            // way to load all entity versions with their block range
            check_at_block(&store, &src, strategy, 0, vec!["1"]).await;
            check_at_block(&store, &src, strategy, 1, vec!["1", "2"]).await;
            for block in 2..=5 {
                check_at_block(&store, &src, strategy, block, vec!["1", "2", "3"]).await;
            }
            Ok(())
        })
    }
}

/// Sorted names of the postponed indexes on `entity` in `loc`.
async fn postponed_attr_indexes(
    store: &DieselSubgraphStore,
    loc: &DeploymentLocator,
    entity: &str,
) -> Result<Vec<String>, StoreError> {
    let mut names: Vec<String> = store
        .indexes_for_entity(loc, entity)
        .await?
        .into_iter()
        .filter(|idx| idx.to_postpone())
        .filter_map(|idx| idx.name().map(str::to_string))
        .collect();
    names.sort();
    Ok(names)
}

/// Run the postponed index build of `loc` and wait for it to finish.
async fn build_postponed_indexes(
    store: &Arc<DieselSubgraphStore>,
    loc: &DeploymentLocator,
) -> Result<(), StoreError> {
    store
        .cheap_clone()
        .writable(LOGGER.clone(), loc.id, Arc::new(Vec::new()))
        .await?
        .create_postponed_indexes()
        .await?;
    for _ in 0..60 {
        if store.postponed_indexes_created(loc).await? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!(
        "postponed indexes for {} were not created within 30s",
        loc.hash
    );
}

// The build skips a column that is indexed under another name.
#[test]
fn late_build_skips_columns_indexed_under_other_names() {
    run_test(|store, _src| async move {
        let dst_id = DeploymentHash::new("grafted_renamed").unwrap();
        let base = Some((
            DeploymentHash::new(TEST_SUBGRAPH_ID.as_str()).unwrap(),
            BLOCKS[1].clone(),
        ));
        let dst = test_store::create_subgraph_unstarted(&dst_id, GRAFT_GQL, base).await?;
        store
            .create_btree_index_for_deployment(&dst, "attr_9_9_user_age", "user", "age", false)
            .await?;
        test_store::start_subgraph(&dst).await?;
        store.set_postponed_indexes_created(&dst, false).await?;

        build_postponed_indexes(&store, &dst).await?;
        let names = postponed_attr_indexes(&store, &dst, USER).await?;
        // One index each on name, email, age, favorite_color and added.
        assert_eq!(5, names.len(), "{names:?}");
        assert!(names.iter().any(|name| name == "attr_9_9_user_age"));
        Ok(())
    })
}

// A copy replaces a set flag on the graft with the source's unset one.
#[test]
fn graft_clears_flag_set_before_interrupted_copy() {
    run_test(|store, _src| async move {
        let dst_id = DeploymentHash::new("grafted_interrupted").unwrap();
        let base = Some((
            DeploymentHash::new(TEST_SUBGRAPH_ID.as_str()).unwrap(),
            BLOCKS[1].clone(),
        ));
        let dst = test_store::create_subgraph_unstarted(&dst_id, GRAFT_GQL, base).await?;
        store.set_postponed_indexes_created(&dst, true).await?;

        test_store::start_subgraph(&dst).await?;
        assert!(!store.postponed_indexes_created(&dst).await?);
        Ok(())
    })
}

// A copy skips a column that the graft already indexes under the source's
// name.
#[test]
fn graft_resumes_after_indexes_copied_under_source_names() {
    run_test(|store, src| async move {
        build_postponed_indexes(&store, &src).await?;

        let dst_id = DeploymentHash::new("grafted_resumed").unwrap();
        let base = Some((
            DeploymentHash::new(TEST_SUBGRAPH_ID.as_str()).unwrap(),
            BLOCKS[1].clone(),
        ));
        let dst = test_store::create_subgraph_unstarted(&dst_id, GRAFT_GQL, base).await?;
        // `email` is column 3 in the source but column 2 in the graft.
        store
            .create_btree_index_for_deployment(
                &dst,
                "attr_0_3_user_email",
                "user",
                r#"left("email", 256)"#,
                false,
            )
            .await?;

        test_store::start_subgraph(&dst).await?;
        assert_eq!(
            vec![
                "attr_0_1_user_name",
                "attr_0_3_user_age",
                "attr_0_3_user_email",
                "attr_0_4_user_favorite_color",
                "attr_0_5_user_added",
            ],
            postponed_attr_indexes(&store, &dst, USER).await?
        );
        Ok(())
    })
}

// A graft of an unbuilt source isn't marked as built and builds its own
// indexes (#6749).
#[test]
fn graft_before_postponed_indexes() {
    run_test(|store, src| async move {
        assert!(!store.postponed_indexes_created(&src).await?);
        assert_eq!(
            Vec::<String>::new(),
            postponed_attr_indexes(&store, &src, USER).await?
        );

        let dst_id = DeploymentHash::new("grafted_postponed").unwrap();
        let dst = create_grafted_subgraph(
            &dst_id,
            GRAFT_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1].clone(),
        )
        .await?;

        assert!(!store.postponed_indexes_created(&dst).await?);
        assert_eq!(
            vec!["attr_0_5_user_added"],
            postponed_attr_indexes(&store, &dst, USER).await?
        );

        build_postponed_indexes(&store, &src).await?;
        build_postponed_indexes(&store, &dst).await?;
        assert_eq!(
            vec![
                "attr_0_1_user_name",
                "attr_0_2_user_email",
                "attr_0_3_user_age",
                "attr_0_4_user_favorite_color",
                "attr_0_5_user_added",
            ],
            postponed_attr_indexes(&store, &dst, USER).await?
        );
        Ok(())
    })
}

// A graft gets the indexes an unbuilt source has, under the graft's names,
// and its own build doesn't duplicate them.
#[test]
fn graft_copies_postponed_indexes_of_unbuilt_source() {
    run_test(|store, src| async move {
        build_postponed_indexes(&store, &src).await?;
        store.set_postponed_indexes_created(&src, false).await?;

        let dst_id = DeploymentHash::new("grafted_unbuilt").unwrap();
        let dst = create_grafted_subgraph(
            &dst_id,
            GRAFT_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1].clone(),
        )
        .await?;

        let all = vec![
            "attr_0_1_user_name",
            "attr_0_2_user_email",
            "attr_0_3_user_age",
            "attr_0_4_user_favorite_color",
            "attr_0_5_user_added",
        ];
        assert!(!store.postponed_indexes_created(&dst).await?);
        assert_eq!(all, postponed_attr_indexes(&store, &dst, USER).await?);

        build_postponed_indexes(&store, &dst).await?;
        assert_eq!(all, postponed_attr_indexes(&store, &dst, USER).await?);
        Ok(())
    })
}

// Same as `graft_before_postponed_indexes`, for a copy to another shard.
// Needs a test configuration with at least two shards.
#[test]
fn copy_before_postponed_indexes() {
    run_test(|store, src| async move {
        let Some(dst_shard) = other_shard(&store, &src).await? else {
            return Ok(());
        };
        assert!(!store.postponed_indexes_created(&src).await?);

        let dst = store
            .copy_deployment(
                &src,
                dst_shard,
                NODE_ID.clone(),
                BLOCKS[1].clone(),
                OnSync::None,
            )
            .await?;
        store
            .cheap_clone()
            .writable(LOGGER.clone(), dst.id, Arc::new(Vec::new()))
            .await?
            .start_subgraph_deployment(&LOGGER)
            .await?;
        // `indexes_for_entity` looks up the active deployment for a hash.
        store.activate(&dst).await?;

        assert!(!store.postponed_indexes_created(&dst).await?);
        assert_eq!(
            Vec::<String>::new(),
            postponed_attr_indexes(&store, &dst, USER).await?
        );

        build_postponed_indexes(&store, &dst).await?;
        assert_eq!(
            vec![
                "attr_0_1_user_name",
                "attr_0_2_user_bin_name",
                "attr_0_3_user_email",
                "attr_0_4_user_age",
                "attr_0_5_user_seconds_age",
                "attr_0_6_user_weight",
                "attr_0_7_user_coffee",
                "attr_0_8_user_favorite_color",
            ],
            postponed_attr_indexes(&store, &dst, USER).await?
        );
        Ok(())
    })
}

// An index dropped on a built source stays dropped on a graft (#6583).
#[test]
fn graft_keeps_dropped_postponed_index() {
    run_test(|store, src| async move {
        build_postponed_indexes(&store, &src).await?;
        store
            .drop_index_for_deployment(&src, "attr_0_3_user_email")
            .await?;

        let dst_id = DeploymentHash::new("grafted_dropped").unwrap();
        let dst = create_grafted_subgraph(
            &dst_id,
            GRAFT_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1].clone(),
        )
        .await?;

        assert!(store.postponed_indexes_created(&dst).await?);
        assert_eq!(
            vec![
                "attr_0_1_user_name",
                "attr_0_3_user_age",
                "attr_0_4_user_favorite_color",
                "attr_0_5_user_added",
            ],
            postponed_attr_indexes(&store, &dst, USER).await?
        );
        Ok(())
    })
}

// A graft ignores an invalid index on its source.
#[test]
fn graft_ignores_invalid_source_index() {
    run_test(|store, src| async move {
        build_postponed_indexes(&store, &src).await?;
        store
            .drop_index_for_deployment(&src, "attr_0_4_user_age")
            .await?;
        // Fails on the two versions of user 3, both aged 28, and leaves an
        // invalid index behind.
        let res = store
            .create_btree_index_for_deployment(&src, "attr_0_4_user_age", "user", "age", true)
            .await;
        assert!(res.is_err());

        let dst_id = DeploymentHash::new("grafted_invalid").unwrap();
        let dst = create_grafted_subgraph(
            &dst_id,
            GRAFT_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1].clone(),
        )
        .await?;

        assert!(store.postponed_indexes_created(&dst).await?);
        assert_eq!(
            vec![
                "attr_0_1_user_name",
                "attr_0_2_user_email",
                "attr_0_4_user_favorite_color",
                "attr_0_5_user_added",
            ],
            postponed_attr_indexes(&store, &dst, USER).await?
        );
        Ok(())
    })
}

// A graft builds the postponed indexes of entity types its source lacks.
#[test]
fn graft_new_table_gets_postponed_indexes() {
    run_test(|store, src| async move {
        build_postponed_indexes(&store, &src).await?;

        let dst_id = DeploymentHash::new("grafted_new_table").unwrap();
        let dst = create_grafted_subgraph(
            &dst_id,
            GRAFT_NEW_TABLE_GQL,
            TEST_SUBGRAPH_ID.as_str(),
            BLOCKS[1].clone(),
        )
        .await?;

        assert!(store.postponed_indexes_created(&dst).await?);
        // Table positions in index names depend on the schema's type order.
        let pet = postponed_attr_indexes(&store, &dst, "Pet").await?;
        assert_eq!(2, pet.len(), "{pet:?}");
        assert!(pet.iter().any(|n| n.ends_with("_pet_name")), "{pet:?}");
        assert!(pet.iter().any(|n| n.ends_with("_pet_age")), "{pet:?}");
        Ok(())
    })
}
