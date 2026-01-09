use graph::{
    components::{
        server::index_node::VersionInfo,
        store::{DeploymentId, DeploymentLocator, StatusStore},
    },
    data::{
        query::QueryTarget,
        subgraph::{
            schema::{DeploymentCreate, SubgraphError, SubgraphHealth},
            DeploymentFeatures, SubgraphFeature,
        },
    },
    prelude::{
        AssignmentChange, BlockPtr, CheapClone, DeploymentHash, NodeId, QueryStoreManager,
        StoreError, StoreEvent, SubgraphManifest, SubgraphName, SubgraphStore as _,
        SubgraphVersionSwitchingMode, UnfailOutcome,
    },
    schema::InputSchema,
    semver::Version,
};
use graph_store_postgres::layout_for_tests::Connection as Primary;
use graph_store_postgres::SubgraphStore;
use std::{collections::HashSet, marker::PhantomData, sync::Arc};
use test_store::*;

const SUBGRAPH_GQL: &str = "
    type User @entity {
        id: ID!,
        name: String
    }
";

const SUBGRAPH_FEATURES_GQL: &str = "
    type User @entity {
        id: ID!,
        name: String
    }

    type User2 @entity(immutable: true) {
        id: Bytes!,
        name: String
    }

    type Data @entity(timeseries: true) {
        id: Int8!
        timestamp: Timestamp!
        price: BigDecimal!
    }

    type Stats @aggregation(intervals: [\"hour\", \"day\"], source: \"Data\") {
        id: Int8!
        timestamp: Timestamp!
        sum: BigDecimal! @aggregate(fn: \"sum\", arg: \"price\")
    }
";

fn assigned(deployment: &DeploymentLocator) -> AssignmentChange {
    AssignmentChange::set(deployment.clone())
}

fn unassigned(deployment: &DeploymentLocator) -> AssignmentChange {
    AssignmentChange::removed(deployment.clone())
}

async fn get_version_info(store: &Store, subgraph_name: &str) -> VersionInfo {
    let mut primary = primary_connection().await;
    let (current, _) = primary.versions_for_subgraph(subgraph_name).await.unwrap();
    let current = current.unwrap();
    store.version_info(&current).await.unwrap()
}

async fn get_subgraph_features(id: String) -> Option<DeploymentFeatures> {
    let mut primary = primary_connection().await;
    primary.get_subgraph_features(id).await.unwrap()
}

async fn latest_block(store: &Store, deployment_id: DeploymentId) -> BlockPtr {
    store
        .subgraph_store()
        .writable(LOGGER.clone(), deployment_id, Arc::new(Vec::new()))
        .await
        .expect("can get writable")
        .block_ptr()
        .unwrap()
}

#[test]
fn reassign_subgraph() {
    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new("reassignSubgraph").unwrap();
        remove_subgraphs().await;
        create_test_subgraph(&id, SUBGRAPH_GQL).await
    }

    async fn find_assignment(
        store: &SubgraphStore,
        deployment: &DeploymentLocator,
    ) -> Option<String> {
        store
            .assigned_node(deployment)
            .await
            .unwrap()
            .map(|node| node.to_string())
    }

    run_test_sequentially(|store| async move {
        let id = setup().await;
        let store = store.subgraph_store();

        // Check our setup
        let node = find_assignment(store.as_ref(), &id).await;
        let placement = place("test").expect("the test config places deployments");
        if let Some((_, nodes)) = placement {
            // If the test config does not have deployment rules, we can't check
            // anything here. This will happen when the tests do not use
            // a configuration file
            assert_eq!(1, nodes.len());
            let placed_node = nodes.first().map(|node| node.to_string());
            assert_eq!(placed_node, node);
        }

        // Assign to node 'left' twice, the first time we assign from 'test'
        // to 'left', the second time from 'left' to 'left', with the same results
        for _ in 0..2 {
            let node = NodeId::new("left").unwrap();
            let expected = vec![StoreEvent::new(vec![assigned(&id)])];

            let (_, events) =
                tap_store_events(async || store.reassign_subgraph(&id, &node).await.unwrap()).await;
            let node = find_assignment(store.as_ref(), &id).await;
            assert_eq!(Some("left"), node.as_deref());
            assert_eq!(expected, events);
        }
    })
}

#[test]
fn create_subgraph() {
    const SUBGRAPH_NAME: &str = "create/subgraph";

    // Return the versions (not deployments) for a subgraph
    async fn subgraph_versions(primary: &mut Primary) -> (Option<String>, Option<String>) {
        primary.versions_for_subgraph(SUBGRAPH_NAME).await.unwrap()
    }

    async fn deployment_for_version(
        primary: &mut Primary,
        name: Option<String>,
    ) -> Result<Option<String>, StoreError> {
        match name {
            None => Ok(None),
            Some(name) => primary.deployment_for_version(&name).await,
        }
    }

    /// Return the deployment for the current and the pending version of the
    /// subgraph with the given `entity_id`
    async fn subgraph_deployments(primary: &mut Primary) -> (Option<String>, Option<String>) {
        let (current, pending) = subgraph_versions(primary).await;
        (
            deployment_for_version(primary, current).await.unwrap(),
            deployment_for_version(primary, pending).await.unwrap(),
        )
    }

    async fn deploy(
        store: &SubgraphStore,
        id: &str,
        mode: SubgraphVersionSwitchingMode,
    ) -> (DeploymentLocator, HashSet<AssignmentChange>) {
        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let id = DeploymentHash::new(id.to_string()).unwrap();
        let schema = InputSchema::parse_latest(SUBGRAPH_GQL, id.clone()).unwrap();

        let manifest = SubgraphManifest::<graph_chain_ethereum::Chain> {
            id,
            spec_version: Version::new(1, 3, 0),
            features: Default::default(),
            description: None,
            repository: None,
            schema: schema.clone(),
            data_sources: vec![],
            graft: None,
            templates: vec![],
            chain: PhantomData,
            indexer_hints: None,
        };
        let deployment = DeploymentCreate::new(String::new(), &manifest, None);
        let node_id = NodeId::new("left").unwrap();

        let (deployment, events) = tap_store_events(async || {
            store
                .create_subgraph_deployment(
                    name,
                    &schema,
                    deployment,
                    node_id,
                    NETWORK_NAME.to_string(),
                    mode,
                )
                .await
                .unwrap()
        })
        .await;
        let events = events
            .into_iter()
            .flat_map(|event| event.changes.into_iter())
            .collect();
        (deployment, events)
    }

    fn deploy_event(deployment: &DeploymentLocator) -> HashSet<AssignmentChange> {
        let mut changes = HashSet::new();
        changes.insert(assigned(deployment));
        changes
    }

    async fn deployment_synced(
        store: &Arc<SubgraphStore>,
        deployment: &DeploymentLocator,
        block_ptr: BlockPtr,
    ) {
        store
            .cheap_clone()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("can get writable")
            .deployment_synced(block_ptr)
            .await
            .unwrap();
    }

    // Test VersionSwitchingMode::Instant
    run_test_sequentially(|store| async move {
        remove_subgraphs().await;
        let store = store.subgraph_store();

        const MODE: SubgraphVersionSwitchingMode = SubgraphVersionSwitchingMode::Instant;
        const ID1: &str = "instant";
        const ID2: &str = "instant2";
        const ID3: &str = "instant3";

        let mut primary = primary_connection().await;

        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let (_, events) =
            tap_store_events(async || store.create_subgraph(name.clone()).await.unwrap()).await;
        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert!(events.is_empty());
        assert!(current.is_none());
        assert!(pending.is_none());

        // Deploy
        let (deployment1, events) = deploy(store.as_ref(), ID1, MODE).await;
        let expected = deploy_event(&deployment1);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID1), current.as_deref());
        assert!(pending.is_none());

        // Deploying again overwrites current
        let (deployment2, events) = deploy(store.as_ref(), ID2, MODE).await;
        let mut expected = deploy_event(&deployment2);
        expected.insert(unassigned(&deployment1));
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID2), current.as_deref());
        assert!(pending.is_none());

        // Sync deployment
        deployment_synced(&store, &deployment2, GENESIS_PTR.clone()).await;

        // Deploying again still overwrites current
        let (deployment3, events) = deploy(store.as_ref(), ID3, MODE).await;
        let mut expected = deploy_event(&deployment3);
        expected.insert(unassigned(&deployment2));
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID3), current.as_deref());
        assert!(pending.is_none());
    });

    // Test VersionSwitchingMode::Synced
    run_test_sequentially(|store| async move {
        remove_subgraphs().await;
        let store = store.subgraph_store();

        const MODE: SubgraphVersionSwitchingMode = SubgraphVersionSwitchingMode::Synced;
        const ID1: &str = "synced";
        const ID2: &str = "synced2";
        const ID3: &str = "synced3";

        let mut primary = primary_connection().await;

        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let (_, events) =
            tap_store_events(async || store.create_subgraph(name.clone()).await.unwrap()).await;
        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert!(events.is_empty());
        assert!(current.is_none());
        assert!(pending.is_none());

        // Deploy
        let (deployment1, events) = deploy(store.as_ref(), ID1, MODE).await;
        let expected = deploy_event(&deployment1);
        assert_eq!(expected, events);

        let versions = subgraph_versions(&mut primary).await;
        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID1), current.as_deref());
        assert!(pending.is_none());

        // Deploying the same thing again does nothing
        let (deployment1_again, events) = deploy(store.as_ref(), ID1, MODE).await;
        assert!(events.is_empty());
        assert_eq!(&deployment1, &deployment1_again);
        let versions2 = subgraph_versions(&mut primary).await;
        assert_eq!(versions, versions2);

        // Deploy again, current is not synced, so it gets replaced
        let (deployment2, events) = deploy(store.as_ref(), ID2, MODE).await;
        let mut expected = deploy_event(&deployment2);
        expected.insert(unassigned(&deployment1));
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID2), current.as_deref());
        assert!(pending.is_none());

        // Deploy when current is synced leaves current alone and adds pending
        deployment_synced(&store, &deployment2, GENESIS_PTR.clone()).await;
        let (deployment3, events) = deploy(store.as_ref(), ID3, MODE).await;
        let expected = deploy_event(&deployment3);
        assert_eq!(expected, events);

        let versions = subgraph_versions(&mut primary).await;
        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID2), current.as_deref());
        assert_eq!(Some(ID3), pending.as_deref());

        // Deploying that same thing again changes nothing
        let (deployment3_again, events) = deploy(store.as_ref(), ID3, MODE).await;
        assert!(events.is_empty());
        assert_eq!(&deployment3, &deployment3_again);
        let versions2 = subgraph_versions(&mut primary).await;
        assert_eq!(versions, versions2);
        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID2), current.as_deref());
        assert_eq!(Some(ID3), pending.as_deref());

        // Deploy the current version `ID2` once more; since it is synced,
        // it will displace the non-synced version `ID3` and remain the
        // current version
        let mut expected = HashSet::new();
        expected.insert(unassigned(&deployment3));

        let (deployment2_again, events) = deploy(store.as_ref(), ID2, MODE).await;
        assert_eq!(&deployment2, &deployment2_again);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID2), current.as_deref());
        assert_eq!(None, pending.as_deref());

        // Mark `ID3` as synced and deploy that again
        deployment_synced(&store, &deployment3, GENESIS_PTR.clone()).await;
        let expected = HashSet::from([unassigned(&deployment2), assigned(&deployment3)]);
        let (deployment3_again, events) = deploy(store.as_ref(), ID3, MODE).await;
        assert_eq!(&deployment3, &deployment3_again);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&mut primary).await;
        assert_eq!(Some(ID3), current.as_deref());
        assert_eq!(None, pending.as_deref());
    })
}

#[test]
fn status() {
    const NAME: &str = "infoSubgraph";
    const OTHER: &str = "otherInfoSubgraph";

    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs().await;
        let deployment = create_test_subgraph(&id, SUBGRAPH_GQL).await;
        create_test_subgraph(&DeploymentHash::new(OTHER).unwrap(), SUBGRAPH_GQL).await;
        deployment
    }

    run_test_sequentially(|store| async move {
        use graph::data::subgraph::status;

        let deployment = setup().await;
        let infos = store
            .status(status::Filter::Deployments(vec![
                deployment.hash.to_string(),
                "notASubgraph".to_string(),
                "not-even-a-valid-id".to_string(),
            ]))
            .await
            .unwrap();
        assert_eq!(1, infos.len());
        let info = infos.first().unwrap();
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store
            .status(status::Filter::Deployments(vec![]))
            .await
            .unwrap();
        assert_eq!(2, infos.len());
        let info = infos
            .into_iter()
            .find(|info| info.subgraph == NAME)
            .expect("the NAME subgraph is in infos");
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store
            .status(status::Filter::SubgraphName(NAME.to_string()))
            .await
            .unwrap();
        assert_eq!(1, infos.len());
        let info = infos.first().unwrap();
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store
            .status(status::Filter::SubgraphVersion(NAME.to_string(), true))
            .await
            .unwrap();
        assert_eq!(1, infos.len());
        let info = infos.first().unwrap();
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store
            .status(status::Filter::SubgraphVersion(NAME.to_string(), false))
            .await
            .unwrap();
        assert!(infos.is_empty());

        let infos = store
            .status(status::Filter::SubgraphName("invalid name".to_string()))
            .await
            .unwrap();
        assert_eq!(0, infos.len());

        let infos = store
            .status(status::Filter::SubgraphName("notASubgraph".to_string()))
            .await
            .unwrap();
        assert_eq!(0, infos.len());

        let infos = store
            .status(status::Filter::SubgraphVersion(
                "notASubgraph".to_string(),
                true,
            ))
            .await
            .unwrap();
        assert_eq!(0, infos.len());

        const MSG: &str = "your father smells of elderberries";
        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: MSG.to_string(),
            block_ptr: Some(GENESIS_PTR.clone()),
            handler: None,
            deterministic: true,
        };

        store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("can get writable")
            .fail_subgraph(error)
            .await
            .unwrap();
        let infos = store
            .status(status::Filter::Deployments(vec![deployment
                .hash
                .to_string()]))
            .await
            .unwrap();
        assert_eq!(1, infos.len());
        let info = infos.first().unwrap();
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);
        assert_eq!(SubgraphHealth::Failed, info.health);
        let error = info.fatal_error.as_ref().unwrap();
        assert_eq!(MSG, error.message.as_str());
        assert!(error.deterministic);
    })
}

#[test]
fn version_info() {
    const NAME: &str = "versionInfoSubgraph";

    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs().await;
        block_store::set_chain(vec![], NETWORK_NAME).await;
        create_test_subgraph(&id, SUBGRAPH_GQL).await
    }

    run_test_sequentially(|store| async move {
        let deployment = setup().await;
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCK_ONE.clone(),
            vec![],
        )
        .await
        .unwrap();

        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.synced);
        assert!(!vi.failed);
        assert_eq!(
            Some("manifest for versionInfoSubgraph"),
            vi.description.as_deref()
        );
        assert_eq!(
            Some("repo for versionInfoSubgraph"),
            vi.repository.as_deref()
        );
        assert_eq!(NAME, vi.schema.id().as_str());
        assert_eq!(Some(1), vi.latest_ethereum_block_number);
        assert_eq!(NETWORK_NAME, vi.network.as_str());
        // We set the head for the network to null in the test framework
        assert_eq!(None, vi.total_ethereum_blocks_count);
    })
}

#[test]
fn subgraph_features() {
    run_test_sequentially(|_store| async move {
        const NAME: &str = "subgraph_features";
        let id = DeploymentHash::new(NAME).unwrap();

        remove_subgraphs().await;
        block_store::set_chain(vec![], NETWORK_NAME).await;
        create_test_subgraph_with_features(&id, SUBGRAPH_FEATURES_GQL).await;

        let DeploymentFeatures {
            id: subgraph_id,
            spec_version,
            api_version,
            features,
            data_source_kinds,
            network,
            handler_kinds,
            has_declared_calls,
            has_bytes_as_ids,
            immutable_entities,
            has_aggregations,
        } = get_subgraph_features(id.to_string()).await.unwrap();

        assert_eq!(NAME, subgraph_id.as_str());
        assert_eq!("1.3.0", spec_version);
        assert_eq!("1.0.0", api_version.unwrap());
        assert_eq!(NETWORK_NAME, network);
        assert_eq!(
            vec![
                SubgraphFeature::NonFatalErrors.to_string(),
                SubgraphFeature::FullTextSearch.to_string(),
            ],
            features
        );
        assert_eq!(1, data_source_kinds.len());
        assert_eq!(handler_kinds.len(), 2);
        assert!(handler_kinds.contains(&"mock_handler_1".to_string()));
        assert!(handler_kinds.contains(&"mock_handler_2".to_string()));
        assert!(has_declared_calls);
        assert!(has_bytes_as_ids);
        assert!(has_aggregations);
        assert_eq!(
            immutable_entities,
            vec!["User2".to_string(), "Data".to_string()]
        );

        test_store::remove_subgraph(&id).await;
        let features = get_subgraph_features(id.to_string()).await;
        // Subgraph was removed, so we expect the entry to be removed from `subgraph_features` table
        assert!(features.is_none());
    })
}

#[test]
fn subgraph_error() {
    test_store::run_test_sequentially(|store| async move {
        let subgraph_id = DeploymentHash::new("testSubgraph").unwrap();
        let deployment =
            test_store::create_test_subgraph(&subgraph_id, "type Foo @entity { id: ID! }").await;

        let count = async || -> usize {
            let store = store.subgraph_store();
            store.error_count(&subgraph_id).await.unwrap()
        };

        let error = SubgraphError {
            subgraph_id: subgraph_id.clone(),
            message: "test".to_string(),
            block_ptr: None,
            handler: None,
            deterministic: false,
        };

        assert!(count().await == 0);

        transact_errors(&store, &deployment, BLOCKS[1].clone(), vec![error], false)
            .await
            .unwrap();
        assert!(count().await == 1);

        let error = SubgraphError {
            subgraph_id: subgraph_id.clone(),
            message: "test".to_string(),
            block_ptr: None,
            handler: None,
            deterministic: false,
        };

        // Inserting the same error is allowed but ignored.
        transact_errors(&store, &deployment, BLOCKS[2].clone(), vec![error], false)
            .await
            .unwrap();
        assert!(count().await == 1);

        let error2 = SubgraphError {
            subgraph_id: subgraph_id.clone(),
            message: "test2".to_string(),
            block_ptr: None,
            handler: None,
            deterministic: false,
        };

        transact_errors(&store, &deployment, BLOCKS[3].clone(), vec![error2], false)
            .await
            .unwrap();
        assert!(count().await == 2);

        test_store::remove_subgraph(&subgraph_id).await;
    })
}

#[test]
fn subgraph_non_fatal_error() {
    test_store::run_test_sequentially(|store| async move {
        let subgraph_store = store.subgraph_store();
        let subgraph_id = DeploymentHash::new("subgraph_non_fatal_error").unwrap();
        let deployment =
            test_store::create_test_subgraph(&subgraph_id, "type Foo @entity { id: ID! }").await;

        let count = async || -> usize {
            let store = store.subgraph_store();
            let count = store.error_count(&subgraph_id).await.unwrap();
            println!("count: {}", count);
            count
        };

        let error = SubgraphError {
            subgraph_id: subgraph_id.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[1].clone()),
            handler: None,
            deterministic: true,
        };

        assert!(count().await == 0);

        transact_errors(&store, &deployment, BLOCKS[1].clone(), vec![error], true)
            .await
            .unwrap();
        assert!(count().await == 1);

        let info = subgraph_store.status_for_id(deployment.id).await;

        assert!(info.non_fatal_errors.len() == 1);
        assert!(info.health == SubgraphHealth::Unhealthy);

        let error2 = SubgraphError {
            subgraph_id: subgraph_id.clone(),
            message: "test2".to_string(),
            block_ptr: None,
            handler: None,
            deterministic: false,
        };

        // Inserting non deterministic errors will increase error count but not count of non fatal errors
        transact_errors(&store, &deployment, BLOCKS[2].clone(), vec![error2], false)
            .await
            .unwrap();
        assert!(count().await == 2);

        let info = subgraph_store.status_for_id(deployment.id).await;

        assert!(info.non_fatal_errors.len() == 1);
        assert!(info.health == SubgraphHealth::Unhealthy);

        test_store::remove_subgraph(&subgraph_id).await;
    })
}

#[test]
fn fatal_vs_non_fatal() {
    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new("failUnfail").unwrap();
        remove_subgraphs().await;
        create_test_subgraph(&id, SUBGRAPH_GQL).await
    }

    run_test_sequentially(|store| async move {
        let deployment = setup().await;
        let query_store = store
            .query_store(QueryTarget::Deployment(
                deployment.hash.clone(),
                Default::default(),
            ))
            .await
            .unwrap();

        // Just to make latest_ethereum_block_number be 0
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[0].clone(),
            vec![],
        )
        .await
        .unwrap();

        let error = || SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[1].clone()),
            handler: None,
            deterministic: true,
        };

        store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("can get writable")
            .fail_subgraph(error())
            .await
            .unwrap();

        let state = query_store.deployment_state().await.unwrap();

        assert!(!state.has_deterministic_errors(&latest_block(&store, deployment.id).await));

        transact_errors(&store, &deployment, BLOCKS[1].clone(), vec![error()], false)
            .await
            .unwrap();

        let state = query_store.deployment_state().await.unwrap();
        assert!(state.has_deterministic_errors(&latest_block(&store, deployment.id).await));
    })
}

#[test]
fn fail_unfail_deterministic_error() {
    const NAME: &str = "failUnfailDeterministic";

    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs().await;
        create_test_subgraph(&id, SUBGRAPH_GQL).await
    }

    run_test_sequentially(|store| async move {
        let deployment = setup().await;

        let query_store = store
            .query_store(QueryTarget::Deployment(
                deployment.hash.clone(),
                Default::default(),
            ))
            .await
            .unwrap();

        // Process the first block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[0].clone(),
            vec![],
        )
        .await
        .unwrap();

        // We don't have any errors and the subgraph is healthy.
        let state = query_store.deployment_state().await.unwrap();
        assert!(!state.has_deterministic_errors(&latest_block(&store, deployment.id).await));
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(0), vi.latest_ethereum_block_number);

        // Process the second block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[1].clone(),
            vec![],
        )
        .await
        .unwrap();

        // Still no fatal errors.
        let state = query_store.deployment_state().await.unwrap();
        assert!(!state.has_deterministic_errors(&latest_block(&store, deployment.id).await));
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[1].clone()),
            handler: None,
            deterministic: true,
        };

        let writable = store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("can get writable");

        // Fail the subgraph with a deterministic error.
        writable.fail_subgraph(error).await.unwrap();

        // Now we have a fatal error because the subgraph failed.
        let state = query_store.deployment_state().await.unwrap();
        assert!(state.has_deterministic_errors(&latest_block(&store, deployment.id).await));
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        // Unfail the subgraph.
        let outcome = writable
            .unfail_deterministic_error(&BLOCKS[1], &BLOCKS[0])
            .await
            .unwrap();

        // We don't have fatal errors anymore and the block got reverted.
        assert_eq!(outcome, UnfailOutcome::Unfailed);
        let state = query_store.deployment_state().await.unwrap();
        assert!(!state.has_deterministic_errors(&latest_block(&store, deployment.id).await));
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(0), vi.latest_ethereum_block_number);

        test_store::remove_subgraphs().await;
    })
}

#[test]
fn fail_unfail_deterministic_error_noop() {
    const NAME: &str = "failUnfailDeterministicNoop";

    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs().await;
        create_test_subgraph(&id, SUBGRAPH_GQL).await
    }

    run_test_sequentially(|store| async move {
        let deployment = setup().await;

        let count = async || -> usize {
            let store = store.subgraph_store();
            store.error_count(&deployment.hash).await.unwrap()
        };

        // Process the first block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[0].clone(),
            vec![],
        )
        .await
        .unwrap();

        // We don't have any errors and the subgraph is healthy.
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(0), vi.latest_ethereum_block_number);

        // Process the second block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[1].clone(),
            vec![],
        )
        .await
        .unwrap();

        // Still no fatal errors.
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        let writable = store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("can get writable");

        // Run unfail with no errors results in NOOP.
        let outcome = writable
            .unfail_deterministic_error(&BLOCKS[1], &BLOCKS[0])
            .await
            .unwrap();

        // Nothing to unfail, state continues the same.
        assert_eq!(outcome, UnfailOutcome::Noop);
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[1].clone()),
            handler: None,
            deterministic: false, // wrong determinism
        };

        // Fail the subraph with a NON-deterministic error.
        writable.fail_subgraph(error).await.unwrap();

        // Now we have a fatal error because the subgraph failed.
        assert_eq!(count().await, 1);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        // Running unfail_deterministic_error against a NON-deterministic error will do nothing.
        let outcome = writable
            .unfail_deterministic_error(&BLOCKS[1], &BLOCKS[0])
            .await
            .unwrap();

        // State continues the same, nothing happened.
        // Neither the block got reverted or error deleted.
        assert_eq!(outcome, UnfailOutcome::Noop);
        assert_eq!(count().await, 1);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[2].clone()), // wrong block
            handler: None,
            deterministic: true, // right determinism
        };

        // Fail the subgraph with an advanced block.
        writable.fail_subgraph(error).await.unwrap();

        // Running unfail_deterministic_error won't do anything,
        // the hashes won't match and there's nothing to revert.
        let outcome = writable
            .unfail_deterministic_error(&BLOCKS[1], &BLOCKS[0])
            .await
            .unwrap();

        // State continues the same.
        // Neither the block got reverted or error deleted.
        assert_eq!(outcome, UnfailOutcome::Noop);
        assert_eq!(count().await, 2);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        test_store::remove_subgraphs().await;
    })
}

#[test]
fn fail_unfail_non_deterministic_error() {
    const NAME: &str = "failUnfailNonDeterministic";

    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs().await;
        create_test_subgraph(&id, SUBGRAPH_GQL).await
    }

    run_test_sequentially(|store| async move {
        let deployment = setup().await;

        let count = async || -> usize {
            let store = store.subgraph_store();
            store.error_count(&deployment.hash).await.unwrap()
        };

        // Process the first block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[0].clone(),
            vec![],
        )
        .await
        .unwrap();

        // We don't have any errors.
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(0), vi.latest_ethereum_block_number);

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[1].clone()),
            handler: None,
            deterministic: false,
        };

        let writable = store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("can get writable");

        // Fail subgraph with a non-deterministic error.
        writable.fail_subgraph(error).await.unwrap();

        // Now we have a fatal error because the subgraph failed.
        assert_eq!(count().await, 1);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(0), vi.latest_ethereum_block_number);

        // Process the second block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[1].clone(),
            vec![],
        )
        .await
        .unwrap();

        // Subgraph failed but it's deployment head pointer advanced.
        assert_eq!(count().await, 1);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        // Unfail the subgraph and delete the fatal error.
        let outcome = writable
            .unfail_non_deterministic_error(&BLOCKS[1])
            .await
            .unwrap();

        // We don't have fatal errors anymore and the subgraph is healthy.
        assert_eq!(outcome, UnfailOutcome::Unfailed);
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        test_store::remove_subgraphs().await;
    })
}

#[test]
fn fail_unfail_non_deterministic_error_noop() {
    const NAME: &str = "failUnfailNonDeterministicNoop";

    async fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs().await;
        create_test_subgraph(&id, SUBGRAPH_GQL).await
    }

    run_test_sequentially(|store| async move {
        let deployment = setup().await;

        let count = async || -> usize {
            let store = store.subgraph_store();
            store.error_count(&deployment.hash).await.unwrap()
        };

        // Process the first block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[0].clone(),
            vec![],
        )
        .await
        .unwrap();

        // We don't have any errors and the subgraph is healthy.
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(0), vi.latest_ethereum_block_number);

        // Process the second block.
        transact_and_wait(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[1].clone(),
            vec![],
        )
        .await
        .unwrap();

        // Still no errors.
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        let writable = store
            .subgraph_store()
            .writable(LOGGER.clone(), deployment.id, Arc::new(Vec::new()))
            .await
            .expect("can get writable");

        // Running unfail without any errors will do nothing.
        let outcome = writable
            .unfail_non_deterministic_error(&BLOCKS[1])
            .await
            .unwrap();

        // State continues the same, nothing happened.
        assert_eq!(outcome, UnfailOutcome::Noop);
        assert_eq!(count().await, 0);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(!vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[1].clone()),
            handler: None,
            deterministic: true, // wrong determinism
        };

        // Fail the subgraph with a DETERMININISTIC error.
        writable.fail_subgraph(error).await.unwrap();

        // We now have a fatal error because the subgraph failed.
        assert_eq!(count().await, 1);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        // Running unfail_non_deterministic_error will be NOOP, the error is deterministic.
        let outcome = writable
            .unfail_non_deterministic_error(&BLOCKS[1])
            .await
            .unwrap();

        // Nothing happeened, state continues the same.
        assert_eq!(outcome, UnfailOutcome::Noop);
        assert_eq!(count().await, 1);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[2].clone()), // wrong block
            handler: None,
            deterministic: false, // right determinism
        };

        // Fail the subgraph with a non-deterministic error, but with an advanced block.
        writable.fail_subgraph(error).await.unwrap();

        // Since the block range of the block won't match the deployment head, this will be NOOP.
        let outcome = writable
            .unfail_non_deterministic_error(&BLOCKS[1])
            .await
            .unwrap();

        // State continues the same besides a new error added to the database.
        assert_eq!(outcome, UnfailOutcome::Noop);
        assert_eq!(count().await, 2);
        let vi = get_version_info(&store, NAME).await;
        assert_eq!(NAME, vi.deployment_id.as_str());
        assert!(vi.failed);
        assert_eq!(Some(1), vi.latest_ethereum_block_number);

        test_store::remove_subgraphs().await;
    })
}
