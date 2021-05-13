use graph::{
    components::store::{DeploymentLocator, StatusStore},
    data::subgraph::schema::SubgraphError,
    data::subgraph::schema::SubgraphHealth,
    prelude::EntityChange,
    prelude::EntityChangeOperation,
    prelude::QueryStoreManager,
    prelude::Schema,
    prelude::StoreEvent,
    prelude::SubgraphDeploymentEntity,
    prelude::SubgraphManifest,
    prelude::SubgraphName,
    prelude::SubgraphVersionSwitchingMode,
    prelude::{CheapClone, DeploymentHash, NodeId, SubgraphStore as _},
};
use graph_store_postgres::layout_for_tests::Connection as Primary;
use graph_store_postgres::SubgraphStore;

use std::{collections::HashSet, sync::Arc};
use test_store::*;

const SUBGRAPH_GQL: &str = "
    type User @entity {
        id: ID!,
        name: String
    }
";

fn assigned(deployment: &DeploymentLocator) -> EntityChange {
    EntityChange::Assignment {
        deployment: deployment.clone(),
        operation: EntityChangeOperation::Set,
    }
}

fn unassigned(deployment: &DeploymentLocator) -> EntityChange {
    EntityChange::Assignment {
        deployment: deployment.clone(),
        operation: EntityChangeOperation::Removed,
    }
}

#[test]
fn reassign_subgraph() {
    fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new("reassignSubgraph").unwrap();
        remove_subgraphs();
        create_test_subgraph(&id, SUBGRAPH_GQL)
    }

    fn find_assignment(store: &SubgraphStore, deployment: &DeploymentLocator) -> Option<String> {
        store
            .assigned_node(deployment)
            .unwrap()
            .map(|node| node.to_string())
    }

    run_test_sequentially(setup, |store, id| async move {
        let store = store.subgraph_store();

        // Check our setup
        let node = find_assignment(store.as_ref(), &id);
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

            let (_, events) = tap_store_events(|| store.reassign_subgraph(&id, &node).unwrap());
            let node = find_assignment(store.as_ref(), &id);
            assert_eq!(Some("left"), node.as_deref());
            assert_eq!(expected, events);
        }
    })
}

#[test]
fn create_subgraph() {
    const SUBGRAPH_NAME: &str = "create/subgraph";

    // Return the versions (not deployments) for a subgraph
    fn subgraph_versions(primary: &Primary) -> (Option<String>, Option<String>) {
        primary.versions_for_subgraph(&*SUBGRAPH_NAME).unwrap()
    }

    /// Return the deployment for the current and the pending version of the
    /// subgraph with the given `entity_id`
    fn subgraph_deployments(primary: &Primary) -> (Option<String>, Option<String>) {
        let (current, pending) = subgraph_versions(primary);
        (
            current.and_then(|v| primary.deployment_for_version(&v).unwrap()),
            pending.and_then(|v| primary.deployment_for_version(&v).unwrap()),
        )
    }

    fn deploy(
        store: &SubgraphStore,
        id: &str,
        mode: SubgraphVersionSwitchingMode,
    ) -> (DeploymentLocator, HashSet<EntityChange>) {
        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let id = DeploymentHash::new(id.to_string()).unwrap();
        let schema = Schema::parse(SUBGRAPH_GQL, id.clone()).unwrap();

        let manifest = SubgraphManifest::<graph_chain_ethereum::DataSource> {
            id: id.clone(),
            spec_version: "1".to_owned(),
            features: Default::default(),
            description: None,
            repository: None,
            schema: schema.clone(),
            data_sources: vec![],
            graft: None,
            templates: vec![],
        };
        let deployment = SubgraphDeploymentEntity::new(&manifest, false, None);
        let node_id = NodeId::new("left").unwrap();

        let (deployment, events) = tap_store_events(|| {
            store
                .create_subgraph_deployment(
                    name,
                    &schema,
                    deployment,
                    node_id,
                    NETWORK_NAME.to_string(),
                    mode,
                )
                .unwrap()
        });
        let events = events
            .into_iter()
            .map(|event| event.changes.into_iter())
            .flatten()
            .collect();
        (deployment, events)
    }

    fn deploy_event(deployment: &DeploymentLocator) -> HashSet<EntityChange> {
        let mut changes = HashSet::new();
        changes.insert(assigned(deployment));
        changes
    }

    fn deployment_synced(store: &Arc<SubgraphStore>, deployment: &DeploymentLocator) {
        store
            .writable(deployment)
            .expect("can get writable")
            .deployment_synced()
            .unwrap();
    }

    // Test VersionSwitchingMode::Instant
    run_test_sequentially(remove_subgraphs, |store, _| async move {
        let store = store.subgraph_store();

        const MODE: SubgraphVersionSwitchingMode = SubgraphVersionSwitchingMode::Instant;
        const ID1: &str = "instant";
        const ID2: &str = "instant2";
        const ID3: &str = "instant3";

        let primary = primary_connection();

        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let (_, events) = tap_store_events(|| store.create_subgraph(name.clone()).unwrap());
        let (current, pending) = subgraph_deployments(&primary);
        assert!(events.is_empty());
        assert!(current.is_none());
        assert!(pending.is_none());

        // Deploy
        let (deployment1, events) = deploy(store.as_ref(), ID1, MODE);
        let expected = deploy_event(&deployment1);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID1), current.as_deref());
        assert!(pending.is_none());

        // Deploying again overwrites current
        let (deployment2, events) = deploy(store.as_ref(), ID2, MODE);
        let mut expected = deploy_event(&deployment2);
        expected.insert(unassigned(&deployment1));
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID2), current.as_deref());
        assert!(pending.is_none());

        // Sync deployment
        deployment_synced(&store, &deployment2);

        // Deploying again still overwrites current
        let (deployment3, events) = deploy(store.as_ref(), ID3, MODE);
        let mut expected = deploy_event(&deployment3);
        expected.insert(unassigned(&deployment2));
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID3), current.as_deref());
        assert!(pending.is_none());
    });

    // Test VersionSwitchingMode::Synced
    run_test_sequentially(remove_subgraphs, |store, _| async move {
        let store = store.subgraph_store();

        const MODE: SubgraphVersionSwitchingMode = SubgraphVersionSwitchingMode::Synced;
        const ID1: &str = "synced";
        const ID2: &str = "synced2";
        const ID3: &str = "synced3";

        let primary = primary_connection();

        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let (_, events) = tap_store_events(|| store.create_subgraph(name.clone()).unwrap());
        let (current, pending) = subgraph_deployments(&primary);
        assert!(events.is_empty());
        assert!(current.is_none());
        assert!(pending.is_none());

        // Deploy
        let (deployment1, events) = deploy(store.as_ref(), ID1, MODE);
        let expected = deploy_event(&deployment1);
        assert_eq!(expected, events);

        let versions = subgraph_versions(&primary);
        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID1), current.as_deref());
        assert!(pending.is_none());

        // Deploying the same thing again does nothing
        let (deployment1_again, events) = deploy(store.as_ref(), ID1, MODE);
        assert!(events.is_empty());
        assert_eq!(&deployment1, &deployment1_again);
        let versions2 = subgraph_versions(&primary);
        assert_eq!(versions, versions2);

        // Deploy again, current is not synced, so it gets replaced
        let (deployment2, events) = deploy(store.as_ref(), ID2, MODE);
        let mut expected = deploy_event(&deployment2);
        expected.insert(unassigned(&deployment1));
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID2), current.as_deref());
        assert!(pending.is_none());

        // Deploy when current is synced leaves current alone and adds pending
        deployment_synced(&store, &deployment2);
        let (deployment3, events) = deploy(store.as_ref(), ID3, MODE);
        let expected = deploy_event(&deployment3);
        assert_eq!(expected, events);

        let versions = subgraph_versions(&primary);
        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID2), current.as_deref());
        assert_eq!(Some(ID3), pending.as_deref());

        // Deploying that same thing again changes nothing
        let (deployment3_again, events) = deploy(store.as_ref(), ID3, MODE);
        assert!(events.is_empty());
        assert_eq!(&deployment3, &deployment3_again);
        let versions2 = subgraph_versions(&primary);
        assert_eq!(versions, versions2);

        // Deploy the current version once more; we wind up with current and pending
        // pointing to ID2. That's not ideal, but will be rectified when the
        // next block gets processed and the pending version is promoted to
        // current
        let mut expected = HashSet::new();
        expected.insert(unassigned(&deployment3));

        let (deployment2_again, events) = deploy(store.as_ref(), ID2, MODE);
        assert_eq!(&deployment2, &deployment2_again);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID2), current.as_deref());
        assert_eq!(Some(ID2), pending.as_deref());
    })
}

#[test]
fn status() {
    const NAME: &str = "infoSubgraph";
    const OTHER: &str = "otherInfoSubgraph";

    fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs();
        let deployment = create_test_subgraph(&id, SUBGRAPH_GQL);
        create_test_subgraph(&DeploymentHash::new(OTHER).unwrap(), SUBGRAPH_GQL);
        deployment
    }

    run_test_sequentially(setup, |store, deployment| async move {
        use graph::data::subgraph::status;

        let infos = store
            .status(status::Filter::Deployments(vec![
                deployment.hash.to_string(),
                "notASubgraph".to_string(),
                "not-even-a-valid-id".to_string(),
            ]))
            .unwrap();
        assert_eq!(1, infos.len());
        let info = infos.first().unwrap();
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store.status(status::Filter::Deployments(vec![])).unwrap();
        assert_eq!(2, infos.len());
        let info = infos
            .into_iter()
            .find(|info| info.subgraph == NAME)
            .expect("the NAME subgraph is in infos");
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store
            .status(status::Filter::SubgraphName(NAME.to_string()))
            .unwrap();
        assert_eq!(1, infos.len());
        let info = infos.first().unwrap();
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store
            .status(status::Filter::SubgraphVersion(NAME.to_string(), true))
            .unwrap();
        assert_eq!(1, infos.len());
        let info = infos.first().unwrap();
        assert_eq!(NAME, info.subgraph);
        assert!(!info.synced);

        let infos = store
            .status(status::Filter::SubgraphVersion(NAME.to_string(), false))
            .unwrap();
        assert!(infos.is_empty());

        let infos = store
            .status(status::Filter::SubgraphName("invalid name".to_string()))
            .unwrap();
        assert_eq!(0, infos.len());

        let infos = store
            .status(status::Filter::SubgraphName("notASubgraph".to_string()))
            .unwrap();
        assert_eq!(0, infos.len());

        let infos = store
            .status(status::Filter::SubgraphVersion(
                "notASubgraph".to_string(),
                true,
            ))
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
            .writable(&deployment)
            .expect("can get writable")
            .fail_subgraph(error)
            .await
            .unwrap();
        let infos = store
            .status(status::Filter::Deployments(vec![deployment
                .hash
                .to_string()]))
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

    fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new(NAME).unwrap();
        remove_subgraphs();
        block_store::set_chain(vec![], NETWORK_NAME);
        create_test_subgraph(&id, SUBGRAPH_GQL)
    }

    run_test_sequentially(setup, |store, deployment| async move {
        transact_entity_operations(
            &store.subgraph_store(),
            &deployment,
            BLOCK_ONE.clone(),
            vec![],
        )
        .unwrap();

        let primary = primary_connection();
        let (current, _) = primary.versions_for_subgraph(&*NAME).unwrap();
        let current = current.unwrap();

        let vi = store.version_info(&current).unwrap();
        assert_eq!(&*NAME, vi.deployment_id.as_str());
        assert_eq!(false, vi.synced);
        assert_eq!(false, vi.failed);
        assert_eq!(
            Some("manifest for versionInfoSubgraph"),
            vi.description.as_deref()
        );
        assert_eq!(
            Some("repo for versionInfoSubgraph"),
            vi.repository.as_deref()
        );
        assert_eq!(&*NAME, vi.schema.id.as_str());
        assert_eq!(Some(1), vi.latest_ethereum_block_number);
        assert_eq!(&*NETWORK_NAME, vi.network.as_str());
        // We set the head for the network to null in the test framework
        assert_eq!(None, vi.total_ethereum_blocks_count);
    })
}

#[test]
fn subgraph_error() {
    test_store::run_test_sequentially(
        || (),
        |store, _| async move {
            let subgraph_id = DeploymentHash::new("testSubgraph").unwrap();
            let deployment = test_store::create_test_subgraph(&subgraph_id, "type Foo { id: ID! }");

            let count = || -> usize {
                let store = store.subgraph_store();
                store.error_count(&subgraph_id).unwrap()
            };

            let error = SubgraphError {
                subgraph_id: subgraph_id.clone(),
                message: "test".to_string(),
                block_ptr: None,
                handler: None,
                deterministic: false,
            };

            assert!(count() == 0);

            transact_errors(&store, &deployment, BLOCKS[1].clone(), vec![error]).unwrap();
            assert!(count() == 1);

            let error = SubgraphError {
                subgraph_id: subgraph_id.clone(),
                message: "test".to_string(),
                block_ptr: None,
                handler: None,
                deterministic: false,
            };

            // Inserting the same error is allowed but ignored.
            transact_errors(&store, &deployment, BLOCKS[2].clone(), vec![error]).unwrap();
            assert!(count() == 1);

            let error2 = SubgraphError {
                subgraph_id: subgraph_id.clone(),
                message: "test2".to_string(),
                block_ptr: None,
                handler: None,
                deterministic: false,
            };

            transact_errors(&store, &deployment, BLOCKS[3].clone(), vec![error2]).unwrap();
            assert!(count() == 2);

            test_store::remove_subgraph(&subgraph_id);
        },
    )
}

#[test]
fn fatal_vs_non_fatal() {
    fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new("failUnfail").unwrap();
        remove_subgraphs();
        create_test_subgraph(&id, SUBGRAPH_GQL)
    }

    run_test_sequentially(setup, |store, deployment| async move {
        let query_store = store
            .query_store(deployment.hash.clone().into(), false)
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
            .writable(&deployment)
            .expect("can get writable")
            .fail_subgraph(error())
            .await
            .unwrap();

        assert!(!query_store.has_non_fatal_errors(None).await.unwrap());

        transact_errors(&store, &deployment, BLOCKS[1].clone(), vec![error()]).unwrap();

        assert!(query_store.has_non_fatal_errors(None).await.unwrap());
    })
}

#[test]
fn fail_unfail() {
    fn setup() -> DeploymentLocator {
        let id = DeploymentHash::new("failUnfail").unwrap();
        remove_subgraphs();
        create_test_subgraph(&id, SUBGRAPH_GQL)
    }

    run_test_sequentially(setup, |store, deployment| async move {
        let query_store = store
            .query_store(deployment.hash.cheap_clone().into(), false)
            .await
            .unwrap();

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "test".to_string(),
            block_ptr: Some(BLOCKS[1].clone()),
            handler: None,
            deterministic: true,
        };

        let writable = store
            .subgraph_store()
            .writable(&deployment)
            .expect("can get writable");
        writable.fail_subgraph(error).await.unwrap();

        assert!(!query_store.has_non_fatal_errors(None).await.unwrap());

        // This will unfail the subgraph and delete the fatal error.
        writable.unfail().unwrap();

        // Advance the block ptr to the block of the deleted error.
        transact_entity_operations(
            &store.subgraph_store(),
            &deployment,
            BLOCKS[1].clone(),
            vec![],
        )
        .unwrap();

        // We still have no fatal errors.
        assert!(!query_store.has_non_fatal_errors(None).await.unwrap());

        test_store::remove_subgraphs();
    })
}
