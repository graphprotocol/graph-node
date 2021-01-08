use graph::{
    data::subgraph::schema::MetadataType,
    data::subgraph::schema::SubgraphError,
    data::subgraph::schema::SubgraphHealth,
    prelude::EntityChange,
    prelude::EntityChangeOperation,
    prelude::Schema,
    prelude::StoreEvent,
    prelude::SubgraphDeploymentEntity,
    prelude::SubgraphManifest,
    prelude::SubgraphName,
    prelude::SubgraphVersionSwitchingMode,
    prelude::{NodeId, Store as _, SubgraphDeploymentId},
};
use graph_store_postgres::layout_for_tests::Connection as Primary;
use graph_store_postgres::NetworkStore;

use std::collections::HashSet;
use test_store::*;

const SUBGRAPH_GQL: &str = "
    type User @entity {
        id: ID!,
        name: String
    }
";

fn set(typ: MetadataType, subgraph_id: &str, id: &str) -> EntityChange {
    EntityChange {
        subgraph_id: SubgraphDeploymentId::new(subgraph_id).unwrap(),
        entity_type: typ.into(),
        entity_id: id.to_string(),
        operation: EntityChangeOperation::Set,
    }
}

fn removed(typ: MetadataType, subgraph_id: &str, id: &str) -> EntityChange {
    EntityChange {
        subgraph_id: SubgraphDeploymentId::new(subgraph_id).unwrap(),
        entity_type: typ.into(),
        entity_id: id.to_string(),
        operation: EntityChangeOperation::Removed,
    }
}

#[test]
fn reassign_subgraph() {
    fn setup() -> SubgraphDeploymentId {
        let id = SubgraphDeploymentId::new("reassignSubgraph").unwrap();
        remove_subgraphs();
        create_test_subgraph(&id, SUBGRAPH_GQL);
        id
    }

    fn find_assignment(store: &NetworkStore, id: &SubgraphDeploymentId) -> Option<String> {
        store
            .assigned_node(id)
            .unwrap()
            .map(|node| node.to_string())
    }

    run_test_sequentially(setup, |store, id| async move {
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
            let expected = vec![StoreEvent::new(vec![set(
                MetadataType::SubgraphDeploymentAssignment,
                &id,
                id.as_str(),
            )])];

            let events = tap_store_events(|| store.reassign_subgraph(&id, &node).unwrap());
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
        store: &NetworkStore,
        id: &str,
        mode: SubgraphVersionSwitchingMode,
    ) -> HashSet<EntityChange> {
        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let id = SubgraphDeploymentId::new(id.to_string()).unwrap();
        let schema = Schema::parse(SUBGRAPH_GQL, id.clone()).unwrap();

        let manifest = SubgraphManifest {
            id: id.clone(),
            location: String::new(),
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

        tap_store_events(|| {
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
        })
        .into_iter()
        .map(|event| event.changes.into_iter())
        .flatten()
        .collect()
    }

    fn deploy_event(id: &str) -> HashSet<EntityChange> {
        let mut changes = HashSet::new();
        changes.insert(set(MetadataType::SubgraphDeployment, id, id));
        changes.insert(set(MetadataType::SubgraphDeploymentAssignment, id, id));
        changes.insert(set(
            MetadataType::SubgraphManifest,
            id,
            &format!("{}-manifest", id),
        ));
        changes
    }

    // Test VersionSwitchingMode::Instant
    run_test_sequentially(remove_subgraphs, |store, _| async move {
        const MODE: SubgraphVersionSwitchingMode = SubgraphVersionSwitchingMode::Instant;
        const ID: &str = "instant";
        const ID2: &str = "instant2";
        const ID3: &str = "instant3";

        let primary = primary_connection();

        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let mut subgraph = String::from("none");
        let events = tap_store_events(|| {
            subgraph = store.create_subgraph(name.clone()).unwrap();
        });
        let (current, pending) = subgraph_deployments(&primary);
        assert!(events.is_empty());
        assert!(current.is_none());
        assert!(pending.is_none());

        // Deploy
        let expected = deploy_event(ID);

        let events = deploy(store.as_ref(), ID, MODE);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID), current.as_deref());
        assert!(pending.is_none());

        // Deploying again overwrites current
        let mut expected = deploy_event(ID2);
        expected.insert(removed(MetadataType::SubgraphDeploymentAssignment, ID, ID));

        let events = deploy(store.as_ref(), ID2, MODE);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID2), current.as_deref());
        assert!(pending.is_none());

        // Sync deployment
        store
            .deployment_synced(&SubgraphDeploymentId::new(ID2).unwrap())
            .unwrap();

        // Deploying again still overwrites current
        let mut expected = deploy_event(ID3);
        expected.insert(removed(
            MetadataType::SubgraphDeploymentAssignment,
            ID2,
            ID2,
        ));

        let events = deploy(store.as_ref(), ID3, MODE);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID3), current.as_deref());
        assert!(pending.is_none());
    });

    // Test VersionSwitchingMode::Synced
    run_test_sequentially(remove_subgraphs, |store, _| async move {
        const MODE: SubgraphVersionSwitchingMode = SubgraphVersionSwitchingMode::Synced;
        const ID: &str = "synced";
        const ID2: &str = "synced2";
        const ID3: &str = "synced3";

        let primary = primary_connection();

        let name = SubgraphName::new(SUBGRAPH_NAME.to_string()).unwrap();
        let mut subgraph = String::from("none");
        let events = tap_store_events(|| {
            subgraph = store.create_subgraph(name.clone()).unwrap();
        });
        let (current, pending) = subgraph_deployments(&primary);
        assert!(events.is_empty());
        assert!(current.is_none());
        assert!(pending.is_none());

        // Deploy
        let expected = deploy_event(ID);

        let events = deploy(store.as_ref(), ID, MODE);
        assert_eq!(expected, events);

        let versions = subgraph_versions(&primary);
        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID), current.as_deref());
        assert!(pending.is_none());

        // Deploying the same thing again does nothing
        let events = deploy(store.as_ref(), ID, MODE);
        assert!(events.is_empty());
        let versions2 = subgraph_versions(&primary);
        assert_eq!(versions, versions2);

        // Deploy again, current is not synced, so it gets replaced
        let mut expected = deploy_event(ID2);
        expected.insert(removed(MetadataType::SubgraphDeploymentAssignment, ID, ID));

        let events = deploy(store.as_ref(), ID2, MODE);
        assert_eq!(expected, events);

        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID2), current.as_deref());
        assert!(pending.is_none());

        // Deploy when current is synced leaves current alone and adds pending
        store
            .deployment_synced(&SubgraphDeploymentId::new(ID2).unwrap())
            .unwrap();
        let expected = deploy_event(ID3);

        let events = deploy(store.as_ref(), ID3, MODE);
        assert_eq!(expected, events);

        let versions = subgraph_versions(&primary);
        let (current, pending) = subgraph_deployments(&primary);
        assert_eq!(Some(ID2), current.as_deref());
        assert_eq!(Some(ID3), pending.as_deref());

        // Deploying that same thing again changes nothing
        let events = deploy(store.as_ref(), ID3, MODE);
        assert!(events.is_empty());
        let versions2 = subgraph_versions(&primary);
        assert_eq!(versions, versions2);

        // Deploy the current version once more; we wind up with current and pending
        // pointing to ID2. That's not ideal, but will be rectified when the
        // next block gets processed and the pending version is promoted to
        // current
        let mut expected = HashSet::new();
        expected.insert(removed(
            MetadataType::SubgraphDeploymentAssignment,
            ID3,
            ID3,
        ));

        let events = deploy(store.as_ref(), ID2, MODE);
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

    fn setup() -> SubgraphDeploymentId {
        let id = SubgraphDeploymentId::new(NAME).unwrap();
        remove_subgraphs();
        create_test_subgraph(&id, SUBGRAPH_GQL);
        create_test_subgraph(&SubgraphDeploymentId::new(OTHER).unwrap(), SUBGRAPH_GQL);
        id
    }

    run_test_sequentially(setup, |store, id| async move {
        use graph::data::subgraph::status;

        let infos = store
            .status(status::Filter::Deployments(vec![
                id.to_string(),
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
            subgraph_id: id.clone(),
            message: MSG.to_string(),
            block_ptr: Some(GENESIS_PTR.clone()),
            handler: None,
            deterministic: true,
        };

        store.fail_subgraph(id.clone(), error).await.unwrap();
        let infos = store
            .status(status::Filter::Deployments(vec![id.to_string()]))
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

    fn setup() -> SubgraphDeploymentId {
        let id = SubgraphDeploymentId::new(NAME).unwrap();
        remove_subgraphs();
        create_test_subgraph(&id, SUBGRAPH_GQL);
        id
    }

    run_test_sequentially(setup, |store, id| async move {
        transact_entity_operations(&store, id.clone(), BLOCK_ONE.clone(), vec![]).unwrap();

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
        // We don't have a data source on the manifest of the test subgraph
        // and can therefore not find the network or the head block
        assert_eq!(None, vi.total_ethereum_blocks_count);
        assert_eq!(None, vi.network);
    })
}

#[test]
fn subgraph_error() {
    test_store::run_test_sequentially(
        || (),
        |store, _| async move {
            let subgraph_id = SubgraphDeploymentId::new("testSubgraph").unwrap();
            test_store::create_test_subgraph(&subgraph_id, "type Foo { id: ID! }");

            let count = || -> usize {
                let store = store.store();
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

            transact_errors(&store, subgraph_id.clone(), BLOCKS[1].clone(), vec![error]).unwrap();
            assert!(count() == 1);

            let error = SubgraphError {
                subgraph_id: subgraph_id.clone(),
                message: "test".to_string(),
                block_ptr: None,
                handler: None,
                deterministic: false,
            };

            // Inserting the same error is allowed but ignored.
            transact_errors(&store, subgraph_id.clone(), BLOCKS[2].clone(), vec![error]).unwrap();
            assert!(count() == 1);

            let error2 = SubgraphError {
                subgraph_id: subgraph_id.clone(),
                message: "test2".to_string(),
                block_ptr: None,
                handler: None,
                deterministic: false,
            };

            transact_errors(&store, subgraph_id.clone(), BLOCKS[3].clone(), vec![error2]).unwrap();
            assert!(count() == 2);

            test_store::remove_subgraph(&subgraph_id);
        },
    )
}
