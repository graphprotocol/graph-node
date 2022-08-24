use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use graph::data::subgraph::schema::SubgraphError;
use graph::data::subgraph::{SPEC_VERSION_0_0_4, SPEC_VERSION_0_0_7};
use graph::data_source::DataSourceTemplate;
use graph::prelude::{
    anyhow, async_trait, serde_yaml, tokio, DeploymentHash, Entity, Link, Logger, SubgraphManifest,
    SubgraphManifestValidationError, UnvalidatedSubgraphManifest,
};
use graph::{
    blockchain::NodeCapabilities as _,
    components::{
        link_resolver::{JsonValueStream, LinkResolver as LinkResolverTrait},
        store::EntityType,
    },
    data::subgraph::SubgraphFeature,
};

use graph_chain_ethereum::{Chain, NodeCapabilities};
use semver::Version;
use test_store::LOGGER;

const GQL_SCHEMA: &str = "type Thing @entity { id: ID! }";
const GQL_SCHEMA_FULLTEXT: &str = include_str!("full-text.graphql");
const MAPPING_WITH_IPFS_FUNC_WASM: &[u8] = include_bytes!("ipfs-on-ethereum-contracts.wasm");
const ABI: &str = "[{\"type\":\"function\", \"inputs\": [{\"name\": \"i\",\"type\": \"uint256\"}],\"name\":\"get\",\"outputs\": [{\"type\": \"address\",\"name\": \"o\"}]}]";
const FILE: &str = "{}";
const FILE_CID: &str = "bafkreigkhuldxkyfkoaye4rgcqcwr45667vkygd45plwq6hawy7j4rbdky";

#[derive(Default, Debug, Clone)]
struct TextResolver {
    texts: HashMap<String, Vec<u8>>,
}

impl TextResolver {
    fn add(&mut self, link: &str, text: &impl AsRef<[u8]>) {
        self.texts.insert(
            link.to_owned(),
            text.as_ref().into_iter().cloned().collect(),
        );
    }
}

#[async_trait]
impl LinkResolverTrait for TextResolver {
    fn with_timeout(&self, _timeout: Duration) -> Box<dyn LinkResolverTrait> {
        Box::new(self.clone())
    }

    fn with_retries(&self) -> Box<dyn LinkResolverTrait> {
        Box::new(self.clone())
    }

    async fn cat(&self, _logger: &Logger, link: &Link) -> Result<Vec<u8>, anyhow::Error> {
        self.texts
            .get(&link.link)
            .ok_or(anyhow!("No text for {}", &link.link))
            .map(Clone::clone)
    }

    async fn get_block(&self, _logger: &Logger, _link: &Link) -> Result<Vec<u8>, anyhow::Error> {
        unimplemented!()
    }

    async fn json_stream(
        &self,
        _logger: &Logger,
        _link: &Link,
    ) -> Result<JsonValueStream, anyhow::Error> {
        unimplemented!()
    }
}

async fn resolve_manifest(
    text: &str,
    max_spec_version: Version,
) -> SubgraphManifest<graph_chain_ethereum::Chain> {
    let mut resolver = TextResolver::default();
    let id = DeploymentHash::new("Qmmanifest").unwrap();

    resolver.add(id.as_str(), &text);
    resolver.add("/ipfs/Qmschema", &GQL_SCHEMA);
    resolver.add("/ipfs/Qmabi", &ABI);
    resolver.add("/ipfs/Qmmapping", &MAPPING_WITH_IPFS_FUNC_WASM);
    resolver.add(FILE_CID, &FILE);

    let resolver: Arc<dyn LinkResolverTrait> = Arc::new(resolver);

    let raw = serde_yaml::from_str(text).unwrap();
    SubgraphManifest::resolve_from_raw(id, raw, &resolver, &LOGGER, max_spec_version)
        .await
        .expect("Parsing simple manifest works")
}

async fn resolve_unvalidated(text: &str) -> UnvalidatedSubgraphManifest<Chain> {
    let mut resolver = TextResolver::default();
    let id = DeploymentHash::new("Qmmanifest").unwrap();

    resolver.add(id.as_str(), &text);
    resolver.add("/ipfs/Qmschema", &GQL_SCHEMA);

    let resolver: Arc<dyn LinkResolverTrait> = Arc::new(resolver);

    let raw = serde_yaml::from_str(text).unwrap();
    UnvalidatedSubgraphManifest::resolve(id, raw, &resolver, &LOGGER, SPEC_VERSION_0_0_4.clone())
        .await
        .expect("Parsing simple manifest works")
}

// Some of these manifest tests should be made chain-independent, but for
// now we just run them for the ethereum `Chain`

#[tokio::test]
async fn simple_manifest() {
    const YAML: &str = "
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
specVersion: 0.0.2
";

    let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;

    assert_eq!("Qmmanifest", manifest.id.as_str());
    assert!(manifest.graft.is_none());
}

#[tokio::test]
async fn ipfs_manifest() {
    let yaml = "
schema:
  file:
    /: /ipfs/Qmschema
dataSources: []
templates:
  - name: IpfsSource
    kind: file/ipfs
    mapping:
      apiVersion: 0.0.6
      language: wasm/assemblyscript
      entities:
        - TestEntity
      file:
        /: /ipfs/Qmmapping
      handler: handleFile
specVersion: 0.0.7
";

    let manifest = resolve_manifest(&yaml, SPEC_VERSION_0_0_7).await;

    assert_eq!("Qmmanifest", manifest.id.as_str());
    assert_eq!(manifest.data_sources.len(), 0);
    let data_source = match &manifest.templates[0] {
        DataSourceTemplate::Offchain(ds) => ds,
        DataSourceTemplate::Onchain(_) => unreachable!(),
    };
    assert_eq!(data_source.kind, "file/ipfs");
}

#[tokio::test]
async fn graft_manifest() {
    const YAML: &str = "
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
graft:
  base: Qmbase
  block: 12345
specVersion: 0.0.2
";

    let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;

    assert_eq!("Qmmanifest", manifest.id.as_str());
    let graft = manifest.graft.expect("The manifest has a graft base");
    assert_eq!("Qmbase", graft.base.as_str());
    assert_eq!(12345, graft.block);
}

#[test]
fn graft_failed_subgraph() {
    const YAML: &str = "
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
graft:
  base: Qmbase
  block: 0
specVersion: 0.0.2
";

    test_store::run_test_sequentially(|store| async move {
        let subgraph_store = store.subgraph_store();

        let unvalidated = resolve_unvalidated(YAML).await;
        let subgraph = DeploymentHash::new("Qmbase").unwrap();

        // Creates base subgraph at block 0 (genesis).
        let deployment = test_store::create_test_subgraph(&subgraph, GQL_SCHEMA).await;

        // Adds an example entity.
        let mut thing = Entity::new();
        thing.set("id", "datthing");
        test_store::insert_entities(&deployment, vec![(EntityType::from("Thing"), thing)])
            .await
            .unwrap();

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "deterministic error".to_string(),
            block_ptr: Some(test_store::BLOCKS[1].clone()),
            handler: None,
            deterministic: true,
        };

        // Fails the base subgraph at block 1 (and advances the pointer).
        test_store::transact_errors(
            &store,
            &deployment,
            test_store::BLOCKS[1].clone(),
            vec![error],
        )
        .await
        .unwrap();

        // Make sure there are no GraftBaseInvalid errors.
        //
        // This is allowed because:
        // - base:  failed at block 1
        // - graft: starts at block 0
        //
        // Meaning that the graft will fail just like it's parent
        // but it started at a valid previous block.
        assert!(
            unvalidated
                .validate(subgraph_store.clone(), true)
                .await
                .expect_err("Validation must fail")
                .into_iter()
                .find(|e| matches!(e, SubgraphManifestValidationError::GraftBaseInvalid(_)))
                .is_none(),
            "There shouldn't be a GraftBaseInvalid error"
        );

        // Resolve the graft normally.
        let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;

        assert_eq!("Qmmanifest", manifest.id.as_str());
        let graft = manifest.graft.expect("The manifest has a graft base");
        assert_eq!("Qmbase", graft.base.as_str());
        assert_eq!(0, graft.block);
    })
}

#[test]
fn graft_invalid_manifest() {
    const YAML: &str = "
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
graft:
  base: Qmbase
  block: 1
specVersion: 0.0.2
";

    test_store::run_test_sequentially(|store| async move {
        let subgraph_store = store.subgraph_store();

        let unvalidated = resolve_unvalidated(YAML).await;
        let subgraph = DeploymentHash::new("Qmbase").unwrap();

        //
        // Validation against subgraph that hasn't synced anything fails
        //
        let deployment = test_store::create_test_subgraph(&subgraph, GQL_SCHEMA).await;
        // This check is awkward since the test manifest has other problems
        // that the validation complains about as setting up a valid manifest
        // would be a bit more work; we just want to make sure that
        // graft-related checks work
        let msg = unvalidated
            .validate(subgraph_store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| matches!(e, SubgraphManifestValidationError::GraftBaseInvalid(_)))
            .expect("There must be a GraftBaseInvalid error")
            .to_string();
        assert_eq!(
            "the graft base is invalid: failed to graft onto `Qmbase` since \
            it has not processed any blocks",
            msg
        );

        let mut thing = Entity::new();
        thing.set("id", "datthing");
        test_store::insert_entities(&deployment, vec![(EntityType::from("Thing"), thing)])
            .await
            .unwrap();

        // Validation against subgraph that has not reached the graft point fails
        let unvalidated = resolve_unvalidated(YAML).await;
        let msg = unvalidated
            .validate(subgraph_store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| matches!(e, SubgraphManifestValidationError::GraftBaseInvalid(_)))
            .expect("There must be a GraftBaseInvalid error")
            .to_string();
        assert_eq!(
            "the graft base is invalid: failed to graft onto `Qmbase` \
            at block 1 since it has only processed block 0",
            msg
        );

        let error = SubgraphError {
            subgraph_id: deployment.hash.clone(),
            message: "deterministic error".to_string(),
            block_ptr: Some(test_store::BLOCKS[1].clone()),
            handler: None,
            deterministic: true,
        };

        test_store::transact_errors(
            &store,
            &deployment,
            test_store::BLOCKS[1].clone(),
            vec![error],
        )
        .await
        .unwrap();

        // This check is bit awkward, but we just want to be sure there is a
        // GraftBaseInvalid error.
        //
        // The validation error happens because:
        // - base:  failed at block 1
        // - graft: starts at block 1
        //
        // Since we start grafts at N + 1, we can't allow a graft to be created
        // at the failed block. They (developers) should choose a previous valid
        // block.
        let unvalidated = resolve_unvalidated(YAML).await;
        let msg = unvalidated
            .validate(subgraph_store, true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| matches!(e, SubgraphManifestValidationError::GraftBaseInvalid(_)))
            .expect("There must be a GraftBaseInvalid error")
            .to_string();
        assert_eq!(
            "the graft base is invalid: failed to graft onto `Qmbase` \
            at block 1 since it's not healthy. You can graft it starting at block 0 backwards",
            msg
        );
    })
}

#[tokio::test]
async fn parse_call_handlers() {
    const YAML: &str = "
dataSources:
  - kind: ethereum/contract
    name: Factory
    network: mainnet
    source:
      abi: Factory
      startBlock: 9562480
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.4
      language: wasm/assemblyscript
      entities:
        - TestEntity
      file:
        /: /ipfs/Qmmapping
      abis:
        - name: Factory
          file:
            /: /ipfs/Qmabi
      callHandlers:
        - function: get(address)
          handler: handleget
schema:
  file:
    /: /ipfs/Qmschema
specVersion: 0.0.2
";

    let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;
    let onchain_data_sources = manifest
        .data_sources
        .iter()
        .filter_map(|ds| ds.as_onchain().cloned())
        .collect::<Vec<_>>();
    let required_capabilities = NodeCapabilities::from_data_sources(&onchain_data_sources);

    assert_eq!("Qmmanifest", manifest.id.as_str());
    assert_eq!(true, required_capabilities.traces);
}

#[test]
fn undeclared_grafting_feature_causes_feature_validation_error() {
    const YAML: &str = "
specVersion: 0.0.4
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
graft:
  base: Qmbase
  block: 1
";
    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated = resolve_unvalidated(YAML).await;
        let error_msg = unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .expect("There must be a FeatureValidation error")
            .to_string();
        assert_eq!(
            "The feature `grafting` is used by the subgraph but it is not declared in the manifest.",
            error_msg
        )
    })
}

#[test]
fn declared_grafting_feature_causes_no_feature_validation_errors() {
    const YAML: &str = "
specVersion: 0.0.4
features:
  - grafting
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
graft:
  base: Qmbase
  block: 1
";
    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated = resolve_unvalidated(YAML).await;
        assert!(unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .is_none());
        let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;
        assert!(manifest.features.contains(&SubgraphFeature::Grafting))
    })
}

#[test]
fn declared_non_fatal_errors_feature_causes_no_feature_validation_errors() {
    const YAML: &str = "
specVersion: 0.0.4
features:
  - nonFatalErrors
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
";
    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated = resolve_unvalidated(YAML).await;
        assert!(unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .is_none());

        let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;
        assert!(manifest.features.contains(&SubgraphFeature::NonFatalErrors))
    });
}

#[test]
fn declared_full_text_search_feature_causes_no_feature_validation_errors() {
    const YAML: &str = "
specVersion: 0.0.4
features:
  - fullTextSearch
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
";

    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated: UnvalidatedSubgraphManifest<Chain> = {
            let mut resolver = TextResolver::default();
            let id = DeploymentHash::new("Qmmanifest").unwrap();
            resolver.add(id.as_str(), &YAML);
            resolver.add("/ipfs/Qmabi", &ABI);
            resolver.add("/ipfs/Qmschema", &GQL_SCHEMA_FULLTEXT);

            let resolver: Arc<dyn LinkResolverTrait> = Arc::new(resolver);

            let raw = serde_yaml::from_str(YAML).unwrap();
            UnvalidatedSubgraphManifest::resolve(
                id,
                raw,
                &resolver,
                &LOGGER,
                SPEC_VERSION_0_0_4.clone(),
            )
            .await
            .expect("Parsing simple manifest works")
        };

        assert!(unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .is_none());

        let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;
        assert!(manifest.features.contains(&SubgraphFeature::FullTextSearch))
    });
}

#[test]
fn undeclared_full_text_search_feature_causes_no_feature_validation_errors() {
    const YAML: &str = "
specVersion: 0.0.4

dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
";

    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated: UnvalidatedSubgraphManifest<Chain> = {
            let mut resolver = TextResolver::default();
            let id = DeploymentHash::new("Qmmanifest").unwrap();
            resolver.add(id.as_str(), &YAML);
            resolver.add("/ipfs/Qmabi", &ABI);
            resolver.add("/ipfs/Qmschema", &GQL_SCHEMA_FULLTEXT);

            let resolver: Arc<dyn LinkResolverTrait> = Arc::new(resolver);

            let raw = serde_yaml::from_str(YAML).unwrap();
            UnvalidatedSubgraphManifest::resolve(
                id,
                raw,
                &resolver,
                &LOGGER,
                SPEC_VERSION_0_0_4.clone(),
            )
            .await
            .expect("Parsing simple manifest works")
        };

        let error_msg = unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .expect("There must be a FeatureValidationError")
            .to_string();

        assert_eq!(
            "The feature `fullTextSearch` is used by the subgraph but it is not declared in the manifest.",
            error_msg
        );
    });
}

#[test]
fn undeclared_ipfs_on_ethereum_contracts_feature_causes_feature_validation_error() {
    const YAML: &str = "
specVersion: 0.0.4
schema:
  file:
    /: /ipfs/Qmschema
dataSources:
  - kind: ethereum/contract
    name: Factory
    network: mainnet
    source:
      abi: Factory
      startBlock: 9562480
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.4
      language: wasm/assemblyscript
      entities:
        - TestEntity
      file:
        /: /ipfs/Qmmapping
      abis:
        - name: Factory
          file:
            /: /ipfs/Qmabi
      callHandlers:
        - function: get(address)
          handler: handleget
";

    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated: UnvalidatedSubgraphManifest<Chain> = {
            let mut resolver = TextResolver::default();
            let id = DeploymentHash::new("Qmmanifest").unwrap();
            resolver.add(id.as_str(), &YAML);
            resolver.add("/ipfs/Qmabi", &ABI);
            resolver.add("/ipfs/Qmschema", &GQL_SCHEMA);
            resolver.add("/ipfs/Qmmapping", &MAPPING_WITH_IPFS_FUNC_WASM);

            let resolver: Arc<dyn LinkResolverTrait> = Arc::new(resolver);

            let raw = serde_yaml::from_str(YAML).unwrap();
            UnvalidatedSubgraphManifest::resolve(
                id,
                raw,
                &resolver,
                &LOGGER,
                SPEC_VERSION_0_0_4.clone(),
            )
            .await
            .expect("Parsing simple manifest works")
        };

        let error_msg = unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .expect("There must be a FeatureValidationError")
            .to_string();

        assert_eq!(
            "The feature `ipfsOnEthereumContracts` is used by the subgraph but it is not declared in the manifest.",
            error_msg
        );
    });
}

#[test]
fn declared_ipfs_on_ethereum_contracts_feature_causes_no_errors() {
    const YAML: &str = "
specVersion: 0.0.4
schema:
  file:
    /: /ipfs/Qmschema
features:
  - ipfsOnEthereumContracts
dataSources:
  - kind: ethereum/contract
    name: Factory
    network: mainnet
    source:
      abi: Factory
      startBlock: 9562480
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.4
      language: wasm/assemblyscript
      entities:
        - TestEntity
      file:
        /: /ipfs/Qmmapping
      abis:
        - name: Factory
          file:
            /: /ipfs/Qmabi
      callHandlers:
        - function: get(address)
          handler: handleget
";

    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated: UnvalidatedSubgraphManifest<Chain> = {
            let mut resolver = TextResolver::default();
            let id = DeploymentHash::new("Qmmanifest").unwrap();
            resolver.add(id.as_str(), &YAML);
            resolver.add("/ipfs/Qmabi", &ABI);
            resolver.add("/ipfs/Qmschema", &GQL_SCHEMA);
            resolver.add("/ipfs/Qmmapping", &MAPPING_WITH_IPFS_FUNC_WASM);

            let resolver: Arc<dyn LinkResolverTrait> = Arc::new(resolver);

            let raw = serde_yaml::from_str(YAML).unwrap();
            UnvalidatedSubgraphManifest::resolve(
                id,
                raw,
                &resolver,
                &LOGGER,
                SPEC_VERSION_0_0_4.clone(),
            )
            .await
            .expect("Parsing simple manifest works")
        };

        assert!(unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .is_none());
    });
}

#[test]
fn can_detect_features_in_subgraphs_with_spec_version_lesser_than_0_0_4() {
    const YAML: &str = "
specVersion: 0.0.2
features:
  - nonFatalErrors
dataSources: []
schema:
  file:
    /: /ipfs/Qmschema
";
    test_store::run_test_sequentially(|store| async move {
        let store = store.subgraph_store();
        let unvalidated = resolve_unvalidated(YAML).await;
        assert!(unvalidated
            .validate(store.clone(), true)
            .await
            .expect_err("Validation must fail")
            .into_iter()
            .find(|e| {
                matches!(
                    e,
                    SubgraphManifestValidationError::FeatureValidationError(_)
                )
            })
            .is_none());

        let manifest = resolve_manifest(YAML, SPEC_VERSION_0_0_4).await;
        assert!(manifest.features.contains(&SubgraphFeature::NonFatalErrors))
    });
}
