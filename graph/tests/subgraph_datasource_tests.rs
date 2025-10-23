use std::{collections::BTreeMap, ops::Range, sync::Arc};

use graph::{
    blockchain::{
        block_stream::{
            EntityOperationKind, EntitySourceOperation, SubgraphTriggerScanRange,
            TriggersAdapterWrapper,
        },
        mock::MockTriggersAdapter,
        Block, SubgraphFilter, Trigger,
    },
    components::store::SourceableStore,
    data_source::CausalityRegion,
    prelude::{BlockHash, BlockNumber, BlockPtr, DeploymentHash, StoreError, Value},
    schema::{EntityType, InputSchema},
};
use slog::Logger;
use tonic::async_trait;

pub struct MockSourcableStore {
    entities: BTreeMap<BlockNumber, Vec<EntitySourceOperation>>,
    schema: InputSchema,
    block_ptr: Option<BlockPtr>,
}

impl MockSourcableStore {
    pub fn new(
        entities: BTreeMap<BlockNumber, Vec<EntitySourceOperation>>,
        schema: InputSchema,
        block_ptr: Option<BlockPtr>,
    ) -> Self {
        Self {
            entities,
            schema,
            block_ptr,
        }
    }

    pub fn set_block_ptr(&mut self, ptr: BlockPtr) {
        self.block_ptr = Some(ptr);
    }

    pub fn clear_block_ptr(&mut self) {
        self.block_ptr = None;
    }

    pub fn increment_block(&mut self) -> Result<(), &'static str> {
        if let Some(ptr) = &self.block_ptr {
            let new_number = ptr.number + 1;
            self.block_ptr = Some(BlockPtr::new(ptr.hash.clone(), new_number));
            Ok(())
        } else {
            Err("No block pointer set")
        }
    }

    pub fn decrement_block(&mut self) -> Result<(), &'static str> {
        if let Some(ptr) = &self.block_ptr {
            if ptr.number == 0 {
                return Err("Block number already at 0");
            }
            let new_number = ptr.number - 1;
            self.block_ptr = Some(BlockPtr::new(ptr.hash.clone(), new_number));
            Ok(())
        } else {
            Err("No block pointer set")
        }
    }
}

#[async_trait]
impl SourceableStore for MockSourcableStore {
    async fn get_range(
        &self,
        entity_types: Vec<EntityType>,
        _causality_region: CausalityRegion,
        block_range: Range<BlockNumber>,
    ) -> Result<BTreeMap<BlockNumber, Vec<EntitySourceOperation>>, StoreError> {
        Ok(self
            .entities
            .range(block_range)
            .map(|(block_num, operations)| {
                let filtered_ops: Vec<EntitySourceOperation> = operations
                    .iter()
                    .filter(|op| entity_types.contains(&op.entity_type))
                    .cloned()
                    .collect();
                (*block_num, filtered_ops)
            })
            .filter(|(_, ops)| !ops.is_empty())
            .collect())
    }

    fn input_schema(&self) -> InputSchema {
        self.schema.clone()
    }

    async fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        Ok(self.block_ptr.clone())
    }
}

#[graph::test]
async fn test_triggers_adapter_with_entities() {
    let id = DeploymentHash::new("test_deployment").unwrap();
    let schema = InputSchema::parse_latest(
        r#"
        type User @entity { 
            id: String!
            name: String!
            age: Int 
        }
        type Post @entity {
            id: String!
            title: String!
            author: String!
        }
        "#,
        id.clone(),
    )
    .unwrap();

    let user1 = schema
        .make_entity(vec![
            ("id".into(), Value::String("user1".to_owned())),
            ("name".into(), Value::String("Alice".to_owned())),
            ("age".into(), Value::Int(30)),
        ])
        .unwrap();

    let user2 = schema
        .make_entity(vec![
            ("id".into(), Value::String("user2".to_owned())),
            ("name".into(), Value::String("Bob".to_owned())),
            ("age".into(), Value::Int(25)),
        ])
        .unwrap();

    let post = schema
        .make_entity(vec![
            ("id".into(), Value::String("post1".to_owned())),
            ("title".into(), Value::String("Test Post".to_owned())),
            ("author".into(), Value::String("user1".to_owned())),
        ])
        .unwrap();

    let user_type = schema.entity_type("User").unwrap();
    let post_type = schema.entity_type("Post").unwrap();

    let entity1 = EntitySourceOperation {
        entity_type: user_type.clone(),
        entity: user1,
        entity_op: EntityOperationKind::Create,
        vid: 1,
    };

    let entity2 = EntitySourceOperation {
        entity_type: user_type,
        entity: user2,
        entity_op: EntityOperationKind::Create,
        vid: 2,
    };

    let post_entity = EntitySourceOperation {
        entity_type: post_type,
        entity: post,
        entity_op: EntityOperationKind::Create,
        vid: 3,
    };

    let mut entities = BTreeMap::new();
    entities.insert(1, vec![entity1, post_entity]); // Block 1 has both User and Post
    entities.insert(2, vec![entity2]); // Block 2 has only User

    // Create block hash and store
    let hash_bytes: [u8; 32] = [0u8; 32];
    let block_hash = BlockHash(hash_bytes.to_vec().into_boxed_slice());
    let initial_block = BlockPtr::new(block_hash, 0);
    let store = Arc::new(MockSourcableStore::new(
        entities,
        schema.clone(),
        Some(initial_block),
    ));

    let adapter = Arc::new(MockTriggersAdapter {});
    let wrapper = TriggersAdapterWrapper::new(adapter, vec![store]);

    // Filter only for User entities
    let filter = SubgraphFilter {
        subgraph: id,
        start_block: 0,
        entities: vec!["User".to_string()], // Only monitoring User entities
        manifest_idx: 0,
    };

    let logger = Logger::root(slog::Discard, slog::o!());
    let result = wrapper
        .blocks_with_subgraph_triggers(&logger, &[filter], SubgraphTriggerScanRange::Range(1, 3))
        .await;

    assert!(result.is_ok(), "Failed to get triggers: {:?}", result.err());
    let blocks = result.unwrap();

    assert_eq!(
        blocks.len(),
        3,
        "Should have found blocks with entities plus the last block"
    );

    let block1 = &blocks[0];
    assert_eq!(block1.block.number(), 1, "First block should be number 1");
    let triggers1 = &block1.trigger_data;
    assert_eq!(
        triggers1.len(),
        1,
        "Block 1 should have exactly one trigger (User, not Post)"
    );

    if let Trigger::Subgraph(trigger_data) = &triggers1[0] {
        assert_eq!(
            trigger_data.entity.entity_type.as_str(),
            "User",
            "Trigger should be for User entity"
        );
        assert_eq!(
            trigger_data.entity.vid, 1,
            "Should be the first User entity"
        );
    } else {
        panic!("Expected subgraph trigger");
    }

    let block2 = &blocks[1];
    assert_eq!(block2.block.number(), 2, "Second block should be number 2");
    let triggers2 = &block2.trigger_data;
    assert_eq!(
        triggers2.len(),
        1,
        "Block 2 should have exactly one trigger"
    );

    if let Trigger::Subgraph(trigger_data) = &triggers2[0] {
        assert_eq!(
            trigger_data.entity.entity_type.as_str(),
            "User",
            "Trigger should be for User entity"
        );
        assert_eq!(
            trigger_data.entity.vid, 2,
            "Should be the second User entity"
        );
    } else {
        panic!("Expected subgraph trigger");
    }

    let block3 = &blocks[2];
    assert_eq!(block3.block.number(), 3, "Third block should be number 3");
    let triggers3 = &block3.trigger_data;
    assert_eq!(
        triggers3.len(),
        0,
        "Block 3 should have no triggers but be included as it's the last block"
    );
}
