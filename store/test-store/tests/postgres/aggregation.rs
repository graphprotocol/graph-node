use std::fmt::Write;
use std::{future::Future, sync::Arc};

use graph::{
    blockchain::{block_stream::FirehoseCursor, BlockPtr, BlockTime},
    components::{
        metrics::stopwatch::StopwatchMetrics,
        store::{
            AttributeNames, BlockNumber, DeploymentLocator, EntityCache, EntityCollection,
            EntityOperation, EntityQuery, ReadStore, StoreError, SubgraphStore as _, WritableStore,
        },
    },
    data::{
        store::{
            scalar::{BigDecimal, Bytes},
            Entity, Value,
        },
        subgraph::DeploymentHash,
    },
    entity,
    prelude::lazy_static,
    schema::InputSchema,
};
use graph_store_postgres::{Store as DieselStore, SubgraphStore};
use test_store::{create_test_subgraph, run_test_sequentially, BLOCKS, LOGGER, METRICS_REGISTRY};

const SCHEMA: &str = r#"
type Data @entity(timeseries: true) {
    id: Int8!
    timestamp: Timestamp!
    token: Bytes!
    price: BigDecimal!
    amount: BigDecimal!
  }

  type Stats @aggregation(intervals: ["day", "hour"], source: "Data") {
    id: Int8!
    timestamp: Timestamp!
    token: Bytes!
    sum: BigDecimal! @aggregate(fn: "sum", arg: "price")
    sum_sq: BigDecimal! @aggregate(fn: "sum", arg: "price * price")
    max: BigDecimal! @aggregate(fn: "max", arg: "amount")
    first: BigDecimal @aggregate(fn: "first", arg: "amount")
    last: BigDecimal! @aggregate(fn: "last", arg: "amount")
    value: BigDecimal! @aggregate(fn: "sum", arg: "price * amount")
    totalValue: BigDecimal! @aggregate(fn: "sum", arg: "price * amount", cumulative: true)
  }

  type TotalStats @aggregation(intervals: ["hour"], source: "Data") {
    id: Int8!
    timestamp: Timestamp!
    max: BigDecimal! @aggregate(fn: "max", arg: "price")
  }
  "#;

fn minutes(n: u32) -> BlockTime {
    BlockTime::since_epoch(n as i64 * 60, 0)
}

lazy_static! {
    static ref TOKEN1: Bytes = "0xdeadbeef01".parse().unwrap();
    static ref TOKEN2: Bytes = "0xdeadbeef02".parse().unwrap();
    static ref TIMES: Vec<BlockTime> = vec![minutes(30), minutes(40), minutes(65), minutes(120)];
}

fn remove_test_data(store: Arc<SubgraphStore>) {
    store
        .delete_all_entities_for_test_use_only()
        .expect("deleting test entities succeeds");
}

pub async fn insert(
    store: &Arc<dyn WritableStore>,
    deployment: &DeploymentLocator,
    block_ptr_to: BlockPtr,
    block_time: BlockTime,
    entities: Vec<Entity>,
) -> Result<(), StoreError> {
    let schema = ReadStore::input_schema(store);
    let ops = entities
        .into_iter()
        .map(|data| {
            let data_type = schema.entity_type("Data").unwrap();
            let key = data_type.key(data.id());
            EntityOperation::Set { data, key }
        })
        .collect();

    let mut entity_cache = EntityCache::new(Arc::new(store.clone()));
    entity_cache.append(ops);
    let mods = entity_cache
        .as_modifications(block_ptr_to.number)
        .expect("failed to convert to modifications")
        .modifications;
    let metrics_registry = METRICS_REGISTRY.clone();
    let stopwatch_metrics = StopwatchMetrics::new(
        LOGGER.clone(),
        deployment.hash.clone(),
        "transact",
        metrics_registry.clone(),
        store.shard().to_string(),
    );
    store
        .transact_block_operations(
            block_ptr_to,
            block_time,
            FirehoseCursor::None,
            mods,
            &stopwatch_metrics,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            false,
            false,
        )
        .await
}

fn bd(n: i32) -> Value {
    Value::BigDecimal(BigDecimal::from(n))
}

async fn insert_test_data(store: Arc<dyn WritableStore>, deployment: DeploymentLocator) {
    let schema = ReadStore::input_schema(&store);

    let ts64 = TIMES[0];
    let entities = vec![
        entity! { schema => id: 1i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(1), amount: bd(10) },
        entity! { schema => id: 2i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(1), amount: bd(1) },
    ];

    insert(&store, &deployment, BLOCKS[0].clone(), TIMES[0], entities)
        .await
        .unwrap();

    let ts64 = TIMES[1];
    let entities = vec![
        entity! { schema => id: 11i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(2), amount: bd(2) },
        entity! { schema => id: 12i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(2), amount: bd(20) },
    ];
    insert(&store, &deployment, BLOCKS[1].clone(), TIMES[1], entities)
        .await
        .unwrap();

    let ts64 = TIMES[2];
    let entities = vec![
        entity! { schema => id: 21i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(3), amount: bd(30) },
        entity! { schema => id: 22i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(3), amount: bd(3) },
    ];
    insert(&store, &deployment, BLOCKS[2].clone(), TIMES[2], entities)
        .await
        .unwrap();

    let ts64 = TIMES[3];
    let entities = vec![
        entity! { schema => id: 31i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(4), amount: bd(4) },
        entity! { schema => id: 32i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(4), amount: bd(40) },
    ];
    insert(&store, &deployment, BLOCKS[3].clone(), TIMES[3], entities)
        .await
        .unwrap();

    store.flush().await.unwrap();
}

fn stats_hour(schema: &InputSchema) -> Vec<Vec<Entity>> {
    // Note that an aggregation that is marked with block N will only
    // contain data up to block N-1 since we do the aggregation at the first
    // block after the aggregation interval has finished

    // Stats_hour aggregations over BLOCKS[0..=1], i.e., at BLOCKS[2]
    let ts2 = BlockTime::since_epoch(0, 0);
    let block2 = vec![
        entity! { schema => id: 11i64, timestamp: ts2, token: TOKEN1.clone(),
        sum: bd(3), sum_sq: bd(5), max: bd(10), first: bd(10), last: bd(2),
        value: bd(14), totalValue: bd(14) },
        entity! { schema => id: 12i64, timestamp: ts2, token: TOKEN2.clone(),
        sum: bd(3), sum_sq: bd(5), max: bd(20), first: bd(1),  last: bd(20),
        value: bd(41), totalValue: bd(41) },
    ];

    let ts3 = BlockTime::since_epoch(3600, 0);
    let block3 = {
        let mut v1 = block2.clone();
        // Stats_hour aggregations over BLOCKS[2], i.e., at BLOCKS[3]
        let mut v2 = vec![
            entity! { schema => id: 21i64, timestamp: ts3, token: TOKEN1.clone(),
            sum: bd(3), sum_sq: bd(9), max: bd(30), first: bd(30), last: bd(30),
            value: bd(90), totalValue: bd(104) },
            entity! { schema => id: 22i64, timestamp: ts3, token: TOKEN2.clone(),
            sum: bd(3), sum_sq: bd(9), max: bd(3),  first: bd(3), last: bd(3),
            value: bd(9), totalValue: bd(50)},
        ];
        v1.append(&mut v2);
        v1
    };

    vec![vec![], vec![], block2, block3]
}

struct TestEnv {
    store: Arc<DieselStore>,
    writable: Arc<dyn WritableStore>,
    deployment: DeploymentLocator,
}

impl TestEnv {
    #[track_caller]
    fn all_entities(&self, entity_type: &str, block: BlockNumber) -> Vec<Entity> {
        let entity_type = self
            .writable
            .input_schema()
            .entity_type(entity_type)
            .expect("we got an existing entity type");
        let query = EntityQuery::new(
            self.deployment.hash.clone(),
            block,
            EntityCollection::All(vec![(entity_type, AttributeNames::All)]),
        );
        self.store
            .subgraph_store()
            .find(query)
            .expect("query succeeds")
    }
}

fn run_test<R, F>(test: F)
where
    F: FnOnce(TestEnv) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
{
    run_test_sequentially(|store| async move {
        let subgraph_store = store.subgraph_store();
        // Reset state before starting
        remove_test_data(subgraph_store.clone());

        // Seed database with test data
        let hash = DeploymentHash::new("rollupSubgraph").unwrap();
        let loc = create_test_subgraph(&hash, SCHEMA).await;
        let writable = store
            .subgraph_store()
            .writable(LOGGER.clone(), loc.id, Arc::new(Vec::new()))
            .await
            .expect("we can get a writable store");
        insert_test_data(writable.clone(), loc.clone()).await;

        // Run test and wait for the background writer to finish its work so
        // it won't conflict with the next test
        let env = TestEnv {
            store: store.clone(),
            writable: writable.clone(),
            deployment: loc.clone(),
        };
        test(env).await;
        writable.flush().await.unwrap();
    });
}

fn entity_diff(left: &[Entity], right: &[Entity]) -> Result<String, std::fmt::Error> {
    let mut diff = String::new();
    for (i, (l, r)) in left.iter().zip(right.iter()).enumerate() {
        if l != r {
            writeln!(
                diff,
                "entities #{}(left: {}, right: {}) differ:",
                i,
                l.id(),
                r.id()
            )?;
            for (k, v) in l.clone().sorted() {
                match r.get(&k) {
                    None => writeln!(diff, "  {}: left: {} right: missing", k, v)?,
                    Some(v2) if &v != v2 => writeln!(diff, "  {}: left: {} right: {}", k, v, v2)?,
                    _ => (),
                }
            }
            for (k, v) in r.clone().sorted() {
                if !l.contains_key(&k) {
                    writeln!(diff, "  {}: left: missing right: {}", k, v)?;
                }
            }
        }
    }
    Ok(diff)
}

#[test]
fn simple() {
    run_test(|env| async move {
        let x = env.all_entities("Stats_day", BlockNumber::MAX);
        assert_eq!(Vec::<Entity>::new(), x);

        let exp = stats_hour(&env.writable.input_schema());
        for i in 0..4 {
            let act = env.all_entities("Stats_hour", BLOCKS[i].number);
            let diff = entity_diff(&exp[i], &act).unwrap();
            if !diff.is_empty() {
                panic!("entities for BLOCKS[{}] differ:\n{}", i, diff);
            }
            assert_eq!(exp[i], act, "entities for BLOCKS[{}] are the same", i);
        }
    })
}
