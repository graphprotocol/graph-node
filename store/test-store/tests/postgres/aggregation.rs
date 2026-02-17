use std::collections::BTreeSet;
use std::fmt::Write;
use std::{future::Future, sync::Arc};

use graph::{
    blockchain::{block_stream::FirehoseCursor, BlockPtr, BlockTime},
    components::{
        metrics::stopwatch::StopwatchMetrics,
        store::{
            AggregationCurrent, AttributeNames, BlockNumber, ChildMultiplicity, DeploymentLocator,
            EntityCache, EntityCollection, EntityFilter, EntityLink, EntityOperation, EntityQuery,
            EntityWindow, ReadStore, StoreError, SubgraphStore as _, WindowAttribute,
            WritableStore,
        },
    },
    data::{
        store::{
            scalar::{BigDecimal, Bytes, Timestamp},
            Entity, Id, IdList, Value,
        },
        subgraph::DeploymentHash,
    },
    entity,
    prelude::lazy_static,
    schema::InputSchema,
};
use graph_store_postgres::Store as DieselStore;
use test_store::{
    create_test_subgraph, remove_subgraphs, run_test_sequentially, BLOCKS, LOGGER, METRICS_REGISTRY,
};

const SCHEMA: &str = r#"
type Token @entity {
    id: Bytes!
    name: String!
  }

  type Data @entity(timeseries: true) {
    id: Int8!
    timestamp: Timestamp!
    token: Token!
    price: BigDecimal!
    amount: BigDecimal!
  }

  type Stats @aggregation(intervals: ["day", "hour"], source: "Data") {
    id: Int8!
    timestamp: Timestamp!
    token: Token!
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
    static ref TOKEN3: Bytes = "0xdeadbeef03".parse().unwrap();
    static ref TIMES: Vec<BlockTime> = vec![minutes(30), minutes(40), minutes(65), minutes(121)];
}

const STATS_HOUR_FIELDS: &[&str] = &[
    "id",
    "timestamp",
    "token",
    "sum",
    "sum_sq",
    "max",
    "first",
    "last",
    "value",
    "totalValue",
];

pub async fn insert_entities(
    store: &Arc<dyn WritableStore>,
    deployment: &DeploymentLocator,
    block_ptr_to: BlockPtr,
    block_time: BlockTime,
    typed_entities: Vec<(&str, Vec<Entity>)>,
) -> Result<(), StoreError> {
    let schema = ReadStore::input_schema(store);
    let ops: Vec<EntityOperation> = typed_entities
        .into_iter()
        .flat_map(|(type_name, entities)| {
            let entity_type = schema.entity_type(type_name).unwrap();
            entities.into_iter().map(move |mut data| {
                let key = entity_type.key(data.id());
                data.set_vid_if_empty();
                EntityOperation::Set { data, key }
            })
        })
        .collect();

    let mut entity_cache = EntityCache::new(Arc::new(store.clone()));
    entity_cache.append(ops);
    let mods = entity_cache
        .as_modifications(block_ptr_to.number)
        .await
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

pub async fn insert(
    store: &Arc<dyn WritableStore>,
    deployment: &DeploymentLocator,
    block_ptr_to: BlockPtr,
    block_time: BlockTime,
    entities: Vec<Entity>,
) -> Result<(), StoreError> {
    insert_entities(
        store,
        deployment,
        block_ptr_to,
        block_time,
        vec![("Data", entities)],
    )
    .await
}

fn bd(n: i32) -> Value {
    Value::BigDecimal(BigDecimal::from(n))
}

fn ts(epoch_secs: i64) -> Value {
    Value::Timestamp(Timestamp::since_epoch(epoch_secs, 0).unwrap())
}

async fn insert_test_data(store: Arc<dyn WritableStore>, deployment: DeploymentLocator) {
    let schema = ReadStore::input_schema(&store);

    // Insert Token entities alongside first Data entities at BLOCKS[0]
    let token_entities = vec![
        entity! { schema => id: TOKEN1.clone(), name: "Token1", vid: 1i64 },
        entity! { schema => id: TOKEN2.clone(), name: "Token2", vid: 2i64 },
        entity! { schema => id: TOKEN3.clone(), name: "Token3", vid: 3i64 },
    ];
    let ts64 = TIMES[0];
    let data_entities = vec![
        entity! { schema => id: 1i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(1), amount: bd(10), vid: 11i64 },
        entity! { schema => id: 2i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(1), amount: bd(1), vid: 12i64 },
    ];

    insert_entities(
        &store,
        &deployment,
        BLOCKS[0].clone(),
        TIMES[0],
        vec![("Token", token_entities), ("Data", data_entities)],
    )
    .await
    .unwrap();

    let ts64 = TIMES[1];
    let entities = vec![
        entity! { schema => id: 11i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(2), amount: bd(2), vid: 21i64 },
        entity! { schema => id: 12i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(2), amount: bd(20), vid: 22i64 },
    ];
    insert(&store, &deployment, BLOCKS[1].clone(), TIMES[1], entities)
        .await
        .unwrap();

    let ts64 = TIMES[2];
    let entities = vec![
        entity! { schema => id: 21i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(3), amount: bd(30), vid: 31i64 },
        entity! { schema => id: 22i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(3), amount: bd(3), vid: 32i64 },
    ];
    insert(&store, &deployment, BLOCKS[2].clone(), TIMES[2], entities)
        .await
        .unwrap();

    let ts64 = TIMES[3];
    let entities = vec![
        entity! { schema => id: 31i64, timestamp: ts64, token: TOKEN1.clone(), price: bd(4), amount: bd(4), vid: 41i64 },
        entity! { schema => id: 32i64, timestamp: ts64, token: TOKEN2.clone(), price: bd(4), amount: bd(40), vid: 42i64 },
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
        value: bd(14), totalValue: bd(14), vid: 1i64 },
        entity! { schema => id: 12i64, timestamp: ts2, token: TOKEN2.clone(),
        sum: bd(3), sum_sq: bd(5), max: bd(20), first: bd(1),  last: bd(20),
        value: bd(41), totalValue: bd(41), vid: 2i64 },
    ];

    let ts3 = BlockTime::since_epoch(3600, 0);
    let block3 = {
        let mut v1 = block2.clone();
        // Stats_hour aggregations over BLOCKS[2], i.e., at BLOCKS[3]
        let mut v2 = vec![
            entity! { schema => id: 21i64, timestamp: ts3, token: TOKEN1.clone(),
            sum: bd(3), sum_sq: bd(9), max: bd(30), first: bd(30), last: bd(30),
            value: bd(90), totalValue: bd(104), vid: 3i64 },
            entity! { schema => id: 22i64, timestamp: ts3, token: TOKEN2.clone(),
            sum: bd(3), sum_sq: bd(9), max: bd(3),  first: bd(3), last: bd(3),
            value: bd(9), totalValue: bd(50), vid: 4i64 },
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
    async fn all_entities(&self, entity_type: &str, block: BlockNumber) -> Vec<Entity> {
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
            .await
            .expect("query succeeds")
    }

    /// Build an `EntityQuery` for an aggregation entity with explicit column
    /// selection and configurable `AggregationCurrent`. The current bucket
    /// UNION ALL query requires `AttributeNames::Select` (not `All`).
    fn aggregation_query(
        &self,
        entity_type_name: &str,
        current: AggregationCurrent,
        block: BlockNumber,
    ) -> EntityQuery {
        let schema = self.writable.input_schema();
        let entity_type = schema
            .entity_type(entity_type_name)
            .expect("entity type exists");

        let columns: BTreeSet<String> = STATS_HOUR_FIELDS.iter().map(|s| s.to_string()).collect();
        let mut query = EntityQuery::new(
            self.deployment.hash.clone(),
            block,
            EntityCollection::All(vec![(entity_type, AttributeNames::Select(columns))]),
        );
        query.aggregation_current = Some(current);
        query
    }

    async fn find_aggregation(
        &self,
        entity_type_name: &str,
        current: AggregationCurrent,
        block: BlockNumber,
    ) -> Vec<Entity> {
        let query = self.aggregation_query(entity_type_name, current, block);
        self.store
            .subgraph_store()
            .find(query)
            .await
            .expect("aggregation query succeeds")
    }

    /// Build an `EntityQuery` for a nested aggregation using
    /// `EntityCollection::Window` where Token is the parent and Stats_hour
    /// is the child.
    fn nested_aggregation_query(
        &self,
        current: AggregationCurrent,
        block: BlockNumber,
        parent_ids: &[&Bytes],
    ) -> EntityQuery {
        let schema = self.writable.input_schema();
        let child_type = schema.entity_type("Stats_hour").expect("Stats_hour exists");

        let columns: BTreeSet<String> = STATS_HOUR_FIELDS.iter().map(|s| s.to_string()).collect();

        let mut ids = IdList::new(graph::data::store::IdType::Bytes);
        for pid in parent_ids {
            ids.push(Id::Bytes((*pid).clone())).expect("push parent id");
        }

        let window = EntityWindow {
            child_type,
            ids,
            link: EntityLink::Direct(
                WindowAttribute::Scalar("token".to_string()),
                ChildMultiplicity::Many,
            ),
            column_names: AttributeNames::Select(columns),
        };

        let mut query = EntityQuery::new(
            self.deployment.hash.clone(),
            block,
            EntityCollection::Window(vec![window]),
        );
        query.aggregation_current = Some(current);
        query
    }

    async fn find_nested_aggregation(
        &self,
        current: AggregationCurrent,
        block: BlockNumber,
        parent_ids: &[&Bytes],
    ) -> Vec<Entity> {
        let query = self.nested_aggregation_query(current, block, parent_ids);
        self.store
            .subgraph_store()
            .find(query)
            .await
            .expect("nested aggregation query succeeds")
    }
}

fn run_test<R, F>(test: F)
where
    F: FnOnce(TestEnv) -> R + Send + 'static,
    R: Future<Output = ()> + Send + 'static,
{
    run_test_sequentially(|store| async move {
        // Reset state before starting
        remove_subgraphs().await;

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
        let x = env.all_entities("Stats_day", BlockNumber::MAX).await;
        assert_eq!(Vec::<Entity>::new(), x);

        let exp = stats_hour(&env.writable.input_schema());
        for i in 0..4 {
            let act = env.all_entities("Stats_hour", BLOCKS[i].number).await;
            let diff = entity_diff(&exp[i], &act).unwrap();
            if !diff.is_empty() {
                panic!("entities for BLOCKS[{}] differ:\n{}", i, diff);
            }
            assert_eq!(exp[i], act, "entities for BLOCKS[{}] are the same", i);
        }
    })
}

/// Query Stats_hour with AggregationCurrent::Include at BLOCKS[3].
/// Expect 6 rows: 2 rolled-up from hour 0, 2 from hour 1, and 2
/// current-bucket rows from hour 2 computed on-the-fly.
#[test]
fn current_include() {
    run_test(|env| async move {
        let result = env
            .find_aggregation("Stats_hour", AggregationCurrent::Include, BLOCKS[3].number)
            .await;

        assert_eq!(
            6,
            result.len(),
            "expected 6 rows (2 per hour for hours 0, 1, 2), got {}",
            result.len()
        );

        let schema = env.writable.input_schema();

        // The rolled-up rows (hours 0 and 1) should match the existing
        // stats_hour expected data at BLOCKS[3].
        let exp_rolled = stats_hour(&schema);
        let hour2_ts = ts(7200);
        let rolled_up: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("timestamp").unwrap() != &hour2_ts)
            .collect();
        assert_eq!(
            4,
            rolled_up.len(),
            "expected 4 rolled-up rows, got {}",
            rolled_up.len()
        );

        // Verify rolled-up entities match
        for exp_entity in &exp_rolled[3] {
            let matching = rolled_up.iter().find(|e| e.id() == exp_entity.id());
            assert!(
                matching.is_some(),
                "expected rolled-up entity with id {} not found",
                exp_entity.id()
            );
        }

        // Current bucket rows (hour 2, timestamp = 7200)
        let current_bucket: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("timestamp").unwrap() == &hour2_ts)
            .collect();
        assert_eq!(
            2,
            current_bucket.len(),
            "expected 2 current-bucket rows for hour 2, got {}",
            current_bucket.len()
        );

        // Verify current bucket values for TOKEN1:
        // At BLOCKS[3], minute 120: price=4, amount=4
        // sum(price) = 4, sum(price*price) = 16, max(amount) = 4,
        // first(amount) = 4, last(amount) = 4, sum(price*amount) = 16
        let token1_current = current_bucket
            .iter()
            .find(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN1.clone()))
            .expect("TOKEN1 current bucket row exists");
        assert_eq!(token1_current.get("sum").unwrap(), &bd(4));
        assert_eq!(token1_current.get("sum_sq").unwrap(), &bd(16));
        assert_eq!(token1_current.get("max").unwrap(), &bd(4));
        assert_eq!(token1_current.get("first").unwrap(), &bd(4));
        assert_eq!(token1_current.get("last").unwrap(), &bd(4));
        assert_eq!(token1_current.get("value").unwrap(), &bd(16));

        // Verify current bucket values for TOKEN2:
        // At BLOCKS[3], minute 120: price=4, amount=40
        // sum(price) = 4, sum(price*price) = 16, max(amount) = 40,
        // first(amount) = 40, last(amount) = 40, sum(price*amount) = 160
        let token2_current = current_bucket
            .iter()
            .find(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN2.clone()))
            .expect("TOKEN2 current bucket row exists");
        assert_eq!(token2_current.get("sum").unwrap(), &bd(4));
        assert_eq!(token2_current.get("sum_sq").unwrap(), &bd(16));
        assert_eq!(token2_current.get("max").unwrap(), &bd(40));
        assert_eq!(token2_current.get("first").unwrap(), &bd(40));
        assert_eq!(token2_current.get("last").unwrap(), &bd(40));
        assert_eq!(token2_current.get("value").unwrap(), &bd(160));
    })
}

/// Query Stats_hour with AggregationCurrent::Exclude at BLOCKS[3].
/// Should return only the 4 rolled-up rows, same as the `simple` test.
#[test]
fn current_exclude() {
    run_test(|env| async move {
        let result = env
            .find_aggregation("Stats_hour", AggregationCurrent::Exclude, BLOCKS[3].number)
            .await;

        let exp = stats_hour(&env.writable.input_schema());
        assert_eq!(
            exp[3].len(),
            result.len(),
            "expected {} rolled-up rows, got {}",
            exp[3].len(),
            result.len()
        );

        let diff = entity_diff(&exp[3], &result).unwrap();
        if !diff.is_empty() {
            panic!(
                "current_exclude results differ from expected rolled-up data:\n{}",
                diff
            );
        }
    })
}

/// Query Stats_hour with AggregationCurrent::Include and an EntityFilter
/// on token == TOKEN1. Should return 3 rows: TOKEN1 for hours 0, 1, and 2.
#[test]
fn current_include_with_filter() {
    run_test(|env| async move {
        let mut query =
            env.aggregation_query("Stats_hour", AggregationCurrent::Include, BLOCKS[3].number);
        query.filter = Some(EntityFilter::Equal(
            "token".to_string(),
            Value::Bytes(TOKEN1.clone()),
        ));

        let result = env
            .store
            .subgraph_store()
            .find(query)
            .await
            .expect("filtered aggregation query succeeds");

        assert_eq!(
            3,
            result.len(),
            "expected 3 rows for TOKEN1 (hours 0, 1, 2), got {}",
            result.len()
        );

        // All rows should be for TOKEN1
        for entity in &result {
            assert_eq!(
                entity.get("token").unwrap(),
                &Value::Bytes(TOKEN1.clone()),
                "all rows should be for TOKEN1"
            );
        }

        // Verify we have one row per hour (timestamps 0, 3600, 7200)
        for expected_ts in [ts(0), ts(3600), ts(7200)] {
            assert!(
                result
                    .iter()
                    .any(|e| e.get("timestamp").unwrap() == &expected_ts),
                "expected a row with timestamp {:?}",
                expected_ts
            );
        }
    })
}

/// Query Stats_hour with AggregationCurrent::Include and verify cumulative
/// aggregate `totalValue` in the current bucket rows.
/// For TOKEN1: prev hour 1 totalValue=104, current hour 2 value=4*4=16,
///   so totalValue = 104 + 16 = 120
/// For TOKEN2: prev hour 1 totalValue=50, current hour 2 value=4*40=160,
///   so totalValue = 50 + 160 = 210
#[test]
fn current_include_cumulative() {
    run_test(|env| async move {
        let result = env
            .find_aggregation("Stats_hour", AggregationCurrent::Include, BLOCKS[3].number)
            .await;

        let hour2_ts = ts(7200);
        let current_bucket: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("timestamp").unwrap() == &hour2_ts)
            .collect();
        assert_eq!(
            2,
            current_bucket.len(),
            "expected 2 current-bucket rows, got {}",
            current_bucket.len()
        );

        let token1 = current_bucket
            .iter()
            .find(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN1.clone()))
            .expect("TOKEN1 current bucket exists");
        assert_eq!(
            token1.get("totalValue").unwrap(),
            &bd(120),
            "TOKEN1 cumulative totalValue = 104 + 16 = 120"
        );

        let token2 = current_bucket
            .iter()
            .find(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN2.clone()))
            .expect("TOKEN2 current bucket exists");
        assert_eq!(
            token2.get("totalValue").unwrap(),
            &bd(210),
            "TOKEN2 cumulative totalValue = 50 + 160 = 210"
        );
    })
}

/// Query Stats_hour as a nested field of Token with
/// AggregationCurrent::Include at BLOCKS[3]. Each token should have
/// 2 rolled-up rows (hour 0 and hour 1) + 1 current bucket row (hour 2).
/// Total: 3 rows per token, 6 rows overall.
#[test]
fn nested_current_include() {
    run_test(|env| async move {
        let result = env
            .find_nested_aggregation(
                AggregationCurrent::Include,
                BLOCKS[3].number,
                &[&TOKEN1, &TOKEN2],
            )
            .await;

        assert_eq!(
            6,
            result.len(),
            "expected 6 rows (3 per token for hours 0, 1, 2), got {}",
            result.len()
        );

        let hour2_ts = ts(7200);

        // Current bucket rows (hour 2, timestamp = 7200)
        let current_bucket: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("timestamp").unwrap() == &hour2_ts)
            .collect();
        assert_eq!(
            2,
            current_bucket.len(),
            "expected 2 current-bucket rows for hour 2, got {}",
            current_bucket.len()
        );

        // Verify current bucket values for TOKEN1: sum=4, max=4
        let token1_current = current_bucket
            .iter()
            .find(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN1.clone()))
            .expect("TOKEN1 current bucket row exists");
        assert_eq!(token1_current.get("sum").unwrap(), &bd(4));
        assert_eq!(token1_current.get("max").unwrap(), &bd(4));

        // Verify current bucket values for TOKEN2: sum=4, max=40
        let token2_current = current_bucket
            .iter()
            .find(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN2.clone()))
            .expect("TOKEN2 current bucket row exists");
        assert_eq!(token2_current.get("sum").unwrap(), &bd(4));
        assert_eq!(token2_current.get("max").unwrap(), &bd(40));
    })
}

/// Query Stats_hour as a nested field of Token with
/// AggregationCurrent::Exclude at BLOCKS[3]. Should return only
/// the 4 rolled-up rows (2 per token, hours 0 and 1).
#[test]
fn nested_current_exclude() {
    run_test(|env| async move {
        let result = env
            .find_nested_aggregation(
                AggregationCurrent::Exclude,
                BLOCKS[3].number,
                &[&TOKEN1, &TOKEN2],
            )
            .await;

        assert_eq!(
            4,
            result.len(),
            "expected 4 rolled-up rows (2 per token), got {}",
            result.len()
        );

        // No current bucket row should appear (no hour 2 timestamp)
        let hour2_ts = ts(7200);
        let current_bucket: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("timestamp").unwrap() == &hour2_ts)
            .collect();
        assert_eq!(
            0,
            current_bucket.len(),
            "expected no current-bucket rows, got {}",
            current_bucket.len()
        );
    })
}

/// Query at BLOCKS[3] with AggregationCurrent::Include, including TOKEN3
/// which has no Data entries at all. TOKEN1 and TOKEN2 each have 3 rows
/// (2 rolled-up + 1 current bucket). TOKEN3 should have 0 rows because
/// it has no timeseries data — no rolled-up buckets and no current bucket.
#[test]
fn nested_current_include_empty_bucket() {
    run_test(|env| async move {
        // Query with TOKEN1, TOKEN2, and TOKEN3
        let result = env
            .find_nested_aggregation(
                AggregationCurrent::Include,
                BLOCKS[3].number,
                &[&TOKEN1, &TOKEN2, &TOKEN3],
            )
            .await;

        assert_eq!(
            6,
            result.len(),
            "expected 6 rows (3 per token for TOKEN1/TOKEN2, 0 for TOKEN3), got {}",
            result.len()
        );

        // TOKEN1 and TOKEN2 each have 3 rows (2 rolled-up + 1 current)
        let token1_rows: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN1.clone()))
            .collect();
        assert_eq!(
            3,
            token1_rows.len(),
            "expected 3 rows for TOKEN1, got {}",
            token1_rows.len()
        );

        let token2_rows: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN2.clone()))
            .collect();
        assert_eq!(
            3,
            token2_rows.len(),
            "expected 3 rows for TOKEN2, got {}",
            token2_rows.len()
        );

        // TOKEN3 has no data entries, so no rows should appear
        let token3_rows: Vec<&Entity> = result
            .iter()
            .filter(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN3.clone()))
            .collect();
        assert_eq!(
            0,
            token3_rows.len(),
            "expected 0 rows for TOKEN3 (no timeseries data), got {}",
            token3_rows.len()
        );
    })
}

/// Verify result counts per parent token at BLOCKS[3] with
/// AggregationCurrent::Include. TOKEN1 should have 3 rows and
/// TOKEN2 should have 3 rows.
#[test]
fn nested_current_include_count() {
    run_test(|env| async move {
        let result = env
            .find_nested_aggregation(
                AggregationCurrent::Include,
                BLOCKS[3].number,
                &[&TOKEN1, &TOKEN2],
            )
            .await;

        // Group results by token (dimension) to count per parent
        let token1_count = result
            .iter()
            .filter(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN1.clone()))
            .count();
        let token2_count = result
            .iter()
            .filter(|e| e.get("token").unwrap() == &Value::Bytes(TOKEN2.clone()))
            .count();

        assert_eq!(
            3, token1_count,
            "TOKEN1 should have 3 rows (2 rolled-up + 1 current), got {}",
            token1_count
        );
        assert_eq!(
            3, token2_count,
            "TOKEN2 should have 3 rows (2 rolled-up + 1 current), got {}",
            token2_count
        );
    })
}
