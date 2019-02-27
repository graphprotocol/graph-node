use diesel::deserialize::QueryableByName;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::sql_types::Text;
use diesel::RunQueryDsl;
use graph::prelude::*;
use graph::serde_json;
use notification_listener::{NotificationListener, SafeChannelName};

pub struct StoreEventListener {
    notification_listener: NotificationListener,
}

impl StoreEventListener {
    pub fn new(logger: &Logger, postgres_url: String) -> Self {
        StoreEventListener {
            notification_listener: NotificationListener::new(
                logger,
                postgres_url,
                SafeChannelName::i_promise_this_is_safe("store_events"),
            ),
        }
    }

    pub fn start(&mut self) {
        self.notification_listener.start()
    }
}

impl EventProducer<StoreEvent> for StoreEventListener {
    fn take_event_stream(&mut self) -> Option<StoreEventStreamBox> {
        self.notification_listener.take_event_stream().map(
            |stream| -> Box<Stream<Item = _, Error = _> + Send> {
                Box::new(stream.map(|notification| {
                    // Create StoreEvent from JSON
                    let change: StoreEvent = serde_json::from_value(notification.payload.clone())
                        .unwrap_or_else(|_| {
                            panic!(
                                "invalid store event received from database: {:?}",
                                notification.payload
                            )
                        });

                    change
                }))
            },
        )
    }
}

// Querying the entity changes corresponding to the reversion of a certain
// block for a certain subgraph. We want to keep the deserialization
// logic (QueryableByName) close to the actual query. That's why we introduce
// the intermediate EntityChangeQBN struct

struct EntityChangeQBN(EntityChange);

impl QueryableByName<Pg> for EntityChangeQBN {
    fn build<R: diesel::row::NamedRow<diesel::pg::Pg>>(
        row: &R,
    ) -> diesel::deserialize::Result<Self> {
        let subgraph_id: String = row.get("subgraph")?;
        let subgraph_id = SubgraphDeploymentId::new(subgraph_id).map_err(|_| {
            format!(
                "Illegal subgraph_id {}",
                row.get("subgraph").unwrap_or("<unknown>".to_string())
            )
        })?;
        let change: String = row.get("change")?;
        Ok(EntityChangeQBN(EntityChange {
            subgraph_id,
            entity_type: row.get("entity")?,
            entity_id: row.get("entity_id")?,
            operation: match change.as_str() {
                "set" => EntityChangeOperation::Set,
                "removed" => EntityChangeOperation::Removed,
                _ => return Err(Box::new(format_err!("bad operation {}", change).compat())),
            },
        }))
    }
}

// Get the store event corresponding to reverting the operations for
// block block_ptr_from. After reversion, we will be at block_ptr_to, which
// must be one less than block_ptr_from. The source of the event will be
// block_ptr_to as we will be at that block after the reversion
pub fn get_revert_event(
    conn: &PgConnection,
    subgraph_id: &SubgraphDeploymentId,
    block_ptr_from: &EthereumBlockPointer,
    block_ptr_to: EthereumBlockPointer,
) -> Result<StoreEvent, StoreError> {
    // Sanity check on block numbers
    assert_eq!(
        block_ptr_from.number,
        block_ptr_to.number + 1,
        "get_revert_event must revert a single block only"
    );

    // The query to find the EntityChanges that need to be emitted for the
    // reversion follows the logic of the revert_block stored procedure closely.
    // If that logic ever changes, this query will need to change, too.
    //
    // The query takes two parameters:
    //   - block_ptr_from: the block hash that we want to revert
    //   - subgraph_id: the subgraph for which we are reverting
    //
    // The query is fairly straightforward: the events_for_block_and_subgraph
    // query finds all events that affect the given subgraph at the given block,
    // and the outer query returns the data we need to construct EntityChanges
    // for those history events
    let query = "
WITH
  events_for_block_and_subgraph AS
    (SELECT distinct h.event_id
       FROM entity_history h, event_meta_data m
      WHERE h.event_id = m.id
        AND m.source = $1
        AND h.subgraph = $2)
SELECT
  h.subgraph,
  h.entity,
  h.entity_id,
  (CASE
     WHEN h.op_id = 0 THEN 'removed'
     WHEN h.op_id in (1,2) THEN 'set'
   END) as change
FROM entity_history h
WHERE h.event_id in (select event_id from events_for_block_and_subgraph)
ORDER BY h.event_id desc";
    let query = diesel::sql_query(query)
        .bind::<Text, _>(block_ptr_from.hash_hex())
        .bind::<Text, _>(subgraph_id.to_string());
    let changes: Vec<EntityChangeQBN> = query.get_results(conn)?;
    let changes = changes.into_iter().map(|qbn| qbn.0).collect();
    let source = EventSource::EthereumBlock(block_ptr_to);

    Ok(StoreEvent { source, changes })
}
