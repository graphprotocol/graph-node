use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel::{debug_query, delete, insert_into, result, select};
use filter::store_filter;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use graph::components::store::{EventSource, Store as StoreTrait};
use graph::prelude::*;
use graph::serde_json;

use entity_changes::EntityChangeListener;
use functions::{revert_block, set_config};

embed_migrations!("./migrations");

/// Internal representation of a Store subscription.
struct Subscription {
    pub subgraph: String,
    pub entities: Vec<String>,
    pub sender: Sender<EntityChange>,
}

/// Run all initial schema migrations.
///
/// Creates the "entities" table if it doesn't already exist.
fn initiate_schema(logger: &slog::Logger, conn: &PgConnection) {
    // Collect migration logging output
    let mut output = vec![];

    match embedded_migrations::run_with_output(conn, &mut output) {
        Ok(_) => info!(logger, "Completed pending Postgres schema migrations"),
        Err(e) => panic!("Error with Postgres schema setup: {:?}", e),
    }

    // If there was any migration output, log it now
    if !output.is_empty() {
        debug!(logger, "Postgres migration output";
               "output" => String::from_utf8(output).unwrap_or(String::from("<unreadable>")));
    }
}

/// Configuration for the Diesel/Postgres store.
pub struct StoreConfig {
    pub url: String,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    logger: slog::Logger,
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    pub conn: PgConnection,
}

impl Store {
    pub fn new(config: StoreConfig, logger: &slog::Logger) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Connect to Postgres
        let conn =
            PgConnection::establish(config.url.as_str()).expect("failed to connect to Postgres");

        info!(logger, "Connected to Postgres"; "url" => &config.url);

        // Create the entities table (if necessary)
        initiate_schema(&logger, &conn);

        // Create the store
        let mut store = Store {
            logger: logger.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            conn: conn,
        };

        // Listen to entity changes in Postgres
        let mut listener = EntityChangeListener::new(logger, config.url.clone());
        let entity_changes = listener
            .take_event_stream()
            .expect("Failed to listen to entity change events in Postgres");

        // Handle entity changes as they come in
        store.handle_entity_changes(entity_changes);

        // Return the store
        store
    }

    /// Handles entity changes emitted by Postgres.
    fn handle_entity_changes(
        &mut self,
        entity_changes: Box<Stream<Item = EntityChange, Error = ()> + Send>,
    ) {
        let logger = self.logger.clone();
        let subscriptions = self.subscriptions.clone();

        tokio::spawn(entity_changes.for_each(move |change| {
            debug!(logger, "Entity change";
                   "subgraph" => &change.subgraph,
                   "entity" => &change.entity,
                   "id" => &change.id);

            // Obtain IDs and senders of subscriptions matching the entity change
            let matches = subscriptions
                .read()
                .unwrap()
                .iter()
                .filter(|(_, subscription)| {
                    subscription.subgraph == change.subgraph
                        && subscription.entities.contains(&change.entity)
                })
                .map(|(id, subscription)| (id.clone(), subscription.sender.clone()))
                .collect::<Vec<_>>();

            let subscriptions = subscriptions.clone();

            // Write change to all matching subscription streams; remove subscriptions
            // whose receiving end has been dropped
            stream::iter_ok::<_, ()>(matches).for_each(move |(id, sender)| {
                let subscriptions = subscriptions.clone();
                sender
                    .send(change.clone())
                    .map_err(move |_| {
                        subscriptions.write().unwrap().remove(&id);
                    })
                    .and_then(|_| Ok(()))
            })
        }));
    }

    /// Handles block reorganizations.
    /// Revert all store events related to the given block
    pub fn revert_events(&self, block_hash: String) {
        select(revert_block(block_hash))
            .execute(&self.conn)
            .unwrap();
    }
}

impl BasicStore for Store {
    fn get(&self, key: StoreKey) -> Result<Entity, ()> {
        debug!(self.logger, "get"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        // Use primary key fields to get the entity; deserialize the result JSON
        entities
            .find((key.id, key.subgraph, key.entity))
            .select(data)
            .first::<serde_json::Value>(&self.conn)
            .map(|value| {
                serde_json::from_value::<Entity>(value).expect("Failed to deserialize entity")
            })
            .map_err(|_| ())
    }

    fn set(
        &mut self,
        key: StoreKey,
        input_entity: Entity,
        input_event_source: EventSource,
    ) -> Result<(), ()> {
        debug!(self.logger, "set"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        // Update the existing entity, if necessary
        let updated_entity = match self.get(key.clone()) {
            Ok(mut existing_entity) => {
                existing_entity.merge(input_entity);
                existing_entity
            }
            Err(_) => input_entity,
        };

        // Convert Entity hashmap to serde_json::Value for insert
        let entity_json: serde_json::Value =
            serde_json::to_value(&updated_entity).expect("Failed to serialize entity");

        // Insert entity, perform an update in case of a primary key conflict
        insert_into(entities)
            .values((
                id.eq(&key.id),
                entity.eq(&key.entity),
                subgraph.eq(&key.subgraph),
                data.eq(&entity_json),
                event_source.eq(&input_event_source.to_string()),
            ))
            .on_conflict((id, entity, subgraph))
            .do_update()
            .set((
                id.eq(&key.id),
                entity.eq(&key.entity),
                subgraph.eq(&key.subgraph),
                data.eq(&entity_json),
                event_source.eq(&input_event_source.to_string()),
            ))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|_| ())
    }

    fn delete(&mut self, key: StoreKey, input_event_source: EventSource) -> Result<(), ()> {
        debug!(self.logger, "delete"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        self.conn
            .transaction::<usize, result::Error, _>(|| {
                // Set session variable to store the source of the event
                select(set_config(
                    "vars.current_event_source",
                    input_event_source.to_string(),
                    false,
                )).execute(&self.conn)
                    .unwrap();

                // Delete from DB where rows match the subgraph ID, entity name and ID
                delete(
                    entities
                        .filter(subgraph.eq(&key.subgraph))
                        .filter(entity.eq(&key.entity))
                        .filter(id.eq(&key.id)),
                ).execute(&self.conn)
            })
            .map(|_| ())
            .map_err(|_| ())
    }

    fn find(&self, query: StoreQuery) -> Result<Vec<Entity>, ()> {
        use db_schema::entities::dsl::*;

        // Create base boxed query; this will be added to based on the
        // query parameters provided
        let mut diesel_query = entities
            .filter(entity.eq(query.entity))
            .filter(subgraph.eq(query.subgraph))
            .select(data)
            .into_boxed::<Pg>();

        // Add specified filter to query
        if let Some(filter) = query.filter {
            diesel_query = store_filter(diesel_query, filter).map_err(|e| {
                error!(self.logger, "value does not support this filter";
                                    "value" => format!("{:?}", e.value),
                                    "filter" => e.filter)
            })?;
        }

        // Add order by filters to query
        if let Some(order_attribute) = query.order_by {
            let direction = query
                .order_direction
                .map(|direction| match direction {
                    StoreOrder::Ascending => String::from("ASC"),
                    StoreOrder::Descending => String::from("DESC"),
                })
                .unwrap_or(String::from("ASC"));

            diesel_query = diesel_query.order(
                sql::<Text>("data ->> ")
                    .bind::<Text, _>(order_attribute)
                    .sql(&format!(" {} ", direction)),
            )
        }

        // Add range filter to query
        if let Some(range) = query.range {
            diesel_query = diesel_query
                .limit(range.first as i64)
                .offset(range.skip as i64);
        }

        debug!(self.logger, "find";
                "sql" => format!("{:?}", debug_query::<Pg, _>(&diesel_query)));

        // Process results; deserialize JSON data
        diesel_query
            .load::<serde_json::Value>(&self.conn)
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| {
                        serde_json::from_value::<Entity>(value)
                            .expect("Error to deserialize entity")
                    })
                    .collect()
            })
            .map_err(|_| ())
    }
}

impl StoreTrait for Store {
    fn subscribe(
        &mut self,
        subgraph: String,
        entities: Vec<String>,
    ) -> Box<Stream<Item = EntityChange, Error = ()> + Send> {
        // Prepare the new subscription by creating a channel and a subscription object
        let (sender, receiver) = channel(100);
        let id = Uuid::new_v4().to_string();
        let subscription = Subscription {
            subgraph,
            entities,
            sender,
        };

        // Add the new subscription (log an error if somehow the UUID is not unique)
        let subscriptions = self.subscriptions.clone();
        let mut subscriptions = subscriptions.write().unwrap();
        if subscriptions.contains_key(&id) {
            error!(self.logger, "Duplicate Store subscription ID generated; \
                                 subscriptions will not work as expected";
                   "id" => &id);
        }
        subscriptions.insert(id.clone(), subscription);

        // Return the subscription ID and entity change stream
        Box::new(receiver)
    }
}
