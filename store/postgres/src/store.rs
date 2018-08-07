use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::Text;
use diesel::{delete, insert_into, result, select};
use filter::store_filter;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graph::tokio;
use serde_json;
use slog;

use functions::{revert_block, set_config};
use graph::components::schema::SchemaProviderEvent;
use graph::components::store::{Store as StoreTrait, *};
use graph::data::store::*;
use graph::util::stream::StreamError;

embed_migrations!("./migrations");

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
    event_sink: Option<Sender<StoreEvent>>,
    logger: slog::Logger,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    _config: StoreConfig,
    pub conn: PgConnection,
}

impl Store {
    pub fn new(config: StoreConfig, logger: &slog::Logger) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Create a channel for handling incoming schema provider events
        let (sink, stream) = channel(100);

        // Connect to Postgres
        let conn =
            PgConnection::establish(config.url.as_str()).expect("Failed to connect to Postgres");

        info!(logger, "Connected to Postgres"; "url" => &config.url);

        // Create the entities table (if necessary)
        initiate_schema(&logger, &conn);

        // Create the store
        let mut store = Store {
            logger,
            event_sink: None,
            schema_provider_event_sink: sink,
            _config: config,
            conn: conn,
        };

        // Spawn a task that handles incoming schema provider events
        store.handle_schema_provider_events(stream);

        // Return the store
        store
    }

    /// Handles incoming schema provider events.
    fn handle_schema_provider_events(&mut self, stream: Receiver<SchemaProviderEvent>) {
        tokio::spawn(stream.for_each(move |_| {
            // We are currently not doing anything in response to schema events
            Ok(())
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
    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent> {
        self.schema_provider_event_sink.clone()
    }

    fn event_stream(&mut self) -> Result<Receiver<StoreEvent>, StreamError> {
        // If possible, create a new channel for streaming store events
        match self.event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.event_sink = Some(sink);
                Ok(stream)
            }
        }
    }
}
