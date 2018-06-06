use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::{Bool, Float, Integer, Text};
use diesel::{delete, insert_into};
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use serde_json;
use slog;
use tokio_core::reactor::Handle;

use thegraph::components::schema::SchemaProviderEvent;
use thegraph::components::store::{Store as StoreTrait, *};
use thegraph::data::store::*;
use thegraph::util::stream::StreamError;
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
    runtime: Handle,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    _config: StoreConfig,
    pub conn: PgConnection,
}

impl Store {
    pub fn new(config: StoreConfig, logger: &slog::Logger, runtime: Handle) -> Self {
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
            runtime,
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
        self.runtime.spawn(stream.for_each(move |_| {
            // We are currently not doing anything in response to schema events
            Ok(())
        }));
    }
}

impl StoreTrait for Store {
    fn get(&self, key: StoreKey) -> Result<Entity, ()> {
        debug!(self.logger, "get"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        // The data source hardcoded at the moment
        let datasource: String = String::from("memefactory");

        // Use primary key fields to get the entity; deserialize the result JSON
        entities
            .find((key.id, datasource, key.entity))
            .select(data)
            .first::<serde_json::Value>(&self.conn)
            .map(|value| {
                serde_json::from_value::<Entity>(value).expect("Failed to deserialize entity")
            })
            .map_err(|_| ())
    }

    fn set(&mut self, key: StoreKey, input_entity: Entity) -> Result<(), ()> {
        debug!(self.logger, "set"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        // Convert Entity hashmap to serde_json::Value for insert
        let entity_json: serde_json::Value =
            serde_json::to_value(&input_entity).expect("Failed to serialize entity");

        // The data source is hardcoded at the moment
        let datasource: String = String::from("memefactory");

        // Insert entity, perform an update in case of a primary key conflict
        insert_into(entities)
            .values((
                id.eq(&key.id),
                entity.eq(&key.entity),
                data_source.eq(&datasource),
                data.eq(&entity_json),
            ))
            .on_conflict((id, entity, data_source))
            .do_update()
            .set((
                id.eq(&key.id),
                entity.eq(&key.entity),
                data_source.eq(&data_source),
                data.eq(&entity_json),
            ))
            .execute(&self.conn)
            .map(|_| ())
            .map_err(|_| ())
    }

    fn delete(&mut self, key: StoreKey) -> Result<(), ()> {
        debug!(self.logger, "delete"; "key" => format!("{:?}", key));

        use db_schema::entities::dsl::*;

        // Delete from DB where rows match the ID and entity value;
        // add data source here when meaningful
        delete(
            entities
                .filter(id.eq(&key.id))
                .filter(entity.eq(&key.entity)),
        ).execute(&self.conn)
            .map(|_| ())
            .map_err(|_| ())
    }

    fn find(&self, query: StoreQuery) -> Result<Vec<Entity>, ()> {
        use db_schema::entities::columns::*;
        use db_schema::*;

        // The data source is hard-coded at the moment
        let _datasource: String = String::from("memefactory");

        // Create base boxed query; this will be added to based on the
        // query parameters provided
        let mut diesel_query = entities::table
            .filter(entity.eq(query.entity))
            .select(data)
            .into_boxed::<Pg>();

        // Add specified filter to query
        match query.filter {
            Some(StoreFilter::And(filters)) => {
                for filter in filters {
                    match filter {
                        StoreFilter::Contains(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' LIKE ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                _ => unimplemented!(),
                            };
                        }
                        StoreFilter::Equal(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' = ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                Value::Float(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Float')::float = ")
                                            .bind::<Float, _>(query_value),
                                    );
                                }
                                Value::Int(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Int')::int = ")
                                            .bind::<Integer, _>(query_value),
                                    );
                                }
                                Value::Bool(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Bool')::boolean = ")
                                            .bind::<Bool, _>(query_value),
                                    );
                                }
                            };
                        }
                        StoreFilter::Not(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' != ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                Value::Float(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Float')::float != ")
                                            .bind::<Float, _>(query_value as f32),
                                    );
                                }
                                Value::Int(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Int')::int != ")
                                            .bind::<Integer, _>(query_value),
                                    );
                                }
                                Value::Bool(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Bool')::boolean != ")
                                            .bind::<Bool, _>(query_value),
                                    );
                                }
                            };
                        }
                        StoreFilter::GreaterThan(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' > ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                Value::Float(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Float')::float > ")
                                            .bind::<Float, _>(query_value as f32),
                                    );
                                }
                                Value::Int(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Int')::int > ")
                                            .bind::<Integer, _>(query_value),
                                    );
                                }
                                _ => unimplemented!(),
                            };
                        }
                        StoreFilter::LessThan(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' < ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                Value::Float(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Float')::float < ")
                                            .bind::<Float, _>(query_value as f32),
                                    );
                                }
                                Value::Int(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Int')::int < ")
                                            .bind::<Integer, _>(query_value),
                                    );
                                }
                                _ => unimplemented!(),
                            };
                        }
                        StoreFilter::GreaterOrEqual(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' >= ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                Value::Float(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Float')::float >= ")
                                            .bind::<Float, _>(query_value as f32),
                                    );
                                }
                                Value::Int(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Int')::int >= ")
                                            .bind::<Integer, _>(query_value),
                                    );
                                }
                                _ => unimplemented!(),
                            };
                        }
                        StoreFilter::LessThanOrEqual(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' <= ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                Value::Float(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Float')::float <= ")
                                            .bind::<Float, _>(query_value as f32),
                                    );
                                }
                                Value::Int(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("(data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'Int')::int <= ")
                                            .bind::<Integer, _>(query_value),
                                    );
                                }
                                _ => unimplemented!(),
                            };
                        }
                        StoreFilter::NotContains(attribute, value) => {
                            match value {
                                Value::String(query_value) => {
                                    diesel_query = diesel_query.filter(
                                        sql("data -> ")
                                            .bind::<Text, _>(attribute)
                                            .sql("->>'String' NOT LIKE ")
                                            .bind::<Text, _>(query_value),
                                    );
                                }
                                _ => unimplemented!(),
                            };
                        }

                        // We will add support for more filters later
                        _ => unimplemented!(),
                    };
                }
            }
            _ => unimplemented!(),
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
                sql::<Text>("data -> ")
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
