use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::RuntimeAdapterEvent;
use thegraph::components::ethereum::*;
use thegraph::prelude::RuntimeAdapter as RuntimeAdapterTrait;
use thegraph::util::stream::StreamError;

use interpreter;

pub struct RuntimeAdapterConfig {
    pub data_source_definition: String,
}

pub struct RuntimeAdapter<T>
where
    T: EthereumWatcher,
{
    _config: RuntimeAdapterConfig,
    _runtime: Handle,
    logger: slog::Logger,
    event_sink: Arc<Mutex<Option<Sender<RuntimeAdapterEvent>>>>,
    ethereum_watcher: Arc<Mutex<T>>,
}

impl<T> RuntimeAdapter<T>
where
    T: EthereumWatcher,
{
    pub fn new(
        logger: &slog::Logger,
        runtime: Handle,
        ethereum_watcher: Arc<Mutex<T>>,
        config: RuntimeAdapterConfig,
    ) -> Self {
        let (sender, _receiver) = channel(100);
        RuntimeAdapter {
            _config: config,
            _runtime: runtime,
            logger: logger.new(o!("component" => "RuntimeAdapter")),
            event_sink: Arc::new(Mutex::new(Some(sender))),
            ethereum_watcher,
        }
    }
}

impl<T> RuntimeAdapterTrait for RuntimeAdapter<T>
where
    T: EthereumWatcher,
{
    fn start(&mut self) {
        info!(self.logger, "Start");

        // Get location of wasm file

        let event_sink = self.event_sink
            .lock()
            .unwrap()
            .clone()
            .expect("Runtime started without event sink");

        // Instantiate Wasmi module
        // TODO: Link this with the wasm runtime compiler output: wasm_location
        debug!(self.logger, "Instantiate wasm module from file");
        let _wasmi_module = interpreter::WasmiModule::new("/test/add_fn.wasm", event_sink);
    }
    fn stop(&mut self) {
        info!(self.logger, "Stop");
    }
    fn event_stream(&mut self) -> Result<Receiver<RuntimeAdapterEvent>, StreamError> {
        // If possible, create a new channel for streaming runtime adapter events
        let mut event_sink = self.event_sink.lock().unwrap();
        match *event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                *event_sink = Some(sink);
                Ok(stream)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::prelude::*;
    use futures::sync::mpsc::channel;
    use interpreter;
    use thegraph::components::data_sources::RuntimeAdapterEvent;
    use thegraph::components::store as StoreComponents;
    use thegraph::data::store as StoreData;

    #[test]
    fn exported_function_create_entity_method_emits_an_entity_added_event() {
        let (sender, receiver) = channel(10);

        // Build event data to send: datasource, StoreKey, Entity
        let datasource = "memefactory".to_string();
        let key = StoreComponents::StoreKey {
            entity: "test_type".to_string(),
            id: 1.to_string(),
        };
        let mut entity = StoreData::Entity::new();
        entity.insert(
            "Name".to_string(),
            StoreData::Value::String("John".to_string()),
        );

        // Create EntityAdded event and send to channel
        interpreter::Db::create_entity(sender.clone(), datasource, key, entity);

        // Consume receiver
        let result = receiver
            .into_future()
            .wait()
            .unwrap()
            .0
            .expect("No event found in receiver");

        // Confirm receiver contains EntityAdded event with correct datasource and StoreKey
        match result {
            RuntimeAdapterEvent::EntityAdded(rec_datasource, rec_key, _rec_entity) => {
                assert_eq!("memefactory".to_string(), rec_datasource);
                assert_eq!(
                    StoreComponents::StoreKey {
                        entity: "test_type".to_string(),
                        id: 1.to_string(),
                    },
                    rec_key
                );
            }
            _ => panic!("EntityAdded event not received, other type found"),
        }
    }
}
