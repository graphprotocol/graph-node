use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::ethereum::*;
use thegraph::prelude::RuntimeHost as RuntimeHostTrait;
use thegraph::util::stream::StreamError;

use interpreter;

pub struct RuntimeHostConfig {
    pub data_source_definition: String,
}

pub struct RuntimeHost<T>
where
    T: EthereumWatcher,
{
    _config: RuntimeHostConfig,
    _runtime: Handle,
    logger: slog::Logger,
    event_sink: Arc<Mutex<Option<Sender<RuntimeHostEvent>>>>,
    ethereum_watcher: Arc<Mutex<T>>,
}

impl<T> RuntimeHost<T>
where
    T: EthereumWatcher,
{
    pub fn new(
        logger: &slog::Logger,
        runtime: Handle,
        ethereum_watcher: Arc<Mutex<T>>,
        config: RuntimeHostConfig,
    ) -> Self {
        let (sender, _receiver) = channel(100);
        RuntimeHost {
            _config: config,
            _runtime: runtime,
            logger: logger.new(o!("component" => "RuntimeHost")),
            event_sink: Arc::new(Mutex::new(Some(sender))),
            ethereum_watcher,
        }
    }
}

impl<T> RuntimeHostTrait for RuntimeHost<T>
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
        info!(self.logger, "Instantiate wasm module from file");
        let _wasmi_module = interpreter::WasmiModule::new("/test/add_fn.wasm", event_sink);
    }

    fn stop(&mut self) {
        info!(self.logger, "Stop");
    }

    fn event_stream(&mut self) -> Result<Receiver<RuntimeHostEvent>, StreamError> {
        // If possible, create a new channel for streaming runtime host events
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
