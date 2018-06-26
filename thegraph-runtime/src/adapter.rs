use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::RuntimeAdapterEvent;
use thegraph::components::ethereum::*;
use thegraph::prelude::RuntimeAdapter as RuntimeAdapterTrait;
use thegraph::util::stream::StreamError;

pub struct RuntimeAdapterConfig {
    pub data_source_definition: String,
}

pub struct RuntimeAdapter<T>
where
    T: EthereumAdapter,
{
    _config: RuntimeAdapterConfig,
    _runtime: Handle,
    logger: slog::Logger,
    event_sink: Arc<Mutex<Option<Sender<RuntimeAdapterEvent>>>>,
    ethereum_watcher: T,
}

impl<T> RuntimeAdapter<T>
where
    T: EthereumAdapter,
{
    pub fn new(
        logger: &slog::Logger,
        runtime: Handle,
        ethereum_watcher: T,
        config: RuntimeAdapterConfig,
    ) -> Self {
        RuntimeAdapter {
            _config: config,
            _runtime: runtime,
            logger: logger.new(o!("component" => "RuntimeAdapter")),
            event_sink: Arc::new(Mutex::new(None)),
            ethereum_watcher,
        }
    }
}

impl<T> RuntimeAdapterTrait for RuntimeAdapter<T>
where
    T: EthereumAdapter,
{
    fn start(&mut self) {
        info!(self.logger, "Start");
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
