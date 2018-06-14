use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::RuntimeAdapterEvent;
use thegraph::components::ethereum::*;
use thegraph::prelude::RuntimeAdapter as RuntimeAdapterTrait;
use thegraph::util::stream::StreamError;

pub struct RuntimeAdapterConfig<S, U, C> {
    pub data_source_definition: String,
    pub on_subscribe_to_event: S,
    pub on_unsubscribe_from_event: U,
    pub on_contract_state: C,
}

pub struct RuntimeAdapter<S, U, C>
where
    S: Fn(EthereumEventSubscription) -> Receiver<EthereumEvent>,
    U: Fn(String) -> bool,
    C: Fn(EthereumContractStateRequest)
        -> Box<Future<Item = EthereumContractState, Error = EthereumContractStateError>>,
{
    _config: RuntimeAdapterConfig<S, U, C>,
    logger: slog::Logger,
    event_sink: Arc<Mutex<Option<Sender<RuntimeAdapterEvent>>>>,
}

impl<S, U, C> RuntimeAdapter<S, U, C>
where
    S: Fn(EthereumEventSubscription) -> Receiver<EthereumEvent>,
    U: Fn(String) -> bool,
    C: Fn(EthereumContractStateRequest)
        -> Box<Future<Item = EthereumContractState, Error = EthereumContractStateError>>,
{
    pub fn new(
        logger: &slog::Logger,
        _runtime: Handle,
        config: RuntimeAdapterConfig<S, U, C>,
    ) -> Self {
        RuntimeAdapter {
            _config: config,
            logger: logger.new(o!("component" => "RuntimeAdapter")),
            event_sink: Arc::new(Mutex::new(None)),
        }
    }
}

impl<S, U, C> RuntimeAdapterTrait for RuntimeAdapter<S, U, C>
where
    S: Fn(EthereumEventSubscription) -> Receiver<EthereumEvent>,
    U: Fn(String) -> bool,
    C: Fn(EthereumContractStateRequest)
        -> Box<Future<Item = EthereumContractState, Error = EthereumContractStateError>>,
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
