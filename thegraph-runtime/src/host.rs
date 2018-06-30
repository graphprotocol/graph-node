use ethereum_types::Address;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};
use slog::Logger;
use std::fs;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;
use uuid::Uuid;

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::ethereum::*;
use thegraph::data::data_sources::DataSet;
use thegraph::prelude::{RuntimeHost as RuntimeHostTrait,
                        RuntimeHostBuilder as RuntimeHostBuilderTrait, *};
use thegraph::util;

use module::{WasmiModule, WasmiModuleConfig};

pub struct RuntimeHostConfig {
    pub data_source_definition: DataSourceDefinition,
}

pub struct RuntimeHostBuilder<T>
where
    T: EthereumAdapter,
{
    logger: Logger,
    runtime: Handle,
    ethereum_adapter: Arc<Mutex<T>>,
}

impl<T> RuntimeHostBuilder<T>
where
    T: EthereumAdapter,
{
    pub fn new(logger: &Logger, runtime: Handle, ethereum_adapter: Arc<Mutex<T>>) -> Self {
        RuntimeHostBuilder {
            logger: logger.new(o!("component" => "RuntimeHostBuilder")),
            runtime,
            ethereum_adapter,
        }
    }
}

impl<T> RuntimeHostBuilderTrait for RuntimeHostBuilder<T>
where
    T: EthereumAdapter,
{
    type Host = RuntimeHost;

    fn build(&mut self, data_source_definition: DataSourceDefinition) -> Self::Host {
        RuntimeHost::new(
            &self.logger,
            self.runtime.clone(),
            self.ethereum_adapter.clone(),
            RuntimeHostConfig {
                data_source_definition,
            },
        )
    }
}

pub struct RuntimeHost {
    config: RuntimeHostConfig,
    output: Option<Receiver<RuntimeHostEvent>>,
}

impl RuntimeHost {
    pub fn new<T>(
        logger: &Logger,
        runtime: Handle,
        ethereum_adapter: Arc<Mutex<T>>,
        config: RuntimeHostConfig,
    ) -> Self
    where
        T: EthereumAdapter,
    {
        let logger = logger.new(o!("component" => "RuntimeHost"));

        // Create channel for sending runtime host events
        let (event_sender, event_receiver) = channel(100);

        // Obtain mapping location
        let location = config.data_source_definition.resolve_path(
            &config
                .data_source_definition
                .datasets
                .first()
                .unwrap()
                .mapping
                .source
                .path,
        );

        info!(logger, "Load WASM runtime from"; "file" => location.to_str());

        // Load the mappings as a WASM module
        let module = WasmiModule::new(
            location,
            &logger,
            WasmiModuleConfig {
                data_source_id: String::from("TODO: DATA SOURCE ID"),
                runtime: runtime.clone(),
                event_sink: event_sender,
                ethereum_adapter: ethereum_adapter.clone(),
            },
        );

        Self::subscribe_to_events(
            logger,
            runtime,
            config.data_source_definition.clone(),
            module,
            ethereum_adapter,
        );

        RuntimeHost {
            config: config,
            output: Some(event_receiver),
        }
    }

    /// Subscribe to all smart contract events of the first data set
    /// in the data source definition.
    ///
    /// NOTE: We'll add support for multiple datasets soon.
    fn subscribe_to_events<T>(
        logger: Logger,
        runtime: Handle,
        data_source: DataSourceDefinition,
        module: WasmiModule,
        ethereum_adapter: Arc<Mutex<T>>,
    ) where
        T: EthereumAdapter,
    {
        info!(logger, "Subscribe to events");

        let dataset = data_source
            .datasets
            .first()
            .expect("Data source must contain at least one data set")
            .clone();

        // Obtain the contract address of the first data set
        let address = Address::from_str(dataset.data.address.as_str())
            .expect("Failed to parse contract address");

        // Load the main dataset contract
        let contract_abi_path = dataset
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == dataset.data.structure.abi)
            .map(|entry| data_source.resolve_path(&entry.source.path))
            .expect("No ABI entry found for the main contract of the dataset");
        let contract_abi_file = fs::File::open(contract_abi_path)
            .expect("Contract ABI file does not exist or could not be opend for reading");
        let contract = Contract::load(contract_abi_file).expect("Failed to parse contract ABI");

        // Prepare subscriptions for the events
        let subscription_results: Vec<EthereumEventSubscription> = {
            // Prepare subscriptions for all events
            dataset
                .mapping
                .event_handlers
                .iter()
                .map(|event_handler| {
                    let subscription_id = Uuid::new_v4().simple().to_string();
                    let event = util::ethereum::contract_event_with_signature(
                        &contract,
                        event_handler.event.as_str(),
                    ).expect(
                        format!("Event not found in contract: {}", event_handler.event).as_str(),
                    );

                    EthereumEventSubscription {
                        address,
                        event: event.clone(),
                        range: BlockNumberRange {
                            from: BlockNumber::Number(0),
                            to: BlockNumber::Latest,
                        },
                        subscription_id: subscription_id.clone(),
                    }
                })
                .collect()
        };

        // Protect the module for concurrent access
        let protected_module = Arc::new(Mutex::new(module));
        let protected_dataset = Arc::new(dataset);

        // Subscribe to the events now
        for subscription in subscription_results.into_iter() {
            Self::subscribe_to_event(
                logger.clone(),
                runtime.clone(),
                protected_module.clone(),
                ethereum_adapter.clone(),
                protected_dataset.clone(),
                subscription,
            );
        }
    }

    fn subscribe_to_event<T>(
        logger: Logger,
        runtime: Handle,
        module: Arc<Mutex<WasmiModule>>,
        ethereum_adapter: Arc<Mutex<T>>,
        dataset: Arc<DataSet>,
        subscription: EthereumEventSubscription,
    ) where
        T: EthereumAdapter,
    {
        info!(logger, "Subscribe to event";
              "name" => format!("{:#?}", subscription.event.name));

        let receiver = ethereum_adapter
            .lock()
            .unwrap()
            .subscribe_to_event(subscription);

        let error_logger = logger.clone();
        runtime.spawn(
            receiver
                .for_each(move |event| {
                    let event_handler = dataset
                        .mapping
                        .event_handlers
                        .iter()
                        .find(|event_handler| {
                            util::ethereum::string_to_h256(event_handler.event.as_str())
                                == event.event_signature
                        })
                        .expect("Received an Ethereum event not mentioned in the data set");

                    module
                        .lock()
                        .unwrap()
                        .handle_ethereum_event(event_handler.handler.as_str(), event);
                    Ok(())
                })
                .map_err(move |e| error!(error_logger, "Event subscription failed: {}", e)),
        );
    }
}

impl EventProducer<RuntimeHostEvent> for RuntimeHost {
    type EventStream = Receiver<RuntimeHostEvent>;

    fn take_event_stream(&mut self) -> Option<Self::EventStream> {
        self.output.take()
    }
}

impl RuntimeHostTrait for RuntimeHost {
    fn data_source_definition(&self) -> &DataSourceDefinition {
        &self.config.data_source_definition
    }
}
