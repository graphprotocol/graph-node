use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::fs;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use tempfile::{tempdir, TempDir};
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::RuntimeAdapterEvent;
use thegraph::components::store::StoreKey;
use thegraph::prelude::{RuntimeAdapter as RuntimeAdapterTrait, Value};
use thegraph::util::stream::StreamError;

use server;

/// Configuration for the runtime adapter.
pub struct RuntimeAdapterConfig {
    pub data_source_definition: String,
    pub runtime_source_dir: String,
    pub json_rpc_url: String,
}

/// Adapter to set up and connect to a Node.js based data source runtime.
pub struct RuntimeAdapter {
    logger: slog::Logger,
    config: RuntimeAdapterConfig,
    runtime: Handle,
    event_sink: Arc<Mutex<Option<Sender<RuntimeAdapterEvent>>>>,
    temp_dir: TempDir,
    child_process: Option<Child>,
}

impl RuntimeAdapter {
    /// Creates a new runtiem adapter for a Node.js based data source runtime.
    pub fn new(logger: &slog::Logger, runtime: Handle, config: RuntimeAdapterConfig) -> Self {
        // Create temporary directory for the runtime
        let temp_dir = tempdir()
            .expect("Failed to create temporary directory for the Node.js runtime adapter");

        // Create the runtime adapter
        let runtime_adapter = RuntimeAdapter {
            logger: logger.new(o!("component" => "RuntimeAdapter")),
            config,
            runtime,
            temp_dir,
            event_sink: Arc::new(Mutex::new(None)),
            child_process: None,
        };

        // Return the runtime adapter
        runtime_adapter
    }
}

impl Drop for RuntimeAdapter {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.child_process {
            child
                .kill()
                .expect("Failed to kill Node.js data source runtime");
        }
        self.child_process = None
    }
}

impl RuntimeAdapterTrait for RuntimeAdapter {
    fn start(&mut self) {
        info!(self.logger, "Prepare runtime");

        let temp_dir_str = self.temp_dir
            .path()
            .to_str()
            .expect("Invalid temporary directory");

        // Copy runtime sources into the temporary directory
        debug!(self.logger, "Copy runtime source files");
        for filename in ["package.json", "index.js", "db.js"].into_iter() {
            fs::copy(
                Path::new(self.config.runtime_source_dir.as_str()).join(filename),
                self.temp_dir.path().join(filename),
            ).expect(format!("Failed to copy data source runtime file: {}", filename).as_str());
        }

        // Run `npm install` in the temporary directory
        debug!(self.logger, "Install NPM dependencies");
        Command::new("npm")
            .arg("install")
            .current_dir(temp_dir_str)
            .output()
            .expect("Failed to run `npm install`");

        // Create a channel to receive JSON updates from the adapter server and
        // start an HTTP server for the runtime to send events to
        debug!(self.logger, "Start adapter server");
        let (runtime_event_sender, runtime_event_receiver) = channel(10);
        self.runtime.spawn(server::start_server(
            self.logger.clone(),
            runtime_event_sender,
            "127.0.0.1:7500",
        ));

        // Spawn the Node.js runtime process; Node needs to be installed on the machine
        // or this will fail
        debug!(self.logger, "Start the Node.js data source runtime");
        let child = Command::new("node")
            .current_dir(temp_dir_str)
            .arg("index.js")
            .arg(self.config.data_source_definition.as_str())
            .arg(temp_dir_str)
            .arg("http://127.0.0.1:7500/")
            .spawn()
            .expect("Failed to start Node.js date source runtime");

        self.child_process = Some(child);

        // Process forwrd incoming runtime events
        let receiver_logger = self.logger.clone();
        let event_sink = self.event_sink
            .lock()
            .unwrap()
            .clone()
            .expect("Node.js runtime adapter started without being connected");
        self.runtime.spawn(
            runtime_event_receiver
                .for_each(move |runtime_event| {
                    let entity_id = match runtime_event.data.get("id") {
                        Some(Value::String(ref s)) => Some(s.to_owned()),
                        _ => None,
                    };

                    // Skip the event if the entity ID is missing or not a string
                    if entity_id == None {
                        error!(receiver_logger,
                           "Field [\"data\", \"id\"] missing or invalid in runtime event";
                           "event" => format!("{:?}", runtime_event));
                        return Err(());
                    }

                    let event = match runtime_event.operation.as_str() {
                        "add" => RuntimeAdapterEvent::EntityAdded(
                            "memefactory".to_string(),
                            StoreKey {
                                entity: runtime_event.entity.to_owned(),
                                id: entity_id.unwrap(),
                            },
                            runtime_event.data,
                        ),
                        "update" => RuntimeAdapterEvent::EntityChanged(
                            "memefactory".to_string(),
                            StoreKey {
                                entity: runtime_event.entity.to_owned(),
                                id: entity_id.unwrap(),
                            },
                            runtime_event.data,
                        ),
                        "remove" => RuntimeAdapterEvent::EntityRemoved(
                            "memefactory".to_string(),
                            StoreKey {
                                entity: runtime_event.entity.to_owned(),
                                id: entity_id.unwrap(),
                            },
                        ),
                        _ => unimplemented!("Unknown runtime event operation"),
                    };

                    event_sink
                        .clone()
                        .send(event)
                        .wait()
                        .expect("Failed to forward runtime adapter event");
                    Ok(())
                })
                .and_then(|_| Ok(())),
        );
    }

    fn stop(&mut self) {
        info!(self.logger, "Stop");
    }

    fn event_stream(&mut self) -> Result<Receiver<RuntimeAdapterEvent>, StreamError> {
        info!(self.logger, "Setting up event stream");

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
