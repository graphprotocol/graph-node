use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::fs;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use tempdir::TempDir;
use tokio_core::reactor::Handle;

use thegraph::components::data_sources::RuntimeAdapterEvent;
use thegraph::components::store::StoreKey;
use thegraph::prelude::{*, RuntimeAdapter as RuntimeAdapterTrait};
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
    temp_dir: Option<TempDir>,
    child_process: Option<Child>,
}

impl RuntimeAdapter {
    /// Creates a new runtiem adapter for a Node.js based data source runtime.
    pub fn new(logger: &slog::Logger, runtime: Handle, config: RuntimeAdapterConfig) -> Self {
        // Create the runtime adapter
        let runtime_adapter = RuntimeAdapter {
            logger: logger.new(o!("component" => "RuntimeAdapter")),
            config,
            runtime,
            event_sink: Arc::new(Mutex::new(None)),
            temp_dir: None,
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

        // Create a temporary directory
        self.temp_dir = Some(
            TempDir::new("nodejs-runtime")
                .expect("Failed to create temporary directory for the Node.js runtime"),
        );

        // Get the temporary path
        let temp_dir = self.temp_dir.as_ref().unwrap();

        debug!(self.logger, "Copy runtime source files");

        // Copy runtime sources into the temporary directory
        for filename in ["package.json", "index.js", "db.js"].into_iter() {
            fs::copy(
                Path::new(self.config.runtime_source_dir.as_str()).join(filename),
                temp_dir.path().join(filename),
            ).expect(format!("Failed to copy data source runtime file: {}", filename).as_str());
        }

        debug!(self.logger, "Inject data source files into the runtime");

        // Write the data source definition to the temporary directory
        let data_source_definition_path = temp_dir.path().join("data-source-definition.json");
        fs::write(
            data_source_definition_path.clone(),
            self.config.data_source_definition.as_str(),
        ).expect("Failed to write mapping.js");

        // TODO Write all other files (mapping.js, ABIs etc.) to the temporary directory

        debug!(self.logger, "Run `npm install`");

        // Run `npm install` in the temporary directory
        Command::new("npm")
            .arg("install")
            .current_dir(temp_dir.path().clone())
            .output()
            .expect("Failed to run `npm install`");

        debug!(self.logger, "Start the Node.js data source runtime");

        // Spawn the Node.js runtime process; Node needs to be installed on the machine
        // or this will fail
        let mut child = Command::new("node")
            .current_dir(temp_dir.path().clone())
            .arg("index.js")
            .arg(data_source_definition_path.as_path())
            .arg(temp_dir.path().clone())
            .arg(self.config.json_rpc_url.as_str())
            .spawn()
            .expect("Failed to start Node.js date source runtime");

        debug!(self.logger, "Start adapter server");

        // Start an HTTP server for the runtime to send events to
        server::start_server(self.config.json_rpc_url.as_str());

        // Process forwrd incoming data source events
        let event_sink = self.event_sink.clone();
        self.runtime.spawn(future::lazy(move || {
            let event_sink = event_sink
                .lock()
                .unwrap()
                .clone()
                .expect("Node.js runtime adapter started without being connected");

            event_sink
                .clone()
                .send(RuntimeAdapterEvent::EntityAdded(
                    "memefactory".to_string(),
                    StoreKey {
                        entity: "user".to_string(),
                        id: "1".to_string(),
                    },
                    Entity::new(),
                ))
                .wait()
                .unwrap();

            Ok(())
        }));
    }

    fn stop(&mut self) {}

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
