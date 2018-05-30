#[macro_use]
extern crate slog;
extern crate thegraph;

use std::process::Command;
mod server;

/// Starts a runtime for running mappings scripts and writing
/// results to the store.
pub struct RuntimeAdapter {
    logger: slog::Logger,
}

impl RuntimeAdapter {
    pub fn new(
        _data_source_definition: &str,
        _node_runtime_path: &str,
        _json_rpc_url: &str,
        _logger: &slog::Logger,
    ) -> Self {
        // Configure the logger
        let logger = _logger.new(o!("component" => "RuntimeAdapter"));
        info!(logger, "Starting runtime");

        // Check if index.js exists on the runtime path
        let index_file_name = "index.js";
        let index_file_path = [_node_runtime_path.clone(), index_file_name].join("/");
        // TODO: Check if the file exists

        // Spawn the child node process
        // Node needs to be installed on the machine or it will fail.
        let mut child = Command::new("node")
            .arg(index_file_path)
            .arg(_data_source_definition)
            .arg(_node_runtime_path)
            .arg(_json_rpc_url)
            .spawn()
            .expect("failed to execute child");

        // Create the runtime adapter
        let runtime_adapter = RuntimeAdapter { logger: logger };

        // Open an HTTP server for DB commands
        server::start_server();

        // Return the runtime adapter
        runtime_adapter
    }

    pub fn stop() -> () {
        // TODO: Kill child node process

        // TODO: Stop HTTP server
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use thegraph::util::log::logger;
    use RuntimeAdapter;
    #[test]
    fn it_starts_the_runtime() {
        let logger = logger();
        let runtime_adapter =
            RuntimeAdapter::new("../mappings.yaml", "./src", "127.0.0.1:7545", &logger);
    }
}
