#[macro_use]
extern crate slog;
extern crate thegraph;

use std::process::Command;

/// A Store based on Diesel and Postgres.
pub struct RuntimeAdapter {
    data_source_definition: String,
    node_runtime_path: String,
    logger: slog::Logger,
}

impl RuntimeAdapter {
    pub fn new(
        _data_source_definition: String,
        _node_runtime_path: String,
        _logger: &slog::Logger,
    ) -> Self {
        // Configure the logger
        let logger = _logger.new(o!("component" => "RuntimeAdapter"));
        info!(logger, "Starting runtime");
        // Spawn the child node process
        let mut child = Command::new("node")
            .arg("./src/test.js")
            .arg("Param1")
            .spawn()
            .expect("failed to execute child");

        // Create the runtime adapter
        let runtime_adapter = RuntimeAdapter {
            logger: logger,
            data_source_definition: _data_source_definition,
            node_runtime_path: _node_runtime_path,
        };

        // Open an HTTP server for DB commands

        // Return the runtime adapter
        runtime_adapter
    }

    pub fn stop() -> () {
        // Kill child node process

        // Stop HTTP server
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use thegraph::util::log::logger;
    use RuntimeAdapter;
    #[test]
    fn it_works() {
        let logger = logger();
        let runtime_adapter = RuntimeAdapter::new("".to_string(), "".to_string(), &logger);
    }
}
