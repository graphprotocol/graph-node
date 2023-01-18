use std::collections::HashMap;
use std::ffi::OsStr;
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::AtomicU16;

use anyhow::Context;

const POSTGRESQL_DEFAULT_PORT: u16 = 5432;
const GANACHE_DEFAULT_PORT: u16 = 8545;
const IPFS_DEFAULT_PORT: u16 = 5001;

/// Maps `Service => Host` exposed ports.
pub type MappedPorts = HashMap<u16, u16>;

/// Strip parent directories from filenames
pub fn basename(path: &impl AsRef<Path>) -> String {
    path.as_ref()
        .file_name()
        .map(OsStr::to_string_lossy)
        .map(String::from)
        .expect("failed to infer basename for path.")
}

/// Parses stdout bytes into a prefixed String
pub fn pretty_output(blob: &[u8], prefix: &str) -> String {
    blob.split(|b| *b == b'\n')
        .map(String::from_utf8_lossy)
        .map(|line| format!("{}{}", prefix, line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Finds and returns a free port. Ports are never *guaranteed* to be free because of
/// [TOCTOU](https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use) race
/// conditions.
///
/// This function guards against conflicts coming from other callers, so you
/// will only get port conflicts from external resources.
fn get_free_port() -> u16 {
    // We start cycling through ports at 10000, which is high enough in the port
    // space to to cause few conflicts.
    const RANGE_START: u16 = 10_000;
    static COUNTER: AtomicU16 = AtomicU16::new(RANGE_START);

    loop {
        let ordering = std::sync::atomic::Ordering::SeqCst;
        let port = COUNTER.fetch_add(1, ordering);
        if port < RANGE_START {
            // We've wrapped around, start over.
            COUNTER.store(RANGE_START, ordering);
            continue;
        }

        let bind = TcpListener::bind(("127.0.0.1", port));
        if bind.is_ok() {
            return port;
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct GraphNodePorts {
    pub http: u16,
    pub index: u16,
    pub ws: u16,
    pub admin: u16,
    pub metrics: u16,
}

impl GraphNodePorts {
    /// Populates all values with random free ports.
    pub fn random_free() -> GraphNodePorts {
        Self {
            http: get_free_port(),
            index: get_free_port(),
            ws: get_free_port(),
            admin: get_free_port(),
            metrics: get_free_port(),
        }
    }
}

// Build a postgres connection string
pub fn make_postgres_uri(db_name: &str, postgres_ports: &MappedPorts) -> String {
    let port = postgres_ports
        .get(&POSTGRESQL_DEFAULT_PORT)
        .expect("failed to fetch Postgres port from mapped ports");
    format!(
        "postgresql://{user}:{password}@{host}:{port}/{database_name}",
        user = "postgres",
        password = "password",
        host = "localhost",
        port = port,
        database_name = db_name,
    )
}

pub fn make_ipfs_uri(ipfs_ports: &MappedPorts) -> String {
    let port = ipfs_ports
        .get(&IPFS_DEFAULT_PORT)
        .expect("failed to fetch IPFS port from mapped ports");
    format!("http://{host}:{port}", host = "localhost", port = port)
}

// Build a Ganache connection string. Returns the port number and the URI.
pub fn make_ganache_uri(ganache_ports: &MappedPorts) -> (u16, String) {
    let port = ganache_ports
        .get(&GANACHE_DEFAULT_PORT)
        .expect("failed to fetch Ganache port from mapped ports");
    let uri = format!("test:http://{host}:{port}", host = "localhost", port = port);
    (*port, uri)
}

pub fn contains_subslice<T: PartialEq>(data: &[T], needle: &[T]) -> bool {
    data.windows(needle.len()).any(|w| w == needle)
}

/// Returns captured stdout
pub fn run_cmd(command: &mut Command) -> String {
    let program = command.get_program().to_str().unwrap().to_owned();
    let output = command
        .output()
        .context(format!("failed to run {}", program))
        .unwrap();
    println!(
        "stdout:\n{}",
        pretty_output(&output.stdout, &format!("[{}:stdout] ", program))
    );
    println!(
        "stderr:\n{}",
        pretty_output(&output.stderr, &format!("[{}:stderr] ", program))
    );

    String::from_utf8(output.stdout).unwrap()
}
