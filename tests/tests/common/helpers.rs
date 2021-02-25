use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::{self, BufRead};
use std::path::Path;
use std::sync::atomic::{AtomicU16, Ordering};

/// A counter for uniquely naming Ganache containers
static GANACHE_CONTAINER_COUNT: AtomicU16 = AtomicU16::new(0);
/// A counter for uniquely naming Postgres databases
static POSTGRES_DATABASE_COUNT: AtomicU16 = AtomicU16::new(0);
/// A counter for uniquely assigning ports.
static PORT_NUMBER_COUNTER: AtomicU16 = AtomicU16::new(10_000);

const POSTGRESQL_DEFAULT_PORT: u16 = 5432;
const GANACHE_DEFAULT_PORT: u16 = 8545;
const IPFS_DEFAULT_PORT: u16 = 5001;

/// Maps `Service => Host` exposed ports.
#[derive(Debug)]
pub struct MappedPorts(pub HashMap<u16, u16>);

/// Strip parent directories from filenames
pub fn basename(path: &impl AsRef<Path>) -> String {
    path.as_ref()
        .file_name()
        .map(OsStr::to_string_lossy)
        .map(String::from)
        .expect("failed to infer basename for path.")
}

/// Fetches a unique number for naming Ganache containers
pub fn get_unique_ganache_counter() -> u16 {
    increase_atomic_counter(&GANACHE_CONTAINER_COUNT)
}
/// Fetches a unique number for naming Postgres databases
pub fn get_unique_postgres_counter() -> u16 {
    increase_atomic_counter(&POSTGRES_DATABASE_COUNT)
}
/// Fetches a unique port number
pub fn get_unique_port_number() -> u16 {
    increase_atomic_counter(&PORT_NUMBER_COUNTER)
}

fn increase_atomic_counter(counter: &'static AtomicU16) -> u16 {
    let old_count = counter.fetch_add(1, Ordering::SeqCst);
    old_count + 1
}

/// Parses stdio bytes into a prefixed String
pub fn pretty_output(stdio: &[u8], prefix: &str) -> String {
    let mut cursor = io::Cursor::new(stdio);
    let mut buf = vec![];
    let mut string = String::new();
    loop {
        buf.clear();
        let bytes_read = cursor
            .read_until(b'\n', &mut buf)
            .expect("failed to read from stdio.");
        if bytes_read == 0 {
            break;
        }
        let as_string = String::from_utf8_lossy(&buf);
        string.push_str(&prefix);
        string.push_str(&as_string); // will contain a newline
    }
    string
}

#[derive(Debug)]
pub struct GraphNodePorts {
    pub http: u16,
    pub index: u16,
    pub ws: u16,
    pub admin: u16,
    pub metrics: u16,
}
impl GraphNodePorts {
    /// Returns five available port numbers, using dynamic port ranges
    pub fn get_ports() -> GraphNodePorts {
        let mut ports = [0u16; 5];
        for port in ports.iter_mut() {
            let min = get_unique_port_number();
            let max = min + 1_000;
            let free_port_in_range = port_check::free_local_port_in_range(min, max)
                .expect("failed to obtain a free port in range");
            *port = free_port_in_range;
        }
        GraphNodePorts {
            http: ports[0],
            index: ports[1],
            ws: ports[2],
            admin: ports[3],
            metrics: ports[4],
        }
    }
}

// Build a postgres connection string
pub fn make_postgres_uri(unique_id: &u16, postgres_ports: &MappedPorts) -> String {
    let port = postgres_ports
        .0
        .get(&POSTGRESQL_DEFAULT_PORT)
        .expect("failed to fetch Postgres port from mapped ports");
    format!(
        "postgresql://{user}:{password}@{host}:{port}/{database_name}",
        user = "postgres",
        password = "password",
        host = "localhost",
        port = port,
        database_name = postgres_test_database_name(unique_id),
    )
}

pub fn make_ipfs_uri(ipfs_ports: &MappedPorts) -> String {
    let port = ipfs_ports
        .0
        .get(&IPFS_DEFAULT_PORT)
        .expect("failed to fetch IPFS port from mapped ports");
    format!("http://{host}:{port}", host = "localhost", port = port)
}

// Build a Ganache connection string. Returns the port number and the URI.
pub fn make_ganache_uri(ganache_ports: &MappedPorts) -> (u16, String) {
    let port = ganache_ports
        .0
        .get(&GANACHE_DEFAULT_PORT)
        .expect("failed to fetch Ganache port from mapped ports");
    let uri = format!("test:http://{host}:{port}", host = "localhost", port = port);
    (port.clone(), uri)
}

pub fn contains_subslice<T: PartialEq>(data: &[T], needle: &[T]) -> bool {
    data.windows(needle.len()).any(|w| w == needle)
}

pub fn postgres_test_database_name(unique_id: &u16) -> String {
    format!("test_database_{}", unique_id)
}
