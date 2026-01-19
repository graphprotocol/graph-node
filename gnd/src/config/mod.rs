//! Configuration file handling for gnd.
//!
//! This module provides parsers and utilities for various configuration files
//! used by the gnd CLI:
//!
//! - `networks.json` - Network-specific contract addresses and start blocks

pub mod networks;

pub use networks::{
    apply_network_config, get_network_config, init_networks_config, load_networks_config,
    update_networks_file, DataSourceNetworkConfig, NetworkConfig, NetworksConfig,
};
