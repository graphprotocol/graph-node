use std::sync::Arc;

use graph::{prelude::MetricsRegistry, prometheus::Registry};

extern crate diesel;

pub mod manager;
pub mod opt;
