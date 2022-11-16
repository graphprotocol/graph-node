use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::Serialize;

use crate::env::ENV_VARS;

#[derive(Debug, Serialize)]
pub enum Trace {
    None,
    Root {
        query: Arc<String>,
        elapsed: Mutex<Duration>,
        children: Vec<(String, Trace)>,
    },
    Query {
        query: String,
        elapsed: Duration,
        entity_count: usize,

        children: Vec<(String, Trace)>,
    },
}

impl Default for Trace {
    fn default() -> Self {
        Self::None
    }
}

impl Trace {
    pub fn root(query: Arc<String>) -> Trace {
        if ENV_VARS.log_sql_timing() || ENV_VARS.log_gql_timing() {
            Trace::Root {
                query,
                elapsed: Mutex::new(Duration::from_millis(0)),
                children: Vec::new(),
            }
        } else {
            Trace::None
        }
    }

    pub fn finish(&self, dur: Duration) {
        match self {
            Trace::None | Trace::Query { .. } => { /* nothing to do */ }
            Trace::Root { elapsed, .. } => *elapsed.lock().unwrap() = dur,
        }
    }

    pub fn query(query: &str, elapsed: Duration, entity_count: usize) -> Trace {
        Trace::Query {
            query: query.to_string(),
            elapsed,
            entity_count,
            children: Vec::new(),
        }
    }

    pub fn push(&mut self, name: &str, trace: Trace) {
        match (self, &trace) {
            (Self::Root { children, .. }, Self::Query { .. }) => {
                children.push((name.to_string(), trace))
            }
            (Self::Query { children, .. }, Self::Query { .. }) => {
                children.push((name.to_string(), trace))
            }
            (Self::None, Self::None) | (Self::Root { .. }, Self::None) => { /* tracing is turned off */
            }
            (s, t) => {
                unreachable!("can not add child self: {:#?} trace: {:#?}", s, t)
            }
        }
    }
}
