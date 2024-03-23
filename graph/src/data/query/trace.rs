use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{ser::SerializeMap, Serialize};

use crate::{
    components::store::{BlockNumber, QueryPermit},
    prelude::{lazy_static, CheapClone},
};

use super::QueryExecutionError;

lazy_static! {
    pub static ref TRACE_NONE: Arc<Trace> = Arc::new(Trace::None);
}

#[derive(Debug)]
pub enum Trace {
    None,
    Root {
        query: Arc<String>,
        variables: Arc<String>,
        query_id: String,
        elapsed: Mutex<Duration>,
        blocks: Vec<Arc<Trace>>,
    },
    Block {
        block: BlockNumber,
        elapsed: Mutex<Duration>,
        permit_wait: Duration,
        /// Pairs of response key and traces. Each trace is either a `Trace::Query` or a `Trace::None`
        children: Vec<(String, Trace)>,
    },
    Query {
        /// The SQL query that was executed
        query: String,
        /// How long executing the SQL query took. This is just the time it
        /// took to send the already built query to the database and receive
        /// results.
        elapsed: Duration,
        /// How long we had to wait for a connection from the pool
        conn_wait: Duration,
        permit_wait: Duration,
        entity_count: usize,
        /// Pairs of response key and traces. Each trace is either a `Trace::Query` or a `Trace::None`
        children: Vec<(String, Trace)>,
    },
}

impl Default for Trace {
    fn default() -> Self {
        Self::None
    }
}

impl Trace {
    pub fn root(
        query: &Arc<String>,
        variables: &Arc<String>,
        query_id: &str,
        do_trace: bool,
    ) -> Trace {
        if do_trace {
            Trace::Root {
                query: query.cheap_clone(),
                variables: variables.cheap_clone(),
                query_id: query_id.to_string(),
                elapsed: Mutex::new(Duration::from_millis(0)),
                blocks: Vec::new(),
            }
        } else {
            Trace::None
        }
    }

    pub fn block(block: BlockNumber, do_trace: bool) -> Trace {
        if do_trace {
            Trace::Block {
                block,
                elapsed: Mutex::new(Duration::from_millis(0)),
                permit_wait: Duration::from_millis(0),
                children: Vec::new(),
            }
        } else {
            Trace::None
        }
    }

    pub fn finish(&self, dur: Duration) {
        match self {
            Trace::None | Trace::Query { .. } => { /* nothing to do */ }
            Trace::Block { elapsed, .. } => *elapsed.lock().unwrap() = dur,
            Trace::Root { elapsed, .. } => *elapsed.lock().unwrap() = dur,
        }
    }

    pub fn query(query: &str, elapsed: Duration, entity_count: usize) -> Trace {
        Trace::Query {
            query: query.to_string(),
            elapsed,
            conn_wait: Duration::from_millis(0),
            permit_wait: Duration::from_millis(0),
            entity_count,
            children: Vec::new(),
        }
    }

    pub fn push(&mut self, name: &str, trace: Trace) {
        match (self, &trace) {
            (Self::Block { children, .. }, Self::Query { .. }) => {
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

    pub fn is_none(&self) -> bool {
        match self {
            Trace::None => true,
            Trace::Root { .. } | Trace::Block { .. } | Trace::Query { .. } => false,
        }
    }

    pub fn conn_wait(&mut self, time: Duration) {
        match self {
            Trace::None => { /* nothing to do  */ }
            Trace::Root { .. } | Trace::Block { .. } => {
                unreachable!("can not add conn_wait to Root or Block")
            }
            Trace::Query { conn_wait, .. } => *conn_wait += time,
        }
    }

    pub fn permit_wait(&mut self, res: &Result<QueryPermit, QueryExecutionError>) {
        let time = match res {
            Ok(permit) => permit.wait,
            Err(_) => {
                return;
            }
        };
        match self {
            Trace::None => { /* nothing to do  */ }
            Trace::Root { .. } => unreachable!("can not add permit_wait to Root"),
            Trace::Block { permit_wait, .. } | Trace::Query { permit_wait, .. } => {
                *permit_wait += time
            }
        }
    }

    pub fn append(&mut self, other: Arc<Trace>) {
        match self {
            Trace::None => { /* tracing turned off */ }
            Trace::Root { blocks, .. } => blocks.push(other),
            s => {
                unreachable!("can not append self: {:#?} trace: {:#?}", s, other)
            }
        }
    }
}

impl Serialize for Trace {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Trace::None => ser.serialize_none(),
            Trace::Root {
                query,
                variables,
                query_id,
                elapsed,
                blocks: children,
            } => {
                let mut map = ser.serialize_map(Some(children.len() + 2))?;
                map.serialize_entry("query", query)?;
                if !variables.is_empty() && variables.as_str() != "{}" {
                    map.serialize_entry("variables", variables)?;
                }
                map.serialize_entry("query_id", query_id)?;
                map.serialize_entry("elapsed_ms", &elapsed.lock().unwrap().as_millis())?;
                map.serialize_entry("blocks", children)?;
                map.end()
            }
            Trace::Block {
                block,
                elapsed,
                permit_wait,
                children,
            } => {
                let mut map = ser.serialize_map(Some(children.len() + 3))?;
                map.serialize_entry("block", block)?;
                map.serialize_entry("elapsed_ms", &elapsed.lock().unwrap().as_millis())?;
                for (child, trace) in children {
                    map.serialize_entry(child, trace)?;
                }
                map.serialize_entry("permit_wait_ms", &permit_wait.as_millis())?;
                map.end()
            }
            Trace::Query {
                query,
                elapsed,
                conn_wait,
                permit_wait,
                entity_count,
                children,
            } => {
                let mut map = ser.serialize_map(Some(children.len() + 3))?;
                map.serialize_entry("query", query)?;
                map.serialize_entry("elapsed_ms", &elapsed.as_millis())?;
                map.serialize_entry("conn_wait_ms", &conn_wait.as_millis())?;
                map.serialize_entry("permit_wait_ms", &permit_wait.as_millis())?;
                map.serialize_entry("entity_count", entity_count)?;
                for (child, trace) in children {
                    map.serialize_entry(child, trace)?;
                }
                map.end()
            }
        }
    }
}
