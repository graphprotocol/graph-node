use std::{sync::Arc, time::Duration};

use serde::{ser::SerializeMap, Serialize};

use crate::{
    components::store::{BlockNumber, QueryPermit},
    derive::CacheWeight,
    prelude::{lazy_static, CheapClone},
};

use super::{CacheStatus, QueryExecutionError};

lazy_static! {
    pub static ref TRACE_NONE: Arc<Trace> = Arc::new(Trace::None);
}

#[derive(Debug, CacheWeight)]
pub struct TraceWithCacheStatus {
    pub trace: Arc<Trace>,
    pub cache_status: CacheStatus,
}

#[derive(Debug, Default)]
pub struct HttpTrace {
    to_json: Duration,
    cache_weight: usize,
}

impl HttpTrace {
    pub fn new(to_json: Duration, cache_weight: usize) -> Self {
        HttpTrace {
            to_json,
            cache_weight,
        }
    }
}

#[derive(Debug, CacheWeight)]
pub enum Trace {
    None,
    Root {
        query: Arc<String>,
        variables: Arc<String>,
        query_id: String,
        /// How long setup took before we executed queries. This includes
        /// the time to get the current state of the deployment and setting
        /// up the `QueryStore`
        setup: Duration,
        /// The total time it took to execute the query; that includes setup
        /// and the processing time for all SQL queries. It does not include
        /// the time it takes to serialize the result
        elapsed: Duration,
        query_parsing: Duration,
        /// A list of `Trace::Block`, one for each block constraint in the query
        blocks: Vec<TraceWithCacheStatus>,
    },
    Block {
        block: BlockNumber,
        elapsed: Duration,
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
                elapsed: Duration::ZERO,
                setup: Duration::ZERO,
                query_parsing: Duration::ZERO,
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
                elapsed: Duration::from_millis(0),
                permit_wait: Duration::from_millis(0),
                children: Vec::new(),
            }
        } else {
            Trace::None
        }
    }

    pub fn query_done(&mut self, dur: Duration, permit: &Result<QueryPermit, QueryExecutionError>) {
        let permit_dur = match permit {
            Ok(permit) => permit.wait,
            Err(_) => Duration::from_millis(0),
        };
        match self {
            Trace::None => { /* nothing to do */ }
            Trace::Root { .. } => {
                unreachable!("can not call query_done on Root")
            }
            Trace::Block {
                elapsed,
                permit_wait,
                ..
            }
            | Trace::Query {
                elapsed,
                permit_wait,
                ..
            } => {
                *elapsed = dur;
                *permit_wait = permit_dur;
            }
        }
    }

    pub fn finish(&mut self, setup_dur: Duration, total: Duration) {
        match self {
            Trace::None => { /* nothing to do */ }
            Trace::Query { .. } | Trace::Block { .. } => {
                unreachable!("can not call finish on Query or Block")
            }
            Trace::Root { elapsed, setup, .. } => {
                *setup = setup_dur;
                *elapsed = total
            }
        }
    }

    pub fn query(query: &str, elapsed: Duration, entity_count: usize) -> Trace {
        // Strip out the comment `/* .. */` that adds various tags to the
        // query that are irrelevant for us
        let query = match query.find("*/") {
            Some(pos) => &query[pos + 2..],
            None => query,
        };

        let query = query.replace("\t", "").replace("\"", "");
        Trace::Query {
            query,
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

    pub fn append(&mut self, trace: Arc<Trace>, cache_status: CacheStatus) {
        match self {
            Trace::None => { /* tracing turned off */ }
            Trace::Root { blocks, .. } => blocks.push(TraceWithCacheStatus {
                trace,
                cache_status,
            }),
            s => {
                unreachable!("can not append self: {:#?} trace: {:#?}", s, trace)
            }
        }
    }

    pub fn query_parsing(&mut self, time: Duration) {
        match self {
            Trace::None => { /* nothing to do  */ }
            Trace::Root { query_parsing, .. } => *query_parsing += time,
            Trace::Block { .. } | Trace::Query { .. } => {
                unreachable!("can not add query_parsing to Block or Query")
            }
        }
    }

    /// Return the total time spent executing database queries
    pub fn query_total(&self) -> QueryTotal {
        QueryTotal::calculate(self)
    }
}

#[derive(Default)]
pub struct QueryTotal {
    pub elapsed: Duration,
    pub conn_wait: Duration,
    pub permit_wait: Duration,
    pub entity_count: usize,
    pub query_count: usize,
    pub cached_count: usize,
}

impl QueryTotal {
    fn add(&mut self, trace: &Trace) {
        use Trace::*;
        match trace {
            None => { /* nothing to do */ }
            Root { blocks, .. } => {
                blocks.iter().for_each(|twc| {
                    if twc.cache_status.uses_database() {
                        self.query_count += 1;
                        self.add(&twc.trace)
                    } else {
                        self.cached_count += 1
                    }
                });
            }
            Block { children, .. } => {
                children.iter().for_each(|(_, trace)| self.add(trace));
            }
            Query {
                elapsed,
                conn_wait,
                permit_wait,
                children,
                entity_count,
                ..
            } => {
                self.elapsed += *elapsed;
                self.conn_wait += *conn_wait;
                self.permit_wait += *permit_wait;
                self.entity_count += entity_count;
                children.iter().for_each(|(_, trace)| self.add(trace));
            }
        }
    }

    fn calculate(trace: &Trace) -> Self {
        let mut qt = QueryTotal::default();
        qt.add(trace);
        qt
    }
}

impl Serialize for QueryTotal {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = ser.serialize_map(Some(4))?;
        map.serialize_entry("elapsed_ms", &self.elapsed.as_millis())?;
        map.serialize_entry("conn_wait_ms", &self.conn_wait.as_millis())?;
        map.serialize_entry("permit_wait_ms", &self.permit_wait.as_millis())?;
        map.serialize_entry("entity_count", &self.entity_count)?;
        map.serialize_entry("query_count", &self.query_count)?;
        map.serialize_entry("cached_count", &self.cached_count)?;
        map.end()
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
                setup,
                query_parsing,
                blocks,
            } => {
                let qt = self.query_total();
                let mut map = ser.serialize_map(Some(8))?;
                map.serialize_entry("query", query)?;
                if !variables.is_empty() && variables.as_str() != "{}" {
                    map.serialize_entry("variables", variables)?;
                }
                map.serialize_entry("query_id", query_id)?;
                map.serialize_entry("elapsed_ms", &elapsed.as_millis())?;
                map.serialize_entry("setup_ms", &setup.as_millis())?;
                map.serialize_entry("query_parsing_ms", &query_parsing.as_millis())?;
                map.serialize_entry("db", &qt)?;
                map.serialize_entry("blocks", blocks)?;
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
                map.serialize_entry("elapsed_ms", &elapsed.as_millis())?;
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

impl Serialize for TraceWithCacheStatus {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = ser.serialize_map(Some(2))?;
        map.serialize_entry("trace", &self.trace)?;
        map.serialize_entry("cache", &self.cache_status)?;
        map.end()
    }
}

impl Serialize for HttpTrace {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = ser.serialize_map(Some(3))?;
        map.serialize_entry("to_json", &format!("{:?}", self.to_json))?;
        map.serialize_entry("cache_weight", &self.cache_weight)?;
        map.end()
    }
}
