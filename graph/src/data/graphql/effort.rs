//! Utilities to keep moving statistics about queries

use rand::{prelude::Rng, thread_rng};
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use lazy_static::lazy_static;

use crate::components::store::PoolWaitStats;
use crate::util::stats::{MovingStats, BIN_SIZE, WINDOW_SIZE};

lazy_static! {
    static ref LOAD_THRESHOLD: Duration = {
        let threshold = env::var("GRAPH_LOAD_THRESHOLD")
            .ok()
            .map(|s| {
                u64::from_str(&s).unwrap_or_else(|_| {
                    panic!("GRAPH_LOAD_THRESHOLD must be a number, but is `{}`", s)
                })
            })
            .unwrap_or(0);
        Duration::from_millis(threshold)
    };

    static ref JAIL_THRESHOLD: f64 = {
        env::var("GRAPH_LOAD_JAIL_THRESHOLD")
            .ok()
            .map(|s| {
                f64::from_str(&s).unwrap_or_else(|_| {
                    panic!("GRAPH_LOAD_JAIL_THRESHOLD must be a number, but is `{}`", s)
                })
            })
            .unwrap_or(0.1)
    };

    static ref WAIT_STAT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

    static ref ZERO_DURATION: Duration = Duration::from_millis(0);

    // Load management can be disabled by setting the threshold to 0. This
    // makes sure in particular that we never take any of the locks
    // associated with it
    static ref LOAD_MANAGEMENT_DISABLED: bool = *LOAD_THRESHOLD == *ZERO_DURATION;

    static ref KILL_RATE_UPDATE_INTERVAL: Duration = Duration::from_millis(1000);
}

struct QueryEffort {
    inner: Arc<RwLock<QueryEffortInner>>,
}

/// Track the effort for queries (identified by their ShapeHash) over a
/// time window.
struct QueryEffortInner {
    window_size: Duration,
    bin_size: Duration,
    effort: HashMap<u64, MovingStats>,
    total: MovingStats,
}

/// Create a `QueryEffort` that uses the window and bin sizes configured in
/// the environment
impl Default for QueryEffort {
    fn default() -> Self {
        Self::new(*WINDOW_SIZE, *BIN_SIZE)
    }
}

impl QueryEffort {
    pub fn new(window_size: Duration, bin_size: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(QueryEffortInner::new(window_size, bin_size))),
        }
    }

    pub fn add(&self, shape_hash: u64, duration: Duration) {
        let mut inner = self.inner.write().unwrap();
        inner.add(shape_hash, duration);
    }

    /// Return what we know right now about the effort for the query
    /// `shape_hash`, and about the total effort. If we have no measurements
    /// at all, return `ZERO_DURATION` as the total effort. If we have no
    /// data for the particular query, return `None` as the effort
    /// for the query
    pub fn current_effort(&self, shape_hash: u64) -> (Option<Duration>, Duration) {
        let inner = self.inner.read().unwrap();
        let total_effort = inner.total.average().unwrap_or(*ZERO_DURATION);
        let query_effort = inner
            .effort
            .get(&shape_hash)
            .map(|stats| stats.average())
            .flatten();
        (query_effort, total_effort)
    }
}

impl QueryEffortInner {
    fn new(window_size: Duration, bin_size: Duration) -> Self {
        Self {
            window_size,
            bin_size,
            effort: HashMap::default(),
            total: MovingStats::new(window_size, bin_size),
        }
    }

    fn add(&mut self, shape_hash: u64, duration: Duration) {
        let window_size = self.window_size;
        let bin_size = self.bin_size;
        let now = Instant::now();
        self.effort
            .entry(shape_hash)
            .or_insert_with(|| MovingStats::new(window_size, bin_size))
            .add_at(now, duration);
        self.total.add_at(now, duration);
    }
}

// The size of any individual step when we adjust the kill_rate
const KILL_RATE_STEP: f64 = 0.1;

struct KillState {
    kill_rate: f64,
    last_update: Instant,
}

impl KillState {
    fn new() -> Self {
        Self {
            kill_rate: 0.0,
            last_update: Instant::now() - Duration::from_secs(86400),
        }
    }
}

pub struct LoadManager {
    effort: QueryEffort,
    store_wait_stats: PoolWaitStats,
    /// List of query shapes that have been statically blocked through
    /// configuration
    blocked_queries: HashSet<u64>,
    /// List of query shapes that have caused more than `JAIL_THRESHOLD`
    /// proportion of the work while the system was overloaded. Currently,
    /// there is no way for a query to get out of jail other than
    /// restarting the process
    jailed_queries: RwLock<HashSet<u64>>,
    kill_state: RwLock<KillState>,
}

impl LoadManager {
    pub fn new(store_wait_stats: PoolWaitStats, blocked_queries: HashSet<u64>) -> Self {
        Self {
            effort: QueryEffort::default(),
            store_wait_stats,
            blocked_queries,
            jailed_queries: RwLock::new(HashSet::new()),
            kill_state: RwLock::new(KillState::new()),
        }
    }

    pub fn add_query(&self, shape_hash: u64, duration: Duration) {
        if !*LOAD_MANAGEMENT_DISABLED {
            self.effort.add(shape_hash, duration);
        }
    }

    /// Return `true` if we should decline running the query with this
    /// `ShapeHash`. This is the heart of reacting to overload situations.
    ///
    /// The decision to decline a query is geared towards mitigating two
    /// different ways in which the system comes under high load:
    /// 1) A relatively small number of queries causes a large fraction
    ///    of the overall work that goes into responding to queries. That
    ///    is usually inadvertent, and the result of a dApp using a new query,
    ///    or the data for a subgraph changing in a way that makes a query
    ///    that was previously fast take a long time
    /// 2) A large number of queries that by themselves are reasonably fast
    ///    cause so much work that the system gets bogged down. When none
    ///    of them by themselves is expensive, it becomes impossible to
    ///    name a culprit for an overload, and we therefore shed
    ///    increasing amounts of traffic by declining to run queries
    ///    in proportion to the work they cause
    ///
    /// Note that any mitigation for (2) is prone to flip-flopping in and
    /// out of overload situations, as we will oscillate  between being
    /// overloaded and not being overloaded, though we'd expect the amount
    /// of traffic we shed to settle on something that stays close to the
    /// point where we alternate between the two states.
    ///
    /// We detect whether we are in an overloaded situation by looking at
    /// the average wait time for connection checkouts. If that exceeds
    /// `GRAPH_LOAD_THRESHOLD`, we consider ourselves to be in an overload
    /// situation.
    ///
    /// There are several criteria that will lead to us declining to run
    /// a query with a certain `ShapeHash`:
    /// 1) If the query is one of the configured `blocked_queries`, we will
    ///    always decline
    /// 2) If a query, during an overload situation, causes more than
    ///    `JAIL_THRESHOLD` fraction of the total query effort, we will
    ///    refuse to run this query again for the lifetime of the process
    /// 3) During an overload situation, we step a `kill_rate` from 0 to 1,
    ///    roughly in steps of `KILL_RATE_STEP`, though with an eye towards
    ///    not hitting a `kill_rate` of 1 too soon. We will decline to run
    ///    queries randomly with a probability of
    ///    kill_rate * query_effort / total_effort
    ///
    /// If `GRAPH_LOAD_THRESHOLD` is set to 0, we bypass all this logic, and
    /// only ever decline to run statically configured queries (1). In that
    /// case, we also do not take any locks when asked to update statistics,
    /// or to check whether we are overloaded; these operations amount to
    /// noops.
    pub fn decline(&self, shape_hash: u64) -> bool {
        if self.blocked_queries.contains(&shape_hash) {
            return true;
        }
        if *LOAD_MANAGEMENT_DISABLED {
            return false;
        }

        if self.jailed_queries.read().unwrap().contains(&shape_hash) {
            return true;
        }

        let overloaded = self.overloaded();
        let (kill_rate, last_update) = self.kill_state();
        if !overloaded && kill_rate == 0.0 {
            return false;
        }

        let (query_effort, total_effort) = self.effort.current_effort(shape_hash);
        // When `total_effort` is `ZERO_DURATION`, we haven't done any work. All are
        // welcome
        if total_effort == *ZERO_DURATION {
            return false;
        }

        // If `query_effort` is `None`, we haven't seen the query. Since we
        // are in an overload situation, we are very suspicious of new things
        // and assume the worst. This ensures that even if we only ever see
        // new queries, we drop `kill_rate` amount of traffic
        let known_query = query_effort.is_some();
        let query_effort = query_effort.unwrap_or_else(|| total_effort).as_millis() as f64;
        let total_effort = total_effort.as_millis() as f64;

        if known_query && query_effort / total_effort > *JAIL_THRESHOLD {
            // Any single query that causes at least JAIL_THRESHOLD of the
            // effort in an overload situation gets killed
            self.jailed_queries.write().unwrap().insert(shape_hash);
            return true;
        }

        // Kill random queries in case we have no queries, or not enough queries
        // that cause at least 20% of the effort
        let kill_rate = self.update_kill_rate(kill_rate, last_update, overloaded);
        thread_rng().gen_bool(kill_rate * query_effort / total_effort)
    }

    fn overloaded(&self) -> bool {
        let stats = self.store_wait_stats.read().unwrap();
        stats.average_gt(*LOAD_THRESHOLD)
    }

    fn kill_state(&self) -> (f64, Instant) {
        let state = self.kill_state.read().unwrap();
        (state.kill_rate, state.last_update)
    }

    fn update_kill_rate(&self, mut kill_rate: f64, last_update: Instant, overloaded: bool) -> f64 {
        let now = Instant::now();
        if now.saturating_duration_since(last_update) > *KILL_RATE_UPDATE_INTERVAL {
            if overloaded {
                kill_rate = kill_rate + KILL_RATE_STEP * (1.0 - kill_rate);
            } else {
                kill_rate = (kill_rate - KILL_RATE_STEP).max(0.0);
            }
            // FIXME: Log the new kill_rate
            let mut state = self.kill_state.write().unwrap();
            state.kill_rate = kill_rate;
            state.last_update = now;
        }
        kill_rate
    }
}
