use std::time::{Duration, Instant};

use diesel::{
    sql_query,
    sql_types::{BigInt, Integer},
    PgConnection, RunQueryDsl as _,
};
use graph::{
    env::ENV_VARS,
    prelude::{BlockNumber, BlockPtr, StoreError},
    util::ogive::Ogive,
};

use crate::{
    catalog,
    primary::Namespace,
    relational::{Table, VID_COLUMN},
};

/// The initial batch size for tables that do not have an array column
const INITIAL_BATCH_SIZE: i64 = 10_000;
/// The initial batch size for tables that do have an array column; those
/// arrays can be large and large arrays will slow down copying a lot. We
/// therefore tread lightly in that case
const INITIAL_BATCH_SIZE_LIST: i64 = 100;

/// Track the desired size of a batch in such a way that doing the next
/// batch gets close to TARGET_DURATION for the time it takes to copy one
/// batch, but don't step up the size by more than 2x at once
#[derive(Debug, Queryable)]
pub(crate) struct AdaptiveBatchSize {
    pub size: i64,
    pub target: Duration,
}

impl AdaptiveBatchSize {
    pub fn new(table: &Table) -> Self {
        let size = if table.columns.iter().any(|col| col.is_list()) {
            INITIAL_BATCH_SIZE_LIST
        } else {
            INITIAL_BATCH_SIZE
        };

        Self {
            size,
            target: ENV_VARS.store.batch_target_duration,
        }
    }

    // adjust batch size by trying to extrapolate in such a way that we
    // get close to TARGET_DURATION for the time it takes to copy one
    // batch, but don't step up batch_size by more than 2x at once
    pub fn adapt(&mut self, duration: Duration) -> i64 {
        // Avoid division by zero
        let duration = duration.as_millis().max(1);
        let new_batch_size = self.size as f64 * self.target.as_millis() as f64 / duration as f64;
        self.size = (2 * self.size).min(new_batch_size.round() as i64);
        self.size
    }
}

/// A timer that works like `std::time::Instant` in non-test code, but
/// returns a fake elapsed value in tests
struct Timer {
    start: Instant,
    #[cfg(test)]
    duration: Duration,
}

impl Timer {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            #[cfg(test)]
            duration: Duration::from_secs(0),
        }
    }

    fn start(&mut self) {
        self.start = Instant::now();
    }

    #[cfg(test)]
    fn elapsed(&self) -> Duration {
        self.duration
    }

    #[cfg(not(test))]
    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    #[cfg(test)]
    fn set(&mut self, duration: Duration) {
        self.duration = duration;
    }
}

/// A batcher for moving through a large range of `vid` values in a way such
/// that each batch takes approximatley the same amount of time. The batcher
/// takes uneven distributions of `vid` values into account by using the
/// histogram from `pg_stats` for the table through which we are iterating.
pub(crate) struct VidBatcher {
    batch_size: AdaptiveBatchSize,
    start: i64,
    end: i64,
    max_vid: i64,

    ogive: Option<Ogive>,

    step_timer: Timer,
}

impl VidBatcher {
    /// Initialize a batcher for batching through entries in `table` with
    /// `vid` in the given `vid_range`
    ///
    /// The `vid_range` is inclusive, i.e., the batcher will iterate over
    /// all vids `vid_range.0 <= vid <= vid_range.1`; for an empty table,
    /// the `vid_range` must be set to `(-1, 0)`
    pub fn load(
        conn: &mut PgConnection,
        nsp: &Namespace,
        table: &Table,
        vid_range: VidRange,
    ) -> Result<Self, StoreError> {
        let bounds = catalog::histogram_bounds(conn, nsp, &table.name, VID_COLUMN)?;
        let batch_size = AdaptiveBatchSize::new(table);
        Self::new(bounds, vid_range, batch_size)
    }

    fn new(
        bounds: Vec<i64>,
        range: VidRange,
        batch_size: AdaptiveBatchSize,
    ) -> Result<Self, StoreError> {
        let start = range.min;

        let bounds = {
            // Keep only histogram bounds that are relevent for the range
            let mut bounds = bounds
                .into_iter()
                .filter(|bound| range.min <= *bound && range.max >= *bound)
                .collect::<Vec<_>>();
            // The first and last entry in `bounds` are Postgres' estimates
            // of the min and max `vid` values in the table. We use the
            // actual min and max `vid` values from the `vid_range` instead
            let len = bounds.len();
            if len > 1 {
                bounds[0] = range.min;
                bounds[len - 1] = range.max;
            } else {
                // If Postgres doesn't have a histogram, just use one bucket
                // from min to max
                bounds = vec![range.min, range.max];
            }
            bounds
        };
        let mut ogive = if range.is_empty() {
            None
        } else {
            Some(Ogive::from_equi_histogram(bounds, range.size())?)
        };
        let end = match ogive.as_mut() {
            None => start + batch_size.size,
            Some(ogive) => ogive.next_point(start, batch_size.size as usize)?,
        };

        Ok(Self {
            batch_size,
            start,
            end,
            max_vid: range.max,
            ogive,
            step_timer: Timer::new(),
        })
    }

    /// Explicitly set the batch size
    pub fn with_batch_size(mut self: VidBatcher, size: usize) -> Self {
        self.batch_size.size = size as i64;
        self
    }

    pub(crate) fn next_vid(&self) -> i64 {
        self.start
    }

    pub(crate) fn target_vid(&self) -> i64 {
        self.max_vid
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size.size as usize
    }

    pub fn finished(&self) -> bool {
        self.start > self.max_vid
    }

    /// Perform the work for one batch. The function `f` is called with the
    /// start and end `vid` for this batch and should perform all the work
    /// for rows with `start <= vid <= end`, i.e. the start and end values
    /// are inclusive.
    ///
    /// Once `f` returns, the batch size will be adjusted so that the time
    /// the next batch will take is close to the target duration.
    ///
    /// The function returns the time it took to process the batch and the
    /// result of `f`. If the batcher is finished, `f` will not be called,
    /// and `None` will be returned as its result.
    pub fn step<F, T>(&mut self, mut f: F) -> Result<(Duration, Option<T>), StoreError>
    where
        F: FnMut(i64, i64) -> Result<T, StoreError>,
    {
        if self.finished() {
            return Ok((Duration::from_secs(0), None));
        }

        match self.ogive.as_mut() {
            None => Ok((Duration::from_secs(0), None)),
            Some(ogive) => {
                self.step_timer.start();

                let res = f(self.start, self.end)?;
                let duration = self.step_timer.elapsed();

                let batch_size = self.batch_size.adapt(duration);
                // We can't possibly copy farther than `max_vid`
                self.start = (self.end + 1).min(self.max_vid + 1);
                self.end = ogive.next_point(self.start, batch_size as usize)?;

                Ok((duration, Some(res)))
            }
        }
    }

    pub(crate) fn set_batch_size(&mut self, size: usize) {
        self.batch_size.size = size as i64;
    }
}

#[derive(Copy, Clone, QueryableByName)]
pub(crate) struct VidRange {
    #[diesel(sql_type = BigInt, column_name = "min_vid")]
    pub min: i64,
    #[diesel(sql_type = BigInt, column_name = "max_vid")]
    pub max: i64,
}

const EMPTY_VID_RANGE: VidRange = VidRange { max: -1, min: 0 };

impl VidRange {
    pub fn new(min_vid: i64, max_vid: i64) -> Self {
        Self {
            min: min_vid,
            max: max_vid,
        }
    }

    pub fn is_empty(&self) -> bool {
        // min > max can happen when we restart a copy job that has finished
        // some tables. For those, min (the next_vid) will be larger than
        // max (the target_vid)
        self.max == -1 || self.min > self.max
    }

    pub fn size(&self) -> usize {
        (self.max - self.min) as usize + 1
    }

    /// Return the full range of `vid` values in the table `src`
    pub fn for_copy(
        conn: &mut PgConnection,
        src: &Table,
        target_block: &BlockPtr,
    ) -> Result<Self, StoreError> {
        let max_block_clause = if src.immutable {
            "block$ <= $1"
        } else {
            "lower(block_range) <= $1"
        };
        let vid_range = sql_query(format!(
            "/* controller=copy,target={target_number} */ \
             select coalesce(min(vid), 0) as min_vid, \
                    coalesce(max(vid), -1) as max_vid \
               from {src_name} where {max_block_clause}",
            target_number = target_block.number,
            src_name = src.qualified_name.as_str(),
            max_block_clause = max_block_clause
        ))
        .bind::<Integer, _>(&target_block.number)
        .load::<VidRange>(conn)?
        .pop()
        .unwrap_or(EMPTY_VID_RANGE);
        Ok(vid_range)
    }

    /// Return the first and last vid of any entity that is visible in the
    /// block range from `first_block` (inclusive) to `last_block`
    /// (exclusive)
    pub fn for_prune(
        conn: &mut PgConnection,
        src: &Table,
        first_block: BlockNumber,
        last_block: BlockNumber,
    ) -> Result<Self, StoreError> {
        sql_query(format!(
            "/* controller=prune,first={first_block},last={last_block} */ \
                     select coalesce(min(vid), 0) as min_vid, \
                            coalesce(max(vid), -1) as max_vid from {src} \
                      where lower(block_range) <= $2 \
                        and coalesce(upper(block_range), 2147483647) > $1 \
                        and coalesce(upper(block_range), 2147483647) <= $2 \
                        and block_range && int4range($1, $2)",
            src = src.qualified_name,
        ))
        .bind::<Integer, _>(first_block)
        .bind::<Integer, _>(last_block)
        .get_result::<VidRange>(conn)
        .map_err(StoreError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S001: Duration = Duration::from_secs(1);
    const S010: Duration = Duration::from_secs(10);
    const S050: Duration = Duration::from_secs(50);
    const S100: Duration = Duration::from_secs(100);
    const S200: Duration = Duration::from_secs(200);

    struct Batcher {
        vid: VidBatcher,
    }

    impl Batcher {
        fn new(bounds: Vec<i64>, size: i64) -> Self {
            let batch_size = AdaptiveBatchSize { size, target: S100 };
            let vid_range = VidRange::new(bounds[0], *bounds.last().unwrap());
            Self {
                vid: VidBatcher::new(bounds, vid_range, batch_size).unwrap(),
            }
        }

        #[track_caller]
        fn at(&self, start: i64, end: i64, size: i64) {
            assert_eq!(self.vid.start, start, "at start");
            assert_eq!(self.vid.end, end, "at end");
            assert_eq!(self.vid.batch_size.size, size, "at size");
        }

        #[track_caller]
        fn step(&mut self, start: i64, end: i64, duration: Duration) {
            self.vid.step_timer.set(duration);

            match self.vid.step(|s, e| Ok((s, e))).unwrap() {
                (d, Some((s, e))) => {
                    // Failing here indicates that our clever Timer is misbehaving
                    assert_eq!(d, duration, "step duration");
                    assert_eq!(s, start, "step start");
                    assert_eq!(e, end, "step end");
                }
                (_, None) => {
                    if start > end {
                        // Expected, the batcher is exhausted
                        return;
                    } else {
                        panic!("step didn't return start and end")
                    }
                }
            }
        }

        #[track_caller]
        fn run(&mut self, start: i64, end: i64, size: i64, duration: Duration) {
            self.at(start, end, size);
            self.step(start, end, duration);
        }

        fn finished(&self) -> bool {
            self.vid.finished()
        }
    }

    impl std::fmt::Debug for Batcher {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Batcher")
                .field("start", &self.vid.start)
                .field("end", &self.vid.end)
                .field("size", &self.vid.batch_size.size)
                .field("duration", &self.vid.batch_size.target.as_secs())
                .finish()
        }
    }

    #[test]
    fn simple() {
        let bounds = vec![10, 20, 30, 40, 49];
        let mut batcher = Batcher::new(bounds, 5);

        batcher.at(10, 15, 5);

        batcher.step(10, 15, S001);
        batcher.at(16, 26, 10);

        batcher.step(16, 26, S001);
        batcher.at(27, 46, 20);
        assert!(!batcher.finished());

        batcher.step(27, 46, S001);
        batcher.at(47, 49, 40);
        assert!(!batcher.finished());

        batcher.step(47, 49, S001);
        assert!(batcher.finished());
        batcher.at(50, 49, 80);
    }

    #[test]
    fn non_uniform() {
        // A distribution that is flat in the beginning and then steeper and
        // linear towards the end. The easiest way to see this is to graph
        // `(bounds[i], i*40)`
        let bounds = vec![40, 180, 260, 300, 320, 330, 340, 350, 359];
        let mut batcher = Batcher::new(bounds, 10);

        // The schedule of how we move through the bounds above in batches,
        // with varying timings for each batch
        batcher.run(040, 075, 10, S010);
        batcher.run(076, 145, 20, S010);
        batcher.run(146, 240, 40, S200);
        batcher.run(241, 270, 20, S200);
        batcher.run(271, 281, 10, S200);
        batcher.run(282, 287, 05, S050);
        batcher.run(288, 298, 10, S050);
        batcher.run(299, 309, 20, S050);
        batcher.run(310, 325, 40, S100);
        batcher.run(326, 336, 40, S100);
        batcher.run(337, 347, 40, S100);
        batcher.run(348, 357, 40, S100);
        batcher.run(358, 359, 40, S010);
        assert!(batcher.finished());

        batcher.at(360, 359, 80);
        batcher.step(360, 359, S010);
    }

    #[test]
    fn vid_batcher_adjusts_bounds() {
        // The first and last entry in `bounds` are estimats of the min and
        // max that are slightly off compared to the actual min and max we
        // put in `vid_range`. Check that `VidBatcher` uses the actual min
        // and max from `vid_range`.
        let bounds = vec![639, 20_000, 40_000, 60_000, 80_000, 90_000];
        let vid_range = VidRange::new(1, 100_000);
        let batch_size = AdaptiveBatchSize {
            size: 1000,
            target: S100,
        };

        let vid_batcher = VidBatcher::new(bounds, vid_range, batch_size).unwrap();
        let ogive = vid_batcher.ogive.as_ref().unwrap();
        assert_eq!(1, ogive.start());
        assert_eq!(100_000, ogive.end());
    }
}
