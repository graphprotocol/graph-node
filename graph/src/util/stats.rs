use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use portable_atomic::AtomicU128;
use prometheus::Gauge;

use crate::prelude::ENV_VARS;

/// One bin of durations. The bin starts at time `start`, and we've added `count`
/// entries to it whose durations add up to `duration`
struct Bin {
    start: Instant,
    duration: Duration,
    count: u32,
}

impl Bin {
    fn new(start: Instant) -> Self {
        Self {
            start,
            duration: Duration::from_millis(0),
            count: 0,
        }
    }

    /// Add a new measurement to the bin
    fn add(&mut self, duration: Duration) {
        self.count += 1;
        self.duration += duration;
    }

    /// Remove the measurements for `other` from this bin. Only used to
    /// keep a running total of measurements in `MovingStats`
    fn remove(&mut self, other: &Bin) {
        self.count -= other.count;
        self.duration -= other.duration;
    }

    /// Return `true` if the average of measurements in this bin is above
    /// `duration`
    fn average_gt(&self, duration: Duration) -> bool {
        // Compute self.duration / self.count > duration as
        // self.duration > duration * self.count. If the RHS
        // overflows, we assume the average would have been smaller
        // than any duration
        duration
            .checked_mul(self.count)
            .map(|rhs| self.duration > rhs)
            .unwrap_or(false)
    }
}

/// Collect statistics over a moving window of size `window_size`. To keep
/// the amount of memory needed to store the values inside the window
/// constant, values are put into bins of size `bin_size`. For example, using
/// a `window_size` of 5 minutes and a bin size of one second would use
/// 300 bins. Each bin has constant size
pub struct MovingStats {
    pub window_size: Duration,
    pub bin_size: Duration,
    /// The buffer with measurements. The back has the most recent entries,
    /// and the front has the oldest entries
    bins: VecDeque<Bin>,
    /// Sum over the values in `elements` The `start` of this bin
    /// is meaningless
    total: Bin,
}

/// Create `MovingStats` that use the window and bin sizes configured in
/// the environment
impl Default for MovingStats {
    fn default() -> Self {
        Self::new(ENV_VARS.load_window_size, ENV_VARS.load_bin_size)
    }
}

impl MovingStats {
    /// Track moving statistics over a window of `window_size` duration
    /// and keep the measurements in bins of `bin_size` each.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` or `bin_size` is `0`, or if `bin_size` >=
    /// `window_size`
    pub fn new(window_size: Duration, bin_size: Duration) -> Self {
        assert!(window_size.as_millis() > 0);
        assert!(bin_size.as_millis() > 0);
        assert!(window_size > bin_size);

        let capacity = window_size.as_millis() as usize / bin_size.as_millis() as usize;

        MovingStats {
            window_size,
            bin_size,
            bins: VecDeque::with_capacity(capacity),
            total: Bin::new(Instant::now()),
        }
    }

    /// Return `true` if the average of measurements in within `window_size`
    /// is above `duration`
    pub fn average_gt(&self, duration: Duration) -> bool {
        // Depending on how often add() is called, we should
        // call expire_bins first, but that would require taking a
        // `&mut self`
        self.total.average_gt(duration)
    }

    /// Return the average over the current window in milliseconds
    pub fn average(&self) -> Option<Duration> {
        self.total.duration.checked_div(self.total.count)
    }

    pub fn add(&mut self, duration: Duration) {
        self.add_at(Instant::now(), duration);
    }

    /// Add an entry with the given timestamp. Note that the entry will
    /// still be added either to the current latest bin or a new
    /// latest bin. It is expected that subsequent calls to `add_at` still
    /// happen with monotonically increasing `now` values. If the `now`
    /// values do not monotonically increase, the average calculation
    /// becomes imprecise because values are expired later than they
    /// should be.
    pub fn add_at(&mut self, now: Instant, duration: Duration) {
        let need_new_bin = self
            .bins
            .back()
            .map(|bin| now.saturating_duration_since(bin.start) >= self.bin_size)
            .unwrap_or(true);
        if need_new_bin {
            self.bins.push_back(Bin::new(now));
        }
        self.expire_bins(now);
        // unwrap is fine because we just added a bin if there wasn't one
        // before
        let bin = self.bins.back_mut().unwrap();
        bin.add(duration);
        self.total.add(duration);
    }

    fn expire_bins(&mut self, now: Instant) {
        while self
            .bins
            .front()
            .map(|existing| now.saturating_duration_since(existing.start) >= self.window_size)
            .unwrap_or(false)
        {
            if let Some(existing) = self.bins.pop_front() {
                self.total.remove(&existing);
            }
        }
    }

    pub fn duration(&self) -> Duration {
        self.total.duration
    }

    /// Adds `duration` to the stats, and register the average ms to `avg_gauge`.
    pub fn add_and_register(&mut self, duration: Duration, avg_gauge: &Gauge) {
        let wait_avg = {
            self.add(duration);
            self.average()
        };
        let wait_avg = wait_avg.map(|wait_avg| wait_avg.as_millis()).unwrap_or(0);
        avg_gauge.set(wait_avg as f64);
    }
}

/// Packed bin for atomic operations: epoch (32 bits) | count (32 bits) | duration_nanos (64 bits)
/// Fits in a single AtomicU128 for lock-free CAS updates.
#[repr(transparent)]
struct PackedBin(AtomicU128);

impl PackedBin {
    fn new() -> Self {
        Self(AtomicU128::new(0))
    }

    /// Pack epoch, count, and duration into a single u128 value.
    fn pack(epoch: u32, count: u32, duration_nanos: u64) -> u128 {
        ((epoch as u128) << 96) | ((count as u128) << 64) | (duration_nanos as u128)
    }

    /// Unpack a u128 value into (epoch, count, duration_nanos).
    fn unpack(packed: u128) -> (u32, u32, u64) {
        let epoch = (packed >> 96) as u32;
        let count = (packed >> 64) as u32;
        let duration_nanos = packed as u64;
        (epoch, count, duration_nanos)
    }
}

/// Lock-free moving statistics using an epoch-based ring buffer.
///
/// This is a thread-safe, lock-free alternative to `MovingStats` that uses
/// atomic operations instead of locks. It tracks durations over a sliding
/// time window, storing values in fixed-size bins.
///
/// Writers use CAS loops to atomically update bins, while readers can
/// scan all bins without blocking writers.
pub struct AtomicMovingStats {
    start_time: Instant,
    bin_size: Duration,
    bins: Box<[PackedBin]>,
}

impl Default for AtomicMovingStats {
    fn default() -> Self {
        Self::new(ENV_VARS.load_window_size, ENV_VARS.load_bin_size)
    }
}

impl AtomicMovingStats {
    /// Create a new AtomicMovingStats with the given window and bin sizes.
    ///
    /// # Panics
    ///
    /// Panics if `window_size` or `bin_size` is `0`, or if `bin_size` >= `window_size`
    pub fn new(window_size: Duration, bin_size: Duration) -> Self {
        assert!(window_size.as_millis() > 0);
        assert!(bin_size.as_millis() > 0);
        assert!(window_size > bin_size);

        let num_bins = window_size.as_millis() as usize / bin_size.as_millis() as usize;
        let bins: Vec<PackedBin> = (0..num_bins).map(|_| PackedBin::new()).collect();

        Self {
            start_time: Instant::now(),
            bin_size,
            bins: bins.into_boxed_slice(),
        }
    }

    /// Calculate the epoch number for a given instant.
    fn epoch_at(&self, now: Instant) -> u32 {
        let elapsed = now.saturating_duration_since(self.start_time);
        (elapsed.as_millis() / self.bin_size.as_millis()) as u32
    }

    /// Add a duration measurement at the current time.
    pub fn add(&self, duration: Duration) {
        self.add_at(Instant::now(), duration);
    }

    /// Add a duration measurement at a specific time.
    ///
    /// Note: It is expected that subsequent calls to `add_at` happen with
    /// monotonically increasing `now` values for optimal accuracy.
    pub fn add_at(&self, now: Instant, duration: Duration) {
        let current_epoch = self.epoch_at(now);
        let bin_idx = current_epoch as usize % self.bins.len();
        let bin = &self.bins[bin_idx];
        let duration_nanos = duration.as_nanos() as u64;

        loop {
            let current = bin.0.load(Ordering::Acquire);
            let (bin_epoch, count, total_nanos) = PackedBin::unpack(current);

            let new_packed = if bin_epoch == current_epoch {
                // Same epoch: increment existing values
                PackedBin::pack(
                    current_epoch,
                    count.saturating_add(1),
                    total_nanos.saturating_add(duration_nanos),
                )
            } else {
                // Stale epoch: reset bin with new measurement
                PackedBin::pack(current_epoch, 1, duration_nanos)
            };

            match bin.0.compare_exchange_weak(
                current,
                new_packed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => continue, // CAS failed, retry
            }
        }
    }

    /// Calculate the average duration over the current window.
    ///
    /// Returns `None` if no measurements have been recorded in the window.
    pub fn average(&self) -> Option<Duration> {
        self.average_at(Instant::now())
    }

    /// Calculate the average duration at a specific time.
    fn average_at(&self, now: Instant) -> Option<Duration> {
        let current_epoch = self.epoch_at(now);
        let num_bins = self.bins.len() as u32;
        let mut total_count: u64 = 0;
        let mut total_nanos: u128 = 0;

        for bin in self.bins.iter() {
            let (bin_epoch, count, duration_nanos) =
                PackedBin::unpack(bin.0.load(Ordering::Acquire));
            // Valid if within window (handles epoch wraparound)
            if current_epoch.wrapping_sub(bin_epoch) < num_bins {
                total_count += count as u64;
                total_nanos += duration_nanos as u128;
            }
        }

        if total_count > 0 {
            Some(Duration::from_nanos(
                (total_nanos / total_count as u128) as u64,
            ))
        } else {
            None
        }
    }

    /// Return `true` if the average of measurements within the window
    /// is above `duration`.
    pub fn average_gt(&self, duration: Duration) -> bool {
        self.average().map(|avg| avg > duration).unwrap_or(false)
    }

    /// Return `true` if the average at a specific time is above `duration`.
    #[cfg(test)]
    fn average_gt_at(&self, now: Instant, duration: Duration) -> bool {
        self.average_at(now)
            .map(|avg| avg > duration)
            .unwrap_or(false)
    }

    /// Return the total duration recorded in the current window.
    pub fn duration(&self) -> Duration {
        self.duration_at(Instant::now())
    }

    /// Return the total duration at a specific time.
    fn duration_at(&self, now: Instant) -> Duration {
        let current_epoch = self.epoch_at(now);
        let num_bins = self.bins.len() as u32;
        let mut total_nanos: u128 = 0;

        for bin in self.bins.iter() {
            let (bin_epoch, _, duration_nanos) = PackedBin::unpack(bin.0.load(Ordering::Acquire));
            if current_epoch.wrapping_sub(bin_epoch) < num_bins {
                total_nanos += duration_nanos as u128;
            }
        }

        Duration::from_nanos(total_nanos as u64)
    }

    /// Adds `duration` to the stats, and register the average ms to `avg_gauge`.
    pub fn add_and_register(&self, duration: Duration, avg_gauge: &Gauge) {
        self.add(duration);
        let wait_avg = self.average().map(|avg| avg.as_millis()).unwrap_or(0);
        avg_gauge.set(wait_avg as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[allow(dead_code)]
    fn dump_bin(msg: &str, bin: &Bin, start: Instant) {
        println!(
            "bin[{}]: age={}ms count={} duration={}ms",
            msg,
            bin.start.saturating_duration_since(start).as_millis(),
            bin.count,
            bin.duration.as_millis()
        );
    }

    #[test]
    fn add_one_const() {
        let mut stats = MovingStats::new(Duration::from_secs(5), Duration::from_secs(1));
        let start = Instant::now();
        for i in 0..10 {
            stats.add_at(start + Duration::from_secs(i), Duration::from_secs(1));
        }
        assert_eq!(5, stats.bins.len());
        for (i, bin) in stats.bins.iter().enumerate() {
            assert_eq!(1, bin.count);
            assert_eq!(Duration::from_secs(1), bin.duration);
            assert_eq!(Duration::from_secs(i as u64 + 5), (bin.start - start));
        }
        assert_eq!(5, stats.total.count);
        assert_eq!(Duration::from_secs(5), stats.total.duration);
        assert!(stats.average_gt(Duration::from_millis(900)));
        assert!(!stats.average_gt(Duration::from_secs(1)));
    }

    #[test]
    fn add_four_linear() {
        let mut stats = MovingStats::new(Duration::from_secs(5), Duration::from_secs(1));
        let start = Instant::now();
        for i in 0..40 {
            stats.add_at(
                start + Duration::from_millis(250 * i),
                Duration::from_secs(i),
            );
        }
        assert_eq!(5, stats.bins.len());
        for (b, bin) in stats.bins.iter().enumerate() {
            assert_eq!(4, bin.count);
            assert_eq!(Duration::from_secs(86 + 16 * b as u64), bin.duration);
        }
        assert_eq!(20, stats.total.count);
        assert_eq!(Duration::from_secs(5 * 86 + 16 * 10), stats.total.duration);
    }

    #[test]
    fn atomic_add_one_const() {
        let stats = AtomicMovingStats::new(Duration::from_secs(5), Duration::from_secs(1));
        let start = stats.start_time;
        for i in 0..10 {
            stats.add_at(start + Duration::from_secs(i), Duration::from_secs(1));
        }
        // After 10 seconds with 5-second window, only last 5 entries are valid
        assert_eq!(5, stats.bins.len());
        // Query at time 10 seconds (end of data range)
        let query_time = start + Duration::from_secs(10);
        // Average should be 1 second
        let avg = stats.average_at(query_time).unwrap();
        assert_eq!(Duration::from_secs(1), avg);
        assert!(stats.average_gt_at(query_time, Duration::from_millis(900)));
        assert!(!stats.average_gt_at(query_time, Duration::from_secs(1)));
    }

    #[test]
    fn atomic_add_four_linear() {
        let stats = AtomicMovingStats::new(Duration::from_secs(5), Duration::from_secs(1));
        let start = stats.start_time;
        for i in 0..40u64 {
            stats.add_at(
                start + Duration::from_millis(250 * i),
                Duration::from_secs(i),
            );
        }
        assert_eq!(5, stats.bins.len());
        // Query at time 9.999 seconds (just before epoch 10 to include epoch 5)
        // At epoch 9, valid bins are epochs 5-9 (9 - bin_epoch < 5)
        let query_time = start + Duration::from_millis(9999);
        // Total duration in window: 4 entries per bin, bins 5-9 contain entries 20-39
        // Bin 5: entries 20,21,22,23 -> sum = 86
        // Bin 6: entries 24,25,26,27 -> sum = 102
        // ...
        // Total count = 20, total duration = 5*86 + 16*10 = 590
        assert_eq!(
            Duration::from_secs(5 * 86 + 16 * 10),
            stats.duration_at(query_time)
        );
    }

    #[test]
    fn atomic_empty_average() {
        let stats = AtomicMovingStats::new(Duration::from_secs(5), Duration::from_secs(1));
        // Query at start_time (no data yet)
        assert!(stats.average_at(stats.start_time).is_none());
        assert!(!stats.average_gt_at(stats.start_time, Duration::from_secs(1)));
    }

    #[test]
    fn atomic_pack_unpack() {
        // Test edge cases of packing/unpacking
        let packed = PackedBin::pack(u32::MAX, u32::MAX, u64::MAX);
        let (epoch, count, nanos) = PackedBin::unpack(packed);
        assert_eq!(u32::MAX, epoch);
        assert_eq!(u32::MAX, count);
        assert_eq!(u64::MAX, nanos);

        let packed = PackedBin::pack(0, 0, 0);
        let (epoch, count, nanos) = PackedBin::unpack(packed);
        assert_eq!(0, epoch);
        assert_eq!(0, count);
        assert_eq!(0, nanos);

        let packed = PackedBin::pack(12345, 67890, 123456789012345);
        let (epoch, count, nanos) = PackedBin::unpack(packed);
        assert_eq!(12345, epoch);
        assert_eq!(67890, count);
        assert_eq!(123456789012345, nanos);
    }
}
