use std::ops::RangeInclusive;

use crate::{internal_error, prelude::StoreError};

/// A helper to deal with cumulative histograms, also known as ogives. This
/// implementation is restricted to histograms where each bin has the same
/// size. As a cumulative function of a histogram, an ogive is a piecewise
/// linear function `f` and since it is strictly monotonically increasing,
/// it has an inverse `g`.
///
/// For the given `points`, `f(points[i]) = i * bin_size` and `f` is the
/// piecewise linear interpolant between those points. The inverse `g` is
/// the piecewise linear interpolant of `g(i * bin_size) = points[i]`. Note
/// that that means that `f` divides the y-axis into `points.len()` equal
/// parts.
///
/// The word 'ogive' is somewhat obscure, but has a lot fewer letters than
/// 'piecewise linear function'. Copolit also claims that it is also a lot
/// more fun to say.
pub struct Ogive {
    /// The breakpoints of the piecewise linear function
    points: Vec<i64>,
    /// The size of each bin; the linear piece from `points[i]` to
    /// `points[i+1]` rises by this much
    bin_size: f64,
    /// The range of the ogive, i.e., the minimum and maximum entries from
    /// points
    range: RangeInclusive<i64>,
}

impl Ogive {
    /// Create an ogive from a histogram with breaks at the given points and
    /// a total count of `total` entries. As a function, the ogive is 0 at
    /// `points[0]` and `total` at `points[points.len() - 1]`.
    ///
    /// The `points` must have at least one entry. The `points` are sorted
    /// and deduplicated, i.e., they don't have to be in ascending order.
    pub fn from_equi_histogram(mut points: Vec<i64>, total: usize) -> Result<Self, StoreError> {
        if points.is_empty() {
            return Err(internal_error!("histogram must have at least one point"));
        }

        points.sort_unstable();
        points.dedup();

        let bins = points.len() - 1;
        let bin_size = total as f64 / bins as f64;
        let range = points[0]..=points[bins];
        Ok(Self {
            points,
            bin_size,
            range,
        })
    }

    pub fn start(&self) -> i64 {
        *self.range.start()
    }

    pub fn end(&self) -> i64 {
        *self.range.end()
    }

    /// Find the next point `next` such that there are `size` entries
    /// between `point` and `next`, i.e., such that `f(next) - f(point) =
    /// size`.
    ///
    /// It is an error if `point` is smaller than `points[0]`. If `point` is
    /// bigger than `points.last()`, that is returned instead.
    ///
    /// The method calculates `g(f(point) + size)`
    pub fn next_point(&self, point: i64, size: usize) -> Result<i64, StoreError> {
        if point >= *self.range.end() {
            return Ok(*self.range.end());
        }
        // This can only fail if point < self.range.start
        self.check_in_range(point)?;

        let point_value = self.value(point)?;
        let next_value = point_value + size as i64;
        let next_point = self.inverse(next_value)?;
        Ok(next_point)
    }

    /// Return the index of the support point immediately preceding `point`.
    /// It is an error if `point` is outside the range of points of this
    /// ogive; this also implies that the returned index is always strictly
    /// less than `self.points.len() - 1`
    fn interval_start(&self, point: i64) -> Result<usize, StoreError> {
        self.check_in_range(point)?;

        let idx = self
            .points
            .iter()
            .position(|&p| point < p)
            .unwrap_or(self.points.len() - 1)
            - 1;
        Ok(idx)
    }

    /// Return the value of the ogive at `point`, i.e., `f(point)`. It is an
    /// error if `point` is outside the range of points of this ogive.
    ///
    /// If `i` is such that
    /// `points[i] <= point < points[i+1]`, then
    /// ```text
    ///   f(point) = i * bin_size + (point - points[i]) / (points[i+1] - points[i]) * bin_size
    /// ```
    // See the comment on `inverse` for numerical considerations
    fn value(&self, point: i64) -> Result<i64, StoreError> {
        if self.points.len() == 1 {
            return Ok(*self.range.end());
        }

        let idx = self.interval_start(point)?;
        let (a, b) = (self.points[idx], self.points[idx + 1]);
        let offset = (point - a) as f64 / (b - a) as f64;
        let value = (idx as f64 + offset) * self.bin_size;
        Ok(value as i64)
    }

    /// Return the value of the inverse ogive at `value`, i.e., `g(value)`.
    /// It is an error if `value` is negative. If `value` is greater than
    /// the total count of the ogive, the maximum point of the ogive is
    /// returned.
    ///
    /// For `points[j] <= v < points[j+1]`, the value of `g(v)` is
    /// ```text
    ///  g(v) = (1-lambda)*points[j] + lambda * points[j+1]
    /// ```
    /// where `lambda = (v - j * bin_size) / bin_size`
    ///
    // Note that in the definition of `lambda`, the numerator is
    // `v.rem_euclid(bin_size)`
    //
    // Numerical consideration: in these calculations, we need to be careful
    // to never convert one of the points directly to f64 since they can be
    // so large that the conversion from i64 to f64 loses precision. That
    // loss of precision can cause the convex combination of `points[j]` and
    // `points[j+1]` above to lie outside of that interval when `(points[j]
    // as f64) as i64 < points[j]`
    //
    // We therefore try to only convert differences between points to f64
    // which are much smaller.
    fn inverse(&self, value: i64) -> Result<i64, StoreError> {
        if value < 0 {
            return Err(internal_error!("value {} can not be negative", value));
        }
        let j = (value / self.bin_size as i64) as usize;
        if j >= self.points.len() - 1 {
            return Ok(*self.range.end());
        }
        let (a, b) = (self.points[j], self.points[j + 1]);
        // This is the same calculation as in the comment above, but
        // rewritten to be more friendly to lossy calculations with f64
        let offset = (value as f64).rem_euclid(self.bin_size) * (b - a) as f64;
        let x = a + (offset / self.bin_size) as i64;
        Ok(x)
    }

    fn check_in_range(&self, point: i64) -> Result<(), StoreError> {
        if !self.range.contains(&point) {
            return Err(internal_error!(
                "point {} is outside of the range [{}, {}]",
                point,
                self.range.start(),
                self.range.end(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        // This is just the linear function y = (70 / 5) * (x - 10)
        let points: Vec<i64> = vec![10, 20, 30, 40, 50, 60];
        let ogive = Ogive::from_equi_histogram(points, 700).unwrap();

        // The function represented by `points`
        fn f(x: i64) -> i64 {
            70 * (x - 10) / 5
        }

        // The inverse of `f`
        fn g(x: i64) -> i64 {
            x * 5 / 70 + 10
        }

        // Check that the ogive is correct
        assert_eq!(ogive.bin_size, 700_f64 / 5_f64);
        assert_eq!(ogive.range, 10..=60);

        // Test value method
        for point in [20, 30, 45, 50, 60] {
            assert_eq!(ogive.value(point).unwrap(), f(point), "value for {}", point);
        }

        // Test next_point method
        for step in [50, 140, 200] {
            for value in [10, 20, 30, 35, 45, 50, 60] {
                assert_eq!(
                    ogive.next_point(value, step).unwrap(),
                    g(f(value) + step as i64).min(60),
                    "inverse for {} with step {}",
                    value,
                    step
                );
            }
        }

        // Exceeding the range caps it at the maximum point
        assert_eq!(ogive.next_point(50, 140).unwrap(), 60);
        assert_eq!(ogive.next_point(50, 500).unwrap(), 60);

        // Point to the left of the range should return an error
        assert!(ogive.next_point(9, 140).is_err());
        // Point to the right of the range gets capped
        assert_eq!(ogive.next_point(61, 140).unwrap(), 60);
    }

    #[test]
    fn single_bin() {
        // A histogram with only one bin
        let points: Vec<i64> = vec![10, 20];
        let ogive = Ogive::from_equi_histogram(points, 700).unwrap();

        // The function represented by `points`
        fn f(x: i64) -> i64 {
            700 * (x - 10) / 10
        }

        // The inverse of `f`
        fn g(x: i64) -> i64 {
            x * 10 / 700 + 10
        }

        // Check that the ogive is correct
        assert_eq!(ogive.bin_size, 700_f64 / 1_f64);
        assert_eq!(ogive.range, 10..=20);

        // Test value method
        for point in [10, 15, 20] {
            assert_eq!(ogive.value(point).unwrap(), f(point), "value for {}", point);
        }

        // Test next_point method
        for step in [50, 140, 200] {
            for value in [10, 15, 20] {
                assert_eq!(
                    ogive.next_point(value, step).unwrap(),
                    g(f(value) + step as i64).min(20),
                    "inverse for {} with step {}",
                    value,
                    step
                );
            }
        }

        // Exceeding the range caps it at the maximum point
        assert_eq!(ogive.next_point(20, 140).unwrap(), 20);
        assert_eq!(ogive.next_point(20, 500).unwrap(), 20);

        // Point to the left of the range should return an error
        assert!(ogive.next_point(9, 140).is_err());
        // Point to the right of the range gets capped
        assert_eq!(ogive.next_point(21, 140).unwrap(), 20);
    }

    #[test]
    fn one_bin() {
        let points: Vec<i64> = vec![10];
        let ogive = Ogive::from_equi_histogram(points, 700).unwrap();

        assert_eq!(ogive.next_point(10, 1).unwrap(), 10);
        assert_eq!(ogive.next_point(10, 4).unwrap(), 10);
        assert_eq!(ogive.next_point(15, 1).unwrap(), 10);

        assert!(ogive.next_point(9, 1).is_err());
    }

    #[test]
    fn exponential() {
        let points: Vec<i64> = vec![32, 48, 56, 60, 62, 64];
        let ogive = Ogive::from_equi_histogram(points, 100).unwrap();

        assert_eq!(ogive.value(50).unwrap(), 25);
        assert_eq!(ogive.value(56).unwrap(), 40);
        assert_eq!(ogive.value(58).unwrap(), 50);
        assert_eq!(ogive.value(63).unwrap(), 90);

        assert_eq!(ogive.next_point(32, 40).unwrap(), 56);
        assert_eq!(ogive.next_point(50, 10).unwrap(), 54);
        assert_eq!(ogive.next_point(50, 50).unwrap(), 61);
        assert_eq!(ogive.next_point(40, 40).unwrap(), 58);
    }
}
