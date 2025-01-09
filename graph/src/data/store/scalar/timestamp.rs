use chrono::{DateTime, Utc};
use diesel::serialize::ToSql;
use serde::{self, Deserialize, Serialize};
use stable_hash::StableHash;

use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;

use crate::derive::CacheWeight;
use crate::runtime::gas::{Gas, GasSizeOf, SaturatingInto};

#[derive(
    Clone, Copy, CacheWeight, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub struct Timestamp(pub DateTime<Utc>);

#[derive(thiserror::Error, Debug)]
pub enum TimestampError {
    #[error("Invalid timestamp string: {0}")]
    StringParseError(ParseIntError),
    #[error("Invalid timestamp format")]
    InvalidTimestamp,
}

impl Timestamp {
    /// A timestamp from a long long time ago used to indicate that we don't
    /// have a timestamp
    pub const NONE: Self = Self(DateTime::<Utc>::MIN_UTC);

    pub const MAX: Self = Self(DateTime::<Utc>::MAX_UTC);

    pub const MIN: Self = Self(DateTime::<Utc>::MIN_UTC);

    pub fn parse_timestamp(v: &str) -> Result<Self, TimestampError> {
        let as_num: i64 = v.parse().map_err(TimestampError::StringParseError)?;
        Timestamp::from_microseconds_since_epoch(as_num)
    }

    pub fn from_rfc3339(v: &str) -> Result<Self, chrono::ParseError> {
        Ok(Timestamp(DateTime::parse_from_rfc3339(v)?.into()))
    }

    pub fn from_microseconds_since_epoch(micros: i64) -> Result<Self, TimestampError> {
        let secs = micros / 1_000_000;
        let ns = (micros % 1_000_000) * 1_000;

        match DateTime::from_timestamp(secs, ns as u32) {
            Some(dt) => Ok(Self(dt)),
            None => Err(TimestampError::InvalidTimestamp),
        }
    }

    pub fn as_microseconds_since_epoch(&self) -> i64 {
        self.0.timestamp_micros()
    }

    pub fn since_epoch(secs: i64, nanos: u32) -> Option<Self> {
        DateTime::from_timestamp(secs, nanos).map(|dt| Timestamp(dt))
    }

    pub fn as_secs_since_epoch(&self) -> i64 {
        self.0.timestamp()
    }

    pub(crate) fn timestamp_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }
}

impl StableHash for Timestamp {
    fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        self.0.timestamp_micros().stable_hash(field_address, state)
    }
}

impl stable_hash_legacy::StableHash for Timestamp {
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        sequence_number: H::Seq,
        state: &mut H,
    ) {
        stable_hash_legacy::StableHash::stable_hash(
            &self.0.timestamp_micros(),
            sequence_number,
            state,
        )
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.as_microseconds_since_epoch())
    }
}

impl ToSql<diesel::sql_types::Timestamptz, diesel::pg::Pg> for Timestamp {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        <_ as ToSql<diesel::sql_types::Timestamptz, _>>::to_sql(&self.0, &mut out.reborrow())
    }
}

impl GasSizeOf for Timestamp {
    fn const_gas_size_of() -> Option<Gas> {
        Some(Gas::new(std::mem::size_of::<Timestamp>().saturating_into()))
    }
}
