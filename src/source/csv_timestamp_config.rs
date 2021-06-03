use crate::util::timestamp_units::TimestampUnits;
use crate::util::tz::ChopperTz;

pub type DateColIdx = usize;
pub type TimeColIdx = usize;

#[derive(Debug, Clone)]
pub enum TimestampFmtConfig {
    Auto,
    Epoch(TimestampUnits),
    Explicit(String),
    DateTimeExplicit(String, String),
}

#[derive(Debug, Clone)]
pub enum TimestampColConfig {
    Auto,
    Index(usize),
    DateTimeIndex(DateColIdx, TimeColIdx),
    Name(String),
    DateTimeName(String, String),
}

#[derive(Debug, Clone)]
pub struct TimestampConfig {
    timestamp_col: TimestampColConfig,
    timestamp_fmt: TimestampFmtConfig,
    timezone: ChopperTz,
}

impl TimestampConfig {
    pub fn new(
        timestamp_col: TimestampColConfig,
        timestamp_fmt: TimestampFmtConfig,
        timezone: ChopperTz,
    ) -> Self {
        TimestampConfig {
            timestamp_col,
            timestamp_fmt,
            timezone,
        }
    }

    pub fn timestamp_col(&self) -> &TimestampColConfig {
        &self.timestamp_col
    }

    pub fn timestamp_fmt(&self) -> &TimestampFmtConfig {
        &self.timestamp_fmt
    }

    pub fn timezone(&self) -> &ChopperTz {
        &self.timezone
    }
}
