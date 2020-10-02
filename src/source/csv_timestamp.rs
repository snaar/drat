use std::ops::Range;

use chrono::{DateTime, NaiveDate, NaiveDateTime};

use crate::chopper::types::{Header, Nanos};
use crate::error::{CliResult, Error};
use crate::source::csv_configs::{DateColIdx, TimeColIdx, TimestampColConfig, TimestampFmtConfig};
use crate::util::tz::ChopperTz;

// https://docs.rs/chrono/0.4/chrono/format/strftime/index.html
const TIME_FORMATS_WITH_TZ: [&'static str; 1] = ["%+"];
const TIME_FORMATS_WITHOUT_TZ: [&'static str; 4] = [
    "%Y%m%d.%H%M%S",
    "%Y%m%d %H%M%S",
    "%Y%m%d %H:%M:%S",
    "%Y/%m/%d-%H:%M:%S",
];
// the # character is used as separator between date and time column values when joining them
// note that we don't really have any good split-column formats that have TZ in them
const DATETIME_FORMATS_WITHOUT_TZ: [&'static str; 4] = [
    "%Y%m%d#%H%M%S%3f",
    "%Y%m%d#%H%M%S",
    "%Y%m%d#%H:%M:%S",
    "%Y%m%d#%-H:%M",
];
const DATE_FORMATS_WITHOUT_TZ: [&'static str; 1] = ["%Y%m%d"];

#[derive(Copy, Clone)]
pub enum TimestampUnits {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl TimestampUnits {
    pub fn to_suffix_str(&self) -> &str {
        match self {
            TimestampUnits::Seconds => "Seconds",
            TimestampUnits::Millis => "Millis",
            TimestampUnits::Micros => "Micros",
            TimestampUnits::Nanos => "Nanos",
        }
    }
}

const TIMESTAMP_UNITS: [(TimestampUnits, &'static str); 4] = [
    (TimestampUnits::Seconds, "seconds"),
    (TimestampUnits::Millis, "millis"),
    (TimestampUnits::Micros, "micros"),
    (TimestampUnits::Nanos, "nanos"),
];

// these limit autodetection to dates starting sep 2001 until 2033, which is mostly fine
const RANGE_NANOS: Range<u64> = 1_000_000_000_000_000_000..2_000_000_000_000_000_000;
const RANGE_MICROS: Range<u64> = 1_000_000_000_000_000..2_000_000_000_000_000;
const RANGE_MILLIS: Range<u64> = 1_000_000_000_000..2_000_000_000_000;
const RANGE_SECONDS: Range<u64> = 1_000_000_000..2_000_000_000;

pub enum TimestampCol {
    Index(usize),
    DateTimeIndex(DateColIdx, TimeColIdx),
}

pub enum TimestampFmt {
    Units(TimestampUnits),
    DateTimeIncludesTz(String),
    DateTimeNeedsExternalTz(String),
    DateNeedsExternalTz(String),
}

pub fn get_timestamp(
    record: &csv::StringRecord,
    timestamp_col: &TimestampCol,
    timestamp_fmt: &TimestampFmt,
    timezone: &ChopperTz,
) -> CliResult<Nanos> {
    let timestamp = get_timestamp_string(record, timestamp_col);

    let timestamp = match timestamp_fmt {
        TimestampFmt::Units(units) => {
            let timestamp: u64 = match timestamp.parse::<u64>() {
                Ok(ts) => ts,
                Err(err) => return Err(Error::Custom(err.to_string())),
            };
            match units {
                TimestampUnits::Seconds => timestamp * 1_000_000_000,
                TimestampUnits::Millis => timestamp * 1_000_000,
                TimestampUnits::Micros => timestamp * 1_000,
                TimestampUnits::Nanos => timestamp,
            }
        }
        TimestampFmt::DateTimeIncludesTz(format) => {
            DateTime::parse_from_str(&timestamp, format)?.timestamp_nanos() as u64
        }
        TimestampFmt::DateTimeNeedsExternalTz(format) => {
            let naive_date_time = NaiveDateTime::parse_from_str(&timestamp, format)?;
            timezone
                .from_local_datetime(&naive_date_time)?
                .timestamp_nanos() as u64
        }
        TimestampFmt::DateNeedsExternalTz(format) => {
            let naive_date = NaiveDate::parse_from_str(&timestamp, format)?;
            let naive_date_time = naive_date.and_hms(0, 0, 0);
            timezone
                .from_local_datetime(&naive_date_time)?
                .timestamp_nanos() as u64
        }
    };
    Ok(timestamp)
}

pub fn get_timestamp_col_and_fmt(
    header: &Header,
    first_row: &csv::StringRecord,
    timestamp_col_config: &TimestampColConfig,
    timestamp_fmt_config: &TimestampFmtConfig,
    timezone: &ChopperTz,
) -> CliResult<(TimestampCol, TimestampFmt)> {
    let (timestamp_col, units_hint) = get_timestamp_col(&header, timestamp_col_config)?;

    let timestamp_fmt = match timestamp_fmt_config {
        TimestampFmtConfig::Auto => {
            get_timestamp_fmt_auto(&timestamp_col, timezone, &first_row, units_hint)?
        }
        TimestampFmtConfig::Explicit(t) => {
            let mut fmt = get_timestamp_fmt_from_user_spec(
                first_row,
                &timestamp_col,
                t.to_owned(),
                timezone,
            )?;

            let t_lowered = t.to_lowercase();
            for (candidate_units_enum, candidate_units_str) in &TIMESTAMP_UNITS {
                if *candidate_units_str == t_lowered {
                    fmt = TimestampFmt::Units(*candidate_units_enum);
                    break;
                }
            }

            fmt
        }
        TimestampFmtConfig::DateTimeExplicit(d, t) => {
            let format = format!("{}#{}", d, t);
            get_timestamp_fmt_from_user_spec(first_row, &timestamp_col, format, timezone)?
        }
    };

    Ok((timestamp_col, timestamp_fmt))
}

fn get_timestamp_fmt_from_user_spec(
    first_row: &csv::StringRecord,
    timestamp_col: &TimestampCol,
    format: String,
    timezone: &ChopperTz,
) -> CliResult<TimestampFmt> {
    let timestamp = get_timestamp_string(first_row, timestamp_col);

    if let Ok(_) = DateTime::parse_from_str(&timestamp, &format) {
        Ok(TimestampFmt::DateTimeIncludesTz(format))
    } else if let Ok(naive_date_time) = NaiveDateTime::parse_from_str(&timestamp, &format) {
        timezone.from_local_datetime(&naive_date_time)?;
        Ok(TimestampFmt::DateTimeNeedsExternalTz(format))
    } else if let Ok(naive_date) = NaiveDate::parse_from_str(&timestamp, &format) {
        let naive_date_time = naive_date.and_hms(0, 0, 0);
        timezone.from_local_datetime(&naive_date_time)?;
        Ok(TimestampFmt::DateNeedsExternalTz(format))
    } else {
        Err(Error::from(format!(
            "Cannot figure out if timestamp format {} requires timezone or not \
            when trying to parse timestamp {}.",
            format, timestamp
        )))
    }
}

fn get_timestamp_string(record: &csv::StringRecord, timestamp_col: &TimestampCol) -> String {
    match timestamp_col {
        TimestampCol::Index(i) => record.get(*i).unwrap().to_string(),
        TimestampCol::DateTimeIndex(d, t) => {
            let date = record.get(*d).unwrap();
            let time = record.get(*t).unwrap();
            let mut timestamp = String::with_capacity(date.len() + 1 + time.len());
            timestamp.push_str(date);
            timestamp.push('#');
            timestamp.push_str(time);
            timestamp
        }
    }
}

fn get_timestamp_col(
    header: &Header,
    timestamp_col_config: &TimestampColConfig,
) -> CliResult<(TimestampCol, Option<TimestampUnits>)> {
    let (timestamp_col, units_hint) = match timestamp_col_config {
        TimestampColConfig::Auto => {
            // these are listed here in relative priority order as a reference
            let mut time_with_units: Option<(usize, TimestampUnits)> = None;
            let mut date: Option<usize> = None;
            let mut time: Option<usize> = None;
            let mut prefixed_time_with_units: Option<(String, usize, TimestampUnits)> = None;
            let mut prefixed_date: Option<(String, usize)> = None;
            let mut prefixed_time: Option<(String, usize)> = None;

            for (i, name) in header
                .field_names()
                .iter()
                .map(|s| s.to_lowercase())
                .enumerate()
                .rev()
            {
                match name.as_str() {
                    name if name == "time" => time = Some(i),
                    name if name == "date" => date = Some(i),
                    name if name.starts_with("time") => {
                        let units = &name[4..];
                        for (candidate_units_enum, candidate_units_str) in &TIMESTAMP_UNITS {
                            if *candidate_units_str == units {
                                time_with_units = Some((i, *candidate_units_enum));
                                break;
                            }
                        }
                    }
                    name if name.ends_with("date") => {
                        prefixed_date = Some((name[..name.len() - 4].to_string(), i))
                    }
                    name if name.ends_with("time") => {
                        prefixed_time = Some((name[..name.len() - 4].to_string(), i))
                    }
                    name if name.contains("time") => {
                        // we want to find the last one, so that we have chance at units suffix
                        let mut split_name = name.rsplitn(2, "time");
                        let suffix = split_name.next().unwrap();
                        for (candidate_units_enum, candidate_units_str) in &TIMESTAMP_UNITS {
                            if *candidate_units_str == suffix {
                                let prefix = split_name.next().unwrap();
                                prefixed_time_with_units =
                                    Some((prefix.to_owned(), i, *candidate_units_enum));
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }

            if let Some((i, units)) = time_with_units {
                (TimestampCol::Index(i), Some(units))
            } else if let Some(date_idx) = date {
                match time {
                    None => (TimestampCol::Index(date_idx), None),
                    Some(time_idx) => (TimestampCol::DateTimeIndex(date_idx, time_idx), None),
                }
            } else if let Some(i) = time {
                (TimestampCol::Index(i), None)
            } else if let Some((_prefix, i, units)) = prefixed_time_with_units {
                (TimestampCol::Index(i), Some(units))
            } else if let Some((date_prefix, date_idx)) = prefixed_date {
                match prefixed_time {
                    None => (TimestampCol::Index(date_idx), None),
                    Some((time_prefix, time_idx)) => {
                        if date_prefix == time_prefix {
                            (TimestampCol::DateTimeIndex(date_idx, time_idx), None)
                        } else {
                            (TimestampCol::Index(date_idx), None)
                        }
                    }
                }
            } else if let Some((_prefix, i)) = prefixed_time {
                (TimestampCol::Index(i), None)
            } else {
                // if can't find anything just try first column
                (TimestampCol::Index(0), None)
            }
        }
        TimestampColConfig::Index(i) => {
            let units = if let Some(name) = header.field_names().get(*i) {
                let mut units = None;
                for (candidate_units_enum, candidate_units_str) in &TIMESTAMP_UNITS {
                    if name.ends_with(candidate_units_str) {
                        units = Some(*candidate_units_enum);
                        break;
                    }
                }
                units
            } else {
                None
            };
            (TimestampCol::Index(*i), units)
        }
        TimestampColConfig::DateTimeIndex(d, t) => (TimestampCol::DateTimeIndex(*d, *t), None),
        TimestampColConfig::Name(name) => {
            let mut units = None;
            for (candidate_units_enum, candidate_units_str) in &TIMESTAMP_UNITS {
                if name.ends_with(candidate_units_str) {
                    units = Some(*candidate_units_enum);
                    break;
                }
            }
            (TimestampCol::Index(header.get_field_index(name)?), units)
        }
        TimestampColConfig::DateTimeName(d, t) => (
            TimestampCol::DateTimeIndex(header.get_field_index(d)?, header.get_field_index(t)?),
            None,
        ),
    };
    Ok((timestamp_col, units_hint))
}

fn get_timestamp_fmt_auto(
    timestamp_col: &TimestampCol,
    timezone: &ChopperTz,
    first_row: &csv::StringRecord,
    units_hint: Option<TimestampUnits>,
) -> CliResult<TimestampFmt> {
    match timestamp_col {
        TimestampCol::Index(t) => {
            get_timestamp_fmt_auto_with_index(*t, timezone, first_row, units_hint)
        }
        TimestampCol::DateTimeIndex(d, t) => {
            get_timestamp_fmt_auto_with_datetimeindex(*d, *t, timezone, first_row)
        }
    }
}

fn get_timestamp_fmt_auto_with_index(
    time_idx: usize,
    timezone: &ChopperTz,
    first_row: &csv::StringRecord,
    units_hint: Option<TimestampUnits>,
) -> CliResult<TimestampFmt> {
    if let Some(units) = units_hint {
        return Ok(TimestampFmt::Units(units));
    }

    let timestamp = first_row.get(time_idx).unwrap();

    for format in &TIME_FORMATS_WITH_TZ {
        if DateTime::parse_from_str(timestamp, format).is_ok() {
            return Ok(TimestampFmt::DateTimeIncludesTz(format.to_string()));
        };
    }

    for format in &TIME_FORMATS_WITHOUT_TZ {
        if let Ok(naive_date_time) = NaiveDateTime::parse_from_str(timestamp, format) {
            // we don't care about result, just want to sanity check that timezone is valid
            timezone.from_local_datetime(&naive_date_time)?;
            return Ok(TimestampFmt::DateTimeNeedsExternalTz(format.to_string()));
        };
    }

    for format in &DATE_FORMATS_WITHOUT_TZ {
        if let Ok(naive_date) = NaiveDate::parse_from_str(timestamp, format) {
            let naive_date_time = naive_date.and_hms(0, 0, 0);
            // we don't care about result, just want to sanity check that timezone is valid
            timezone.from_local_datetime(&naive_date_time)?;
            return Ok(TimestampFmt::DateNeedsExternalTz(format.to_string()));
        };
    }

    if timestamp.chars().all(|c| c.is_ascii_digit()) {
        let n = match timestamp.parse::<u64>() {
            Ok(ts) => ts,
            Err(err) => return Err(Error::Custom(err.to_string())),
        };
        if RANGE_NANOS.contains(&n) {
            return Ok(TimestampFmt::Units(TimestampUnits::Nanos));
        }
        if RANGE_MICROS.contains(&n) {
            return Ok(TimestampFmt::Units(TimestampUnits::Micros));
        }
        if RANGE_MILLIS.contains(&n) {
            return Ok(TimestampFmt::Units(TimestampUnits::Millis));
        }
        if RANGE_SECONDS.contains(&n) {
            return Ok(TimestampFmt::Units(TimestampUnits::Seconds));
        }
    };

    Err(Error::from(format!(
        "Cannot parse timestamp. Please provide format for parsing."
    )))
}

fn get_timestamp_fmt_auto_with_datetimeindex(
    date_idx: usize,
    time_idx: usize,
    timezone: &ChopperTz,
    first_row: &csv::StringRecord,
) -> CliResult<TimestampFmt> {
    let date = first_row.get(date_idx).unwrap();
    let time = first_row.get(time_idx).unwrap();

    let mut timestamp = String::with_capacity(date.len() + 1 + time.len());
    timestamp.push_str(date);
    timestamp.push('#');
    timestamp.push_str(time);
    let timestamp = &timestamp;

    for format in &DATETIME_FORMATS_WITHOUT_TZ {
        if let Ok(naive_date_time) = NaiveDateTime::parse_from_str(timestamp, format) {
            // we don't care about result, just want to sanity check that timezone is valid
            timezone.from_local_datetime(&naive_date_time)?;
            return Ok(TimestampFmt::DateTimeNeedsExternalTz(format.to_string()));
        };
    }

    Err(Error::from(format!(
        "Cannot parse timestamp. Please provide format for parsing."
    )))
}
