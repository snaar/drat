use std::error::Error as StdError;
use chrono::DateTime;

use crate::chopper::types::Nanos;
use crate::error::{CliResult, Error};

pub static DEFAULT_MONTH: &str = "01";
pub static DEFAULT_DAY: &str = "01";
pub static DEFAULT_TIME: &str = "00:00:00";
pub static DEFAULT_ZONE: &str = "+0000";
pub static DEFAULT_DATE_FORMAT: &str = "%Y%m%d";
pub static DEFAULT_TIME_FORMAT: &str = "%H:%M:%S";
pub static DEFAULT_ZONE_FORMAT: &str = "%z";

// list of timestamp formats
lazy_static! {
    pub static ref TIMESTAMP_FORMATS: Vec<String> = create_timestamp_formats();
}

fn create_timestamp_formats() -> Vec<String> {
    let format = vec![
        format!("{}-{} {}", DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_ZONE_FORMAT),
        format!("{},{} {}", DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_ZONE_FORMAT),
        format!("{}/{} {}", DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_ZONE_FORMAT),
    ];
    format
}

pub fn parse_into_nanos_from_str(timestamp: &str, format: &str) -> CliResult<Nanos> {
    match DateTime::parse_from_str(timestamp, format) {
        Ok(t) => Ok(t.timestamp_nanos() as Nanos),
        Err(e) => {
            return Err(Error::from(e.description()))
        }
    }
}

pub fn parse_date_range_timestamp(mut date_time: String, time_zone: &str) -> CliResult<Nanos> {
    // if time is not specified
    if date_time.len() <= 8 {
        date_time = match date_time.len() {
            4 => format!("{}{}{}", date_time, DEFAULT_MONTH, DEFAULT_DAY), // year only
            6 => format!("{}{}", date_time, DEFAULT_DAY), // year month only
            _ => date_time // year month day
        };
        date_time = format!("{}-{}", date_time, DEFAULT_TIME);
    }
    let timestamp = format!("{} {}", date_time, time_zone);

    let formats = TIMESTAMP_FORMATS.clone();
    for f in formats {
        let nanos = DateTime::parse_from_str(timestamp.as_ref(), f.as_str());
        if nanos.is_ok() {
            return Ok(nanos.unwrap().timestamp() as Nanos)
        }
    }
    Err(Error::from(format!("Cannot parse timestamp: {}. Please check the format.", timestamp)))
}

pub fn parse_time_zone(time_zone: Option<&str>) -> String {
    let time_zone = match time_zone {
        Some(z) => {
            let zone = match z.to_lowercase().as_str() {
                "utc" => "+0000",
                "ny" => "-0500",
                _ => unreachable!()
            };
            String::from(zone)
        }
        None => DEFAULT_ZONE.to_string()
    };
    time_zone
}
