use chrono::NaiveDateTime;

use crate::chopper::types::Nanos;
use crate::error::{CliResult, Error};
use crate::util::tz::ChopperTz;

pub static DEFAULT_MONTH: &str = "01";
pub static DEFAULT_DAY: &str = "01";
pub static DEFAULT_TIME: &str = "00:00:00";

// list of timestamp formats
lazy_static! {
    pub static ref DATE_TIME_FORMATS: Vec<String> = create_date_time_formats();
}

fn create_date_time_formats() -> Vec<String> {
    let mut format: Vec<String> = Vec::new();
    let date_formats = vec![
        "%Y%m%d", "%m%d%Y", "%d%m%Y", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y",
    ];
    let delimiter = vec!["", "-", ",", "/"];
    for df in date_formats {
        for d in &delimiter {
            format.push(format!("{}{}{}", df, d, "%H:%M:%S"));
        }
    }
    format
}

pub fn parse_timestamp_range(timestamp: String, timezone: &ChopperTz) -> CliResult<Nanos> {
    let timestamp = complete_timestamp(timestamp);

    // try available datetime formats
    for fmt in DATE_TIME_FORMATS.iter() {
        // try parsing to naive datetime first
        let naive_dt = NaiveDateTime::parse_from_str(timestamp.as_ref(), fmt.as_ref());
        // if matching format is found, convert naive datetime to a timezone-aware datetime
        if naive_dt.is_ok() {
            return Ok(timezone
                .from_local_datetime(&naive_dt?)
                .unwrap()
                .timestamp() as Nanos);
        }
    }
    match timestamp.parse::<Nanos>() {
        Ok(n) => Ok(n),
        Err(_) => Err(Error::from(format!(
            "Cannot parse timestamp: {}. Please provide format for parsing.",
            timestamp
        ))),
    }
}

pub fn complete_timestamp(mut timestamp: String) -> String {
    // if time is not specified
    if timestamp.len() <= 8 {
        let date = match timestamp.len() {
            // add default month and/or day if not specified
            4 => format!("{}{}{}", timestamp, DEFAULT_MONTH, DEFAULT_DAY),
            6 => format!("{}{}", timestamp, DEFAULT_DAY),
            _ => timestamp,
        };
        // add default time
        timestamp = format!("{}{}", date, DEFAULT_TIME);
    }
    timestamp
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz::America::New_York;

    #[test]
    fn test_parse_timestamp_range() {
        let timezone = ChopperTz::from(New_York);
        let timestamp_year = parse_timestamp_range("2019".to_string(), &timezone).unwrap();
        let timestamp_datetime =
            parse_timestamp_range("20190101-00:00:00".to_string(), &timezone).unwrap();
        assert_eq!(timestamp_year, 1546318800);
        assert_eq!(timestamp_datetime, 1546318800);
    }
}
