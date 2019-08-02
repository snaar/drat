use chrono::{NaiveDateTime, TimeZone};
use chrono_tz::{Tz, UTC};

use crate::chopper::types::Nanos;
use crate::error::{CliResult, Error};

pub static DEFAULT_MONTH: &str = "01";
pub static DEFAULT_DAY: &str = "01";
pub static DEFAULT_TIME: &str = "00:00:00";
pub static DEFAULT_ZONE: Tz = UTC;
pub static DEFAULT_DATE_FORMAT: &str = "%Y%m%d";
pub static DEFAULT_TIME_FORMAT: &str = "%H:%M:%S";
pub static DEFAULT_TIMESTAMP_FORMAT: &str = "%Y%m%d%H:%M:%S";
pub static DEFAULT_ZONE_FORMAT: &str = "%z";

// list of timestamp formats
lazy_static! {
    pub static ref DATE_TIME_FORMATS: Vec<String> = create_date_time_formats();
}

fn create_date_time_formats() -> Vec<String> {
    let format = vec![
        format!("{}{}", DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT),
        format!("{}-{}", DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT),
        format!("{},{}", DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT),
        format!("{}/{}", DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT),
    ];
    format
}

pub fn parse_timestamp_range(mut datetime: String, timezone: Tz) -> CliResult<Nanos> {
    // if time is not specified
    if datetime.len() <= 8 {
        let date = match datetime.len() {
            // add default month and/or day if not specified
            4 => format!("{}{}{}", datetime, DEFAULT_MONTH, DEFAULT_DAY),
            6 => format!("{}{}", datetime, DEFAULT_DAY),
            _ => datetime
        };
        // add default time
        datetime = format!("{}{}", date, DEFAULT_TIME);
    }

    // try available datetime formats
    for fmt in DATE_TIME_FORMATS.iter() {
        // try parsing to naive datetime first
        let naive_dt = NaiveDateTime::parse_from_str(datetime.as_ref(), fmt.as_ref());
        // if matching format is found, convert naive datetime to a timezone-aware datetime
        if naive_dt.is_ok() {
            return Ok(timezone.from_local_datetime(&naive_dt?).unwrap().timestamp() as Nanos)
        }
    }
    Err(Error::from(format!("Cannot parse timestamp: {}. Please check the format.", datetime)))
}

pub fn parse_time_zone(timezone: Option<&str>) -> Tz {
    let tz: Tz = match timezone {
        Some(z) => z.parse().unwrap(),
        None => DEFAULT_ZONE,
    };
    tz
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz::America::New_York;

    #[test]
    fn test_parse_timestamp_range() {
        let timestamp_year = parse_timestamp_range("2019".to_string(), New_York).unwrap();
        let timestamp_datetime = parse_timestamp_range(
            "20190101-00:00:00".to_string(), New_York).unwrap();
        assert_eq!(timestamp_year, 1546318800);
        assert_eq!(timestamp_datetime, 1546318800);
    }
}
