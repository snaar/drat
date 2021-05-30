use chrono::{DateTime, NaiveDateTime};

use crate::chopper::types::Nanos;
use crate::error::{CliResult, Error};
use crate::source::csv_timestamp::{RANGE_MICROS, RANGE_MILLIS, RANGE_NANOS, RANGE_SECONDS};
use crate::util::tz::ChopperTz;

pub static DEFAULT_MONTH: &str = "01";
pub static DEFAULT_DAY: &str = "01";
pub static DEFAULT_TIME: &str = "00:00:00";

// %+ is the ISO 8601 / RFC 3339 format
const DATETIME_RANGE_FORMATS_WITH_TZ: [&'static str; 1] = ["%+"];

lazy_static! {
    static ref DATETIME_RANGE_FORMATS_NEED_TZ: Vec<String> = create_datetime_range_formats();
}

fn create_datetime_range_formats() -> Vec<String> {
    let mut formats: Vec<String> = Vec::new();
    let date_formats = vec!["%Y%m%d", "%Y/%m/%d", "%Y-%m-%d"];
    let delimiters = vec![" ", "-", "T"];
    let time_formats = vec!["%H:%M:%S", "%H%M%S"];
    let fractions = vec!["", "%.f"];
    for df in &date_formats {
        for d in &delimiters {
            for tf in &time_formats {
                for f in &fractions {
                    formats.push(format!("{}{}{}{}", df, d, tf, f));
                }
            }
        }
    }
    formats
}

pub fn parse_datetime_range_element(datetime: &str, timezone: &ChopperTz) -> CliResult<Nanos> {
    let is_datetime_all_digits = datetime.chars().all(|c| c.is_ascii_digit());
    if is_datetime_all_digits {
        let n = datetime.parse::<u64>()?;
        if RANGE_NANOS.contains(&n) {
            return Ok(n);
        }
        if RANGE_MICROS.contains(&n) {
            return Ok(n * 1_000);
        }
        if RANGE_MILLIS.contains(&n) {
            return Ok(n * 1_000_000);
        }
        if RANGE_SECONDS.contains(&n) {
            return Ok(n * 1_000_000_000);
        }
    }

    let datetime_raw = datetime;
    let datetime = if is_datetime_all_digits {
        // as a special case, handle inputs like YYYY and YYYYMM by autocompleting them
        match datetime.len() {
            4 => format!(
                "{}{}{}-{}",
                datetime, DEFAULT_MONTH, DEFAULT_DAY, DEFAULT_TIME
            ),
            6 => format!("{}{}-{}", datetime, DEFAULT_DAY, DEFAULT_TIME),
            8 => format!("{}-{}", datetime, DEFAULT_TIME),
            _ => datetime.to_owned(),
        }
    } else {
        datetime.to_owned()
    };

    // try the formats that don't need external TZ first
    for format in &DATETIME_RANGE_FORMATS_WITH_TZ {
        if let Ok(dt) = DateTime::parse_from_str(&datetime, format) {
            return Ok(dt.timestamp_nanos() as Nanos);
        };
    }

    // try available datetime formats that need external TZ
    for format in DATETIME_RANGE_FORMATS_NEED_TZ.iter() {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(&datetime, &format) {
            return Ok(timezone.from_local_datetime(&ndt)?.timestamp_nanos() as Nanos);
        }
    }

    Err(Error::from(format!(
        "Cannot parse either begin or end datetime provided: {}",
        datetime_raw
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz::America::New_York;
    use chrono_tz::Etc::UTC;

    #[test]
    fn test_parse_timestamp_range() {
        let ts = parse_datetime_range_element(
            "2001-07-08T00:34:60.026490+09:30",
            &ChopperTz::new_always_fails(),
        )
        .unwrap();
        assert_eq!(ts, 994518300026490000);

        let ts = parse_datetime_range_element(
            "2019-01-01T00:00:00.001000+00:00",
            &ChopperTz::new_always_fails(),
        )
        .unwrap();
        assert_eq!(ts, 1546300800001000000);

        let ts = parse_datetime_range_element(
            "2019-01-01T00:00:00.001000-05:00",
            &ChopperTz::new_always_fails(),
        )
        .unwrap();
        assert_eq!(ts, 1546318800001000000);

        let utc = ChopperTz::from(UTC);
        let nyc = ChopperTz::from(New_York);

        let ts = parse_datetime_range_element("2019", &utc).unwrap();
        assert_eq!(ts, 1546300800000000000);
        let ts = parse_datetime_range_element("2019", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("201901", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("20190101", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("20190101-00:00:00", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("20190101-00:00:00.0", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("20190101-00:00:00.1", &nyc).unwrap();
        assert_eq!(ts, 1546318800100000000);
        let ts = parse_datetime_range_element("20190101-00:00:00.000", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("20190101-00:00:00.001", &nyc).unwrap();
        assert_eq!(ts, 1546318800001000000);
        let ts = parse_datetime_range_element("20190101-00:00:00.000000", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("20190101-00:00:00.000001", &nyc).unwrap();
        assert_eq!(ts, 1546318800000001000);
        let ts = parse_datetime_range_element("20190101-00:00:00.000000000", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000000);
        let ts = parse_datetime_range_element("20190101-00:00:00.000000001", &nyc).unwrap();
        assert_eq!(ts, 1546318800000000001);

        // nanos
        let ts = parse_datetime_range_element("630000000000000000", &ChopperTz::new_always_fails())
            .unwrap();
        assert_eq!(ts, 630_000_000_000_000_000);
        // micros
        let ts = parse_datetime_range_element("630000000000000", &ChopperTz::new_always_fails())
            .unwrap();
        assert_eq!(ts, 630_000_000_000_000_000);
        // millis
        let ts =
            parse_datetime_range_element("630000000000", &ChopperTz::new_always_fails()).unwrap();
        assert_eq!(ts, 630_000_000_000_000_000);
        // seconds
        let ts = parse_datetime_range_element("630000000", &ChopperTz::new_always_fails()).unwrap();
        assert_eq!(ts, 630_000_000_000_000_000);
    }
}
