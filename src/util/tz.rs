use std::collections::HashMap;
use std::env;

use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone};
use chrono_tz::Tz;

use crate::error::CliResult;
use crate::error::Error::{TimeConversion, TimeZoneMissingForOutput, TimeZoneMissingForParsing};

#[derive(Debug, Clone)]
pub struct ChopperTz {
    timezone: Option<Tz>,
}

impl ChopperTz {
    pub fn new_from_cli_arg(
        cli_timezone_arg: Option<&str>,
        timezone_map: Option<HashMap<&str, Tz>>,
    ) -> ChopperTz {
        let timezone = Self::resolve_timezone(cli_timezone_arg, timezone_map);
        ChopperTz { timezone }
    }

    pub fn new_always_fails() -> ChopperTz {
        ChopperTz { timezone: None }
    }

    fn resolve_timezone(
        cli_timezone_arg: Option<&str>,
        timezone_map: Option<HashMap<&str, Tz>>,
    ) -> Option<Tz> {
        let timezone_arg = match cli_timezone_arg {
            None => match env::var("CHOPPER_TZ") {
                Ok(s) => Some(s),
                Err(_) => None,
            },
            Some(s) => Some(s.to_owned()),
        }?;

        let timezone: Tz = match timezone_map {
            None => timezone_arg.parse().unwrap(),
            Some(map) => match map.get(timezone_arg.as_str()) {
                None => timezone_arg.parse().unwrap(),
                Some(s) => *s,
            },
        };
        Some(timezone)
    }

    pub fn from_local_datetime(&self, local: &NaiveDateTime) -> CliResult<DateTime<Tz>> {
        match self.timezone {
            None => Err(TimeZoneMissingForParsing(local.clone())),
            Some(timezone) => match timezone.from_local_datetime(local) {
                LocalResult::None | LocalResult::Ambiguous(_, _) => {
                    Err(TimeConversion(local.clone(), timezone))
                }
                LocalResult::Single(t) => Ok(t),
            },
        }
    }

    pub fn timestamp(&self, nanoseconds: u64) -> CliResult<DateTime<Tz>> {
        match self.timezone {
            None => Err(TimeZoneMissingForOutput(nanoseconds)),
            Some(timezone) => {
                let secs = (nanoseconds / 1_000_000_000) as i64;
                let nsecs = (nanoseconds % 1_000_000_000) as u32;
                Ok(timezone.timestamp(secs, nsecs))
            }
        }
    }
}

impl From<Tz> for ChopperTz {
    fn from(original: Tz) -> Self {
        ChopperTz {
            timezone: Some(original),
        }
    }
}
