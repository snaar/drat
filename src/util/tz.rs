use std::collections::HashMap;
use std::env;

use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone};
use chrono_tz::Tz;

use crate::chopper::error::ChopperResult;
use crate::chopper::error::Error::{
    TimeConversion, TimeZoneMissingForOutput, TimeZoneMissingForParsing,
};

#[derive(Debug, Clone)]
pub struct ChopperTz {
    timezone: Option<Tz>,
}

impl ChopperTz {
    pub fn new_from_cli_arg(
        cli_timezone_arg: Option<&str>,
        timezone_map: Option<HashMap<String, Tz>>,
    ) -> ChopperResult<ChopperTz> {
        let timezone_arg = match cli_timezone_arg {
            None => match env::var("CHOPPER_TZ") {
                Ok(s) => s,
                Err(_) => return Ok(Self::new_always_fails()),
            },
            Some(s) => s.to_owned(),
        };

        Self::new_from_str(timezone_arg, timezone_map)
    }

    pub fn new_always_fails() -> ChopperTz {
        ChopperTz { timezone: None }
    }

    pub fn new_from_str<S: AsRef<str>>(
        tz: S,
        timezone_map: Option<HashMap<String, Tz>>,
    ) -> ChopperResult<ChopperTz> {
        let timezone: Tz = match timezone_map {
            None => tz.as_ref().parse()?,
            Some(map) => match map.get(tz.as_ref()) {
                None => tz.as_ref().parse()?,
                Some(s) => *s,
            },
        };
        Ok(ChopperTz {
            timezone: Some(timezone),
        })
    }

    pub fn from_local_datetime(&self, local: &NaiveDateTime) -> ChopperResult<DateTime<Tz>> {
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

    pub fn timestamp(&self, nanoseconds: u64) -> ChopperResult<DateTime<Tz>> {
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
