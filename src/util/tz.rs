use crate::error::CliResult;
use crate::error::Error::Custom;
use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use std::collections::HashMap;
use std::env;

#[derive(Clone)]
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
            None => Err(Custom(format!(
                "Timezone is needed to parse a date/time value of '{}'. None was provided. \
                Either provide it by setting -z command line arg or by setting CHOPPER_TZ env var.",
                local
            ))),
            Some(timezone) => match timezone.from_local_datetime(local) {
                LocalResult::None | LocalResult::Ambiguous(_, _) => Err(Custom(format!(
                    "Converting '{}' in timezone '{}' to timestamp failed.",
                    local, timezone
                ))),
                LocalResult::Single(t) => Ok(t),
            },
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
