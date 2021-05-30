use std::{io, num};

use chrono::NaiveDateTime;
use chrono_tz::Tz;
use thiserror::Error as ThisError;

use crate::chopper::types::Nanos;

pub type ChopperResult<T> = Result<T, Error>;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    CliParsing(#[from] clap::Error),
    #[error(transparent)]
    Csv(csv::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    TimeParsing(#[from] chrono::ParseError),
    #[error(transparent)]
    NumParseInt(#[from] num::ParseIntError),
    #[error("Failed to find column named '{0}'.")]
    ColumnMissing(String),
    #[error(
        "TimeZone is needed to parse a date/time value of '{0}'. None was provided. \
        Either provide it by setting -z command line arg or by setting CHOPPER_TZ env var."
    )]
    TimeZoneMissingForParsing(NaiveDateTime),
    #[error(
        "Timezone is needed to convert timestamp {0} to a human-readable time for output. \
        None was provided. Either provide it by setting -z command line arg or by setting \
        CHOPPER_TZ env var or by using --epoch for raw timestamps."
    )]
    TimeZoneMissingForOutput(Nanos),
    #[error("Converting '{0}' in timezone '{1}' to timestamp failed.")]
    TimeConversion(NaiveDateTime, Tz),
    #[error("Error: {0}")]
    Custom(String),
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Error {
        if !err.is_io_error() {
            return Error::Csv(err);
        }
        match err.into_kind() {
            csv::ErrorKind::Io(v) => From::from(v),
            _ => unreachable!(),
        }
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::Custom(err)
    }
}

impl<'a> From<&'a str> for Error {
    fn from(err: &'a str) -> Error {
        Error::Custom(err.to_owned())
    }
}
