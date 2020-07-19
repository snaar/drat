use std::fmt;

use chrono_tz::Tz;

use crate::cli::util::YesNoAuto;
use crate::error::CliResult;
use crate::util::{csv_util, timestamp_util};

pub static OUTPUT_DELIMITER_DEFAULT: &str = ",";
pub static TIMESTAMP_COL_DATE_DEFAULT: usize = 0;

pub type DateCol = usize;
pub type TimeCol = usize;

#[derive(Clone)]
pub enum TimestampCol {
    Timestamp(usize),
    DateAndTime(DateCol, TimeCol),
}

#[derive(Clone)]
pub struct TimestampConfig {
    timestamp_col: TimestampCol,
    timestamp_fmt: Option<String>,
    timezone: Tz,
}

impl TimestampConfig {
    pub fn new(timestamp_col: TimestampCol, timestamp_fmt: Option<String>, timezone: Tz) -> Self {
        TimestampConfig {
            timestamp_col,
            timestamp_fmt,
            timezone,
        }
    }

    pub fn default() -> Self {
        let timestamp_col = TimestampCol::Timestamp(0);
        let timezone = timestamp_util::DEFAULT_ZONE;
        TimestampConfig {
            timestamp_col,
            timestamp_fmt: None,
            timezone,
        }
    }

    pub fn timestamp_col(&mut self) -> &mut TimestampCol {
        &mut self.timestamp_col
    }

    pub fn timestamp_fmt(&mut self) -> &mut Option<String> {
        &mut self.timestamp_fmt
    }

    pub fn set_timestamp_fmt(&mut self, fmt: String) {
        self.timestamp_fmt = Some(fmt)
    }

    pub fn timezone(&self) -> Tz {
        self.timezone
    }
}

#[derive(Clone)]
pub struct CSVInputConfig {
    delimiter: Option<u8>,
    has_header: YesNoAuto,
    timestamp_config: TimestampConfig,
}

#[derive(Clone)]
pub struct CSVOutputConfig {
    delimiter: String,
    print_timestamp: bool,
}

impl CSVInputConfig {
    pub fn new(
        delimiter: Option<&str>,
        has_header: YesNoAuto,
        timestamp_config: TimestampConfig,
    ) -> CliResult<Self> {
        let delimiter = match delimiter {
            None => None,
            Some(x) => Some(csv_util::parse_into_delimiter(x)?),
        };
        Ok(CSVInputConfig {
            delimiter,
            has_header,
            timestamp_config,
        })
    }

    pub fn new_default() -> CliResult<Self> {
        let timestamp_config = TimestampConfig::default();
        Ok(CSVInputConfig {
            delimiter: None,
            has_header: YesNoAuto::Auto,
            timestamp_config,
        })
    }

    pub fn has_header(&self) -> YesNoAuto {
        self.has_header
    }

    pub fn delimiter(&self) -> Option<u8> {
        self.delimiter
    }

    pub fn timestamp_config(&mut self) -> &mut TimestampConfig {
        &mut self.timestamp_config
    }
}

impl CSVOutputConfig {
    pub fn new(delimiter: &str, print_timestamp: bool) -> Self {
        CSVOutputConfig {
            delimiter: delimiter.to_string(),
            print_timestamp,
        }
    }

    pub fn new_default() -> Self {
        CSVOutputConfig {
            delimiter: OUTPUT_DELIMITER_DEFAULT.to_string(),
            print_timestamp: true,
        }
    }

    pub fn delimiter(&self) -> &String {
        &self.delimiter
    }

    pub fn print_timestamp(&self) -> bool {
        self.print_timestamp
    }
}

impl fmt::Debug for CSVInputConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "delimiter: {:?}, has headers: {:?}",
            self.delimiter, self.has_header
        )
    }
}
