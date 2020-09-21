use std::fmt;

use crate::cli::util::YesNoAuto;
use crate::error::CliResult;
use crate::util::csv_util;
use crate::util::tz::ChopperTz;

pub static OUTPUT_DELIMITER_DEFAULT: &str = ",";
pub static TIMESTAMP_COL_DATE_DEFAULT: usize = 0;

pub type DateColIdx = usize;
pub type TimeColIdx = usize;

#[derive(Clone)]
pub enum TimestampColConfig {
    Auto,
    Index(usize),
    DateTimeIndex(DateColIdx, TimeColIdx),
    Name(String),
    DateTimeName(String, String),
}

#[derive(Clone)]
pub struct TimestampConfig {
    timestamp_col: TimestampColConfig,
    timestamp_fmt: Option<String>,
    timezone: ChopperTz,
}

impl TimestampConfig {
    pub fn new(
        timestamp_col: TimestampColConfig,
        timestamp_fmt: Option<String>,
        timezone: ChopperTz,
    ) -> Self {
        TimestampConfig {
            timestamp_col,
            timestamp_fmt,
            timezone,
        }
    }

    pub fn timestamp_col(&self) -> &TimestampColConfig {
        &self.timestamp_col
    }

    pub fn timestamp_fmt(&self) -> &Option<String> {
        &self.timestamp_fmt
    }

    pub fn set_timestamp_fmt(&mut self, fmt: String) {
        self.timestamp_fmt = Some(fmt)
    }

    pub fn timezone(&self) -> &ChopperTz {
        &self.timezone
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

    pub fn has_header(&self) -> YesNoAuto {
        self.has_header
    }

    pub fn delimiter(&self) -> Option<u8> {
        self.delimiter
    }

    pub fn timestamp_config(&self) -> &TimestampConfig {
        &self.timestamp_config
    }

    pub fn timestamp_config_as_mut(&mut self) -> &mut TimestampConfig {
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
