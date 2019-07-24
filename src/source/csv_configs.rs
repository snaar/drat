use std::fmt;

use crate::error::CliResult;
use crate::util::{csv_util, timestamp_util};

pub static DELIMITER_DEFAULT: &str = ",";
pub static TIMESTAMP_COL_DATE_DEFAULT: usize = 0;

#[derive(Clone)]
pub struct CSVInputConfig {
    delimiter: u8,
    has_header: bool,
    timestamp_col_date: usize,
    timestamp_col_time: Option<usize>,
    timestamp_format: Option<String>,
    time_zone: String
}

#[derive(Clone)]
pub struct CSVOutputConfig {
    delimiter: String,
    print_timestamp: bool
}

impl CSVInputConfig {
    pub fn new(delimiter: &str,
               has_header: bool,
               timestamp_col_date: usize,
               timestamp_col_time: Option<usize>,
               timestamp_format_date: Option<&str>,
               timestamp_format_time: Option<&str>,
               time_zone: String) -> CliResult<Self>
    {
        let delimiter = csv_util::parse_into_delimiter(delimiter)?;
        let date = match timestamp_format_date {
            Some(s) => s,
            None => timestamp_util::DEFAULT_DATE_FORMAT
        };
        let time = match timestamp_format_time {
            Some(s) => s,
            None => timestamp_util::DEFAULT_TIME_FORMAT
        };
        let timestamp_format = format!("{}{}{}", date, time, timestamp_util::DEFAULT_ZONE_FORMAT);

        Ok(CSVInputConfig {
            delimiter,
            has_header,
            timestamp_col_date,
            timestamp_col_time,
            timestamp_format: Some(timestamp_format),
            time_zone
        })
    }

    pub fn new_default() -> CliResult<Self> {
        let delimiter = csv_util::parse_into_delimiter(DELIMITER_DEFAULT)?;
        Ok(CSVInputConfig {
            delimiter,
            has_header: false,
            timestamp_col_date: TIMESTAMP_COL_DATE_DEFAULT,
            timestamp_col_time: None,
            timestamp_format: None,
            time_zone: timestamp_util::DEFAULT_ZONE.to_string()
        })
    }

    pub fn has_header(&self) -> bool {
        self.has_header
    }

    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    pub fn timestamp_col_date(&self) -> usize {
        self.timestamp_col_date
    }

    pub fn timestamp_col_time(&self) -> Option<usize> {
        self.timestamp_col_time
    }

    pub fn timestamp_format(&self) -> Option<&String> {
        self.timestamp_format.as_ref()
    }

    pub fn time_zone(&self) -> &str {
        self.time_zone.as_str()
    }
}

impl CSVOutputConfig {
    pub fn new(delimiter: &str, print_timestamp: bool) -> Self {
        CSVOutputConfig { delimiter: delimiter.to_string(), print_timestamp }
    }

    pub fn new_default() -> Self {
        CSVOutputConfig { delimiter: DELIMITER_DEFAULT.to_string(), print_timestamp: true }
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
        write!(f, "delimiter: {:?}, has headers: {:?}", self.delimiter, self.has_header)
    }
}
