use std::fmt;

use crate::error::CliResult;
use crate::util::csv_util;

pub static DELIMITER_DEFAULT: &str = ",";
pub static TIMESTAMP_COL_DEFAULT: usize = 0;

#[derive(Clone)]
pub struct CSVConfig {
    delimiter: u8,
    has_headers: bool,
    timestamp_column_index: usize,
    print_timestamp: bool
}

impl CSVConfig {
    pub fn new(delimiter: &str, has_headers: bool, timestamp_column_index: usize, print_timestamp: bool) -> CliResult<Self> {
        let delimiter = csv_util::parse_into_delimiter(delimiter)?;
        Ok(CSVConfig { delimiter, has_headers, timestamp_column_index, print_timestamp })
    }

    pub fn new_default() -> CliResult<Self> {
        let delimiter = csv_util::parse_into_delimiter(DELIMITER_DEFAULT)?;
        Ok(CSVConfig { delimiter, has_headers: false, timestamp_column_index: TIMESTAMP_COL_DEFAULT, print_timestamp: false })
    }

    pub fn has_headers(&self) -> bool {
        self.has_headers
    }

    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    pub fn timestamp_col_index(&self) -> usize {
        self.timestamp_column_index
    }

    pub fn print_timestamp(&self) -> bool {
        self.print_timestamp
    }
}

impl fmt::Debug for CSVConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "delimiter: {:?}, has headers: {:?}", self.delimiter, self.has_headers)
    }
}
