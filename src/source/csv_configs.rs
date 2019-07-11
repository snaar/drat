use std::fmt;

use crate::error::CliResult;
use crate::util::csv_util;

pub static DELIMITER_DEFAULT: &str = ",";
pub static TIMESTAMP_COL_DEFAULT: usize = 0;

#[derive(Clone)]
pub struct CSVInputConfig {
    delimiter: u8,
    has_header: bool,
    timestamp_column_index: usize,
}

#[derive(Clone)]
pub struct CSVOutputConfig {
    delimiter: String,
    print_timestamp: bool
}

impl CSVInputConfig {
    pub fn new(delimiter: &str, has_header: bool, timestamp_column_index: usize) -> CliResult<Self> {
        let delimiter = csv_util::parse_into_delimiter(delimiter)?;
        Ok(CSVInputConfig { delimiter, has_header, timestamp_column_index })
    }

    pub fn new_default() -> CliResult<Self> {
        let delimiter = csv_util::parse_into_delimiter(DELIMITER_DEFAULT)?;
        Ok(CSVInputConfig { delimiter, has_header: false, timestamp_column_index: TIMESTAMP_COL_DEFAULT})
    }

    pub fn has_header(&self) -> bool {
        self.has_header
    }

    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    pub fn timestamp_col_index(&self) -> usize {
        self.timestamp_column_index
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
