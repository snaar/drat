use crate::cli::util::YesNoAuto;
use crate::error::CliResult;
use crate::source::csv_configs::TimestampConfig;
use crate::util::csv_util;

#[derive(Debug, Clone)]
pub struct CSVInputConfig {
    pub delimiter: Option<u8>,
    pub has_header: YesNoAuto,
    pub timestamp_config: TimestampConfig,
}

impl CSVInputConfig {
    pub fn new(timestamp_config: TimestampConfig) -> Self {
        CSVInputConfig {
            delimiter: None,
            has_header: YesNoAuto::Auto,
            timestamp_config,
        }
    }

    pub fn with_delimiter(mut self, delimiter: Option<&str>) -> CliResult<Self> {
        self.delimiter = match delimiter {
            None => None,
            Some(x) => Some(csv_util::parse_into_delimiter(x)?),
        };
        Ok(self)
    }

    pub fn with_header(mut self, has_header: YesNoAuto) -> Self {
        self.has_header = has_header;
        self
    }
}
