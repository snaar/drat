use crate::source::csv_timestamp::TimestampUnits;
use crate::util::tz::ChopperTz;

pub static OUTPUT_DELIMITER_DEFAULT: &str = ",";
pub static TIMESTAMP_COL_DATE_DEFAULT: usize = 0;

pub type DateColIdx = usize;
pub type TimeColIdx = usize;

#[derive(Debug, Clone)]
pub enum TimestampFmtConfig {
    Auto,
    Epoch(TimestampUnits),
    Explicit(String),
    DateTimeExplicit(String, String),
}

#[derive(Debug, Clone)]
pub enum TimestampColConfig {
    Auto,
    Index(usize),
    DateTimeIndex(DateColIdx, TimeColIdx),
    Name(String),
    DateTimeName(String, String),
}

#[derive(Clone, Copy)]
pub enum TimestampStyle {
    Epoch,
    HumanReadable,
}

#[derive(Debug, Clone)]
pub struct TimestampConfig {
    timestamp_col: TimestampColConfig,
    timestamp_fmt: TimestampFmtConfig,
    timezone: ChopperTz,
}

impl TimestampConfig {
    pub fn new(
        timestamp_col: TimestampColConfig,
        timestamp_fmt: TimestampFmtConfig,
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

    pub fn timestamp_fmt(&self) -> &TimestampFmtConfig {
        &self.timestamp_fmt
    }

    pub fn timezone(&self) -> &ChopperTz {
        &self.timezone
    }
}

#[derive(Clone)]
pub struct CSVOutputConfig {
    delimiter: String,
    print_time_col: bool,
    time_col_name: String,
    time_col_style: TimestampStyle,
    time_col_units: TimestampUnits,
    timezone: ChopperTz,
}

impl CSVOutputConfig {
    pub fn new(
        delimiter: &str,
        print_time_col: bool,
        time_col_name: Option<String>,
        time_col_style: TimestampStyle,
        time_col_units: TimestampUnits,
        timezone: ChopperTz,
    ) -> Self {
        let time_col_name = match time_col_name {
            None => match time_col_style {
                TimestampStyle::Epoch => {
                    let base_name = "timestamp".to_string();
                    let units = time_col_units.to_suffix_str();
                    base_name + units
                }
                TimestampStyle::HumanReadable => "time".to_string(),
            },
            Some(name) => name,
        };

        CSVOutputConfig {
            delimiter: delimiter.to_string(),
            print_time_col,
            time_col_name,
            time_col_style,
            time_col_units,
            timezone,
        }
    }

    pub fn new_default() -> Self {
        Self::new(
            OUTPUT_DELIMITER_DEFAULT,
            true,
            None,
            TimestampStyle::Epoch,
            TimestampUnits::Nanos,
            ChopperTz::new_always_fails(),
        )
    }

    pub fn delimiter(&self) -> &String {
        &self.delimiter
    }

    pub fn print_time_col(&self) -> bool {
        self.print_time_col
    }

    pub fn time_col_name(&self) -> &String {
        &self.time_col_name
    }

    pub fn time_col_style(&self) -> TimestampStyle {
        self.time_col_style
    }
    pub fn time_col_units(&self) -> TimestampUnits {
        self.time_col_units
    }

    pub fn timezone(&self) -> &ChopperTz {
        &self.timezone
    }
}
