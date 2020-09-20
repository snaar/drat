use crate::chopper::chopper::Source;
use crate::chopper::types::{FieldType, FieldValue, Header, Nanos, Row};
use crate::cli::util::YesNoAuto;
use crate::error::{CliResult, Error};
use crate::source::csv_configs::{CSVInputConfig, DateColIdx, TimeColIdx, TimestampColConfig};
use crate::util::preview::Preview;
use crate::util::{csv_util, timestamp_util};

use chrono::NaiveDateTime;
use csv::{self, Trim};
use std::io::Read;

const DELIMITERS: &[u8] = b",\t ";

enum TimestampCol {
    Index(usize),
    DateTimeIndex(DateColIdx, TimeColIdx),
}

pub struct CSVSource {
    reader: csv::Reader<Box<dyn Read>>,
    header: Header,
    csv_config: CSVInputConfig,
    timestamp_col: TimestampCol,
    next_row: Row,
    has_next_row: bool,
}

impl CSVSource {
    pub fn new(previewer: Box<dyn Preview>, csv_config: &CSVInputConfig) -> CliResult<Self> {
        let (line1, line2) = match previewer.get_lines() {
            None => (None, None),
            Some(lines) => (lines.get(0), lines.get(1)),
        };

        let delimiter = match csv_config.delimiter() {
            None => {
                match line1 {
                    Some(line) => csv_util::guess_delimiter(line.as_str(), DELIMITERS),
                    None => DELIMITERS[0], // doesn't really matter, since file is empty, just give something back
                }
            }
            Some(d) => d,
        };

        let has_header = match csv_config.has_header() {
            YesNoAuto::Yes => true,
            YesNoAuto::No => false,
            YesNoAuto::Auto => csv_util::guess_has_header(line1, line2, delimiter),
        };

        let reader = previewer.get_reader();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_header)
            .trim(Trim::All)
            .from_reader(reader);

        // get field names if available
        let mut field_names: Vec<String> = Vec::new();
        if reader.has_headers() {
            let header_record = reader.headers()?;
            for i in header_record {
                field_names.push(i.to_string());
            }
        }

        // get first row and initialize next_row
        let first_row: csv::StringRecord = reader.records().next().unwrap()?;
        let field_count = first_row.len();
        if !reader.has_headers() {
            // if field name is not given, assign default name - "col_x"
            for i in 0..field_count {
                field_names.push(format!("col_{}", i));
            }
        }

        let timestamp: Nanos = 0;
        let field_values: Vec<FieldValue> = vec![FieldValue::None; field_count];
        let next_row = Row {
            timestamp,
            field_values,
        };
        let field_types: Vec<FieldType> = vec![FieldType::String; field_count];
        let header: Header = Header::new(field_names, field_types);
        let csv_config = csv_config.clone();

        let timestamp_col = match csv_config.timestamp_config().timestamp_col() {
            TimestampColConfig::Index(i) => TimestampCol::Index(*i),
            TimestampColConfig::DateTimeIndex(d, t) => TimestampCol::DateTimeIndex(*d, *t),
            TimestampColConfig::Name(name) => TimestampCol::Index(header.get_field_index(name)?),
            TimestampColConfig::DateTimeName(d, t) => {
                TimestampCol::DateTimeIndex(header.get_field_index(d)?, header.get_field_index(t)?)
            }
        };

        let mut csv_reader = CSVSource {
            reader,
            header,
            csv_config,
            timestamp_col,
            next_row,
            has_next_row: true,
        };

        // update timestamp format
        let ts = csv_reader.get_timestamp(&first_row);
        let timestamp = timestamp_util::complete_timestamp(ts);

        if csv_reader
            .csv_config
            .timestamp_config()
            .timestamp_fmt()
            .is_none()
        {
            for fmt in timestamp_util::DATE_TIME_FORMATS.iter() {
                if NaiveDateTime::parse_from_str(timestamp.as_ref(), fmt.as_ref()).is_ok() {
                    csv_reader
                        .csv_config
                        .timestamp_config_as_mut()
                        .set_timestamp_fmt(fmt.clone());
                }
            }
        }
        if csv_reader
            .csv_config
            .timestamp_config()
            .timestamp_fmt()
            .is_none()
        {
            if timestamp.parse::<Nanos>().is_err() {
                return Err(Error::from(format!(
                    "Cannot parse timestamp. Please provide format for parsing."
                )));
            }
        }

        // update next_row with first row
        csv_reader.update_row(first_row)?;

        Ok(csv_reader)
    }

    fn update_row(&mut self, next_record: csv::StringRecord) -> CliResult<()> {
        for i in 0..next_record.len() {
            self.next_row.field_values[i] =
                FieldValue::String(next_record.get(i).unwrap().to_string());
        }

        let ts = self.get_timestamp(&next_record);
        let timestamp = timestamp_util::complete_timestamp(ts);

        // parse timestamp into Nanos
        self.next_row.timestamp = match self.csv_config.timestamp_config().timestamp_fmt() {
            Some(fmt) => {
                let naive_dt = NaiveDateTime::parse_from_str(timestamp.as_ref(), fmt.as_ref())?;
                self.csv_config
                    .timestamp_config()
                    .timezone()
                    .from_local_datetime(&naive_dt)
                    .unwrap()
                    .timestamp() as Nanos
            }
            None => match timestamp.parse::<Nanos>() {
                Ok(t) => t,
                Err(_) => {
                    return Err(Error::from(format!(
                        "Cannot parse timestamp value - {:?}. \
                        If the csv file has header, please turn on csv header option.",
                        timestamp
                    )))
                }
            },
        };
        Ok(())
    }

    fn get_timestamp(&mut self, record: &csv::StringRecord) -> String {
        match self.timestamp_col {
            TimestampCol::Index(i) => record.get(i).unwrap().to_string(),
            TimestampCol::DateTimeIndex(d, t) => {
                let date = record.get(d).unwrap();
                let time = record.get(t).unwrap();
                format!("{}{}", date, time)
            }
        }
    }

    fn next_row(&mut self) -> CliResult<Option<Row>> {
        if !self.has_next_row {
            return Ok(None);
        }

        let current_row = self.next_row.clone();
        match self.reader.records().next() {
            Some(r) => self.update_row(r?)?,
            None => self.has_next_row = false,
        }
        Ok(Some(current_row))
    }
}

impl Source for CSVSource {
    fn header(&self) -> &Header {
        &self.header
    }

    fn next_row(&mut self) -> CliResult<Option<Row>> {
        self.next_row()
    }

    fn has_native_timestamp_column(&self) -> bool {
        false
    }
}
