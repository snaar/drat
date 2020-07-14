use std::io;

use chrono::{NaiveDateTime, TimeZone};
use csv::{self, Trim};

use crate::chopper::chopper::Source;
use crate::chopper::types::{FieldType, FieldValue, Header, Nanos, Row};
use crate::error::{CliResult, Error};
use crate::source::csv_configs::{CSVInputConfig, TimestampCol};
use crate::util::{csv_util, timestamp_util};
use crate::util::reader::{ChopperHeaderPreview, ChopperBufReader};

const DELIMITERS: &[u8] = b", ";

pub struct CSVSource<R> {
    reader: csv::Reader<ChopperBufReader<R>>,
    header: Header,
    csv_config: CSVInputConfig,
    next_row: Row,
    has_next_row: bool,
}

impl<R: io::Read> CSVSource<R> {
    pub fn new(reader: R, csv_config: &CSVInputConfig) -> CliResult<Self> {
        let header_preview = ChopperHeaderPreview::new(reader).unwrap();
        let delimiter = match csv_config.delimiter() {
            None => {
                csv_util::guess_delimiter(header_preview.header.as_str(), DELIMITERS)
            },
            Some(d) => d,
        };
        let reader = header_preview.rewind_and_get_reader();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(csv_config.has_header())
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
        let next_row = Row { timestamp, field_values };
        let field_types: Vec<FieldType> = vec![FieldType::String; field_count];
        let header: Header = Header::new(field_names, field_types);
        let csv_config = csv_config.clone();

        let mut csv_reader = CSVSource { reader, header, csv_config, next_row, has_next_row: true };

        // update timestamp format
        let ts = csv_reader.get_timestamp(&first_row);
        let timestamp = timestamp_util::complete_timestamp(ts);

        if csv_reader.csv_config.timestamp_config().timestamp_fmt().is_none() {
            for fmt in timestamp_util::DATE_TIME_FORMATS.iter() {
                if NaiveDateTime::parse_from_str(timestamp.as_ref(), fmt.as_ref()).is_ok() {
                    csv_reader.csv_config.timestamp_config().set_timestamp_fmt(fmt.clone());
                }
            }
        }
        if csv_reader.csv_config.timestamp_config().timestamp_fmt().is_none() {
            if timestamp.parse::<Nanos>().is_err() {
                return Err(Error::from
                    (format!("Cannot parse timestamp. Please provide format for parsing.")))
            }
        }

        // update next_row with first row
        csv_reader.update_row(first_row)?;

        Ok(csv_reader)
    }

    fn update_row(&mut self, next_record: csv::StringRecord) -> CliResult<()> {
        for i in 0..next_record.len() {
            self.next_row.field_values[i]
                = FieldValue::String(next_record.get(i).unwrap().to_string());
        }

        let ts = self.get_timestamp(&next_record);
        let timestamp = timestamp_util::complete_timestamp(ts);

        // parse timestamp into Nanos
        self.next_row.timestamp = match self.csv_config.timestamp_config().timestamp_fmt() {
            Some(fmt) => {
                let naive_dt = NaiveDateTime::parse_from_str(timestamp.as_ref(), fmt.as_ref())?;
                self.csv_config.timestamp_config().timezone().from_local_datetime(&naive_dt).unwrap().timestamp() as Nanos
            },
            None => {
                match timestamp.parse::<Nanos>() {
                    Ok(t) => t,
                    Err(_) => return Err(Error::from(format!("Cannot parse timestamp value - {:?}. \
                        If the csv file has header, please turn on csv header option.", timestamp)))
                }
            }
        };
        Ok(())
    }

    fn get_timestamp(&mut self, record: &csv::StringRecord) -> String {
        match self.csv_config.timestamp_config().timestamp_col() {
            TimestampCol::Timestamp(i) => record.get(*i).unwrap().to_string(),
            TimestampCol::DateAndTime(d, t) => {
                let date = record.get(*d).unwrap();
                let time = record.get(*t).unwrap();
                format!("{}{}", date, time)
            }
        }
    }

    fn next_row(&mut self) -> CliResult<Option<Row>> {
        if !self.has_next_row {
            return Ok(None)
        }

        let current_row = self.next_row.clone();
        match self.reader.records().next() {
            Some(r) => self.update_row(r?)?,
            None => self.has_next_row = false,
        }
        Ok(Some(current_row))
    }
}

impl<R: io::Read> Source for CSVSource<R> {
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
