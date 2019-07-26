use std::io;

use chrono::{NaiveDateTime, TimeZone};
use csv;
use csv::Trim;

use crate::chopper::chopper::Source;
use crate::chopper::types::{FieldType, FieldValue, Header, Nanos, Row};
use crate::error::{CliResult, Error};
use crate::source::csv_configs::CSVInputConfig;
use crate::util::timestamp_util;

pub struct CSVSource<R> {
    reader: csv::Reader<R>,
    header: Header,
    csv_config: CSVInputConfig,
    next_row: Row,
    has_next_row: bool,
}

impl <R: io::Read> CSVSource<R> {
    pub fn new(reader: R, csv_config: &CSVInputConfig) -> CliResult<Self> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(csv_config.delimiter())
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
        let first_row = reader.records().next().unwrap()?;
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

        // update next_row with first row
        csv_reader.update_row(first_row)?;

        Ok(csv_reader)
    }

    fn update_row(&mut self, next_record: csv::StringRecord) -> CliResult<()> {
        let mut current_column = 0;
        let mut date: &str = "";
        let mut time: &str = timestamp_util::DEFAULT_TIME;

        for i in next_record.iter() {
            if current_column == self.csv_config.timestamp_col_date() {
                date = i;
            }
            else if self.csv_config.timestamp_col_time().is_some()
                && current_column == self.csv_config.timestamp_col_time().unwrap() {
                time = i;
            }
            self.next_row.field_values[current_column] = FieldValue::String(i.to_string());
            current_column += 1;
        }

        // parse timestamp into Nanos
        let timestamp = format!("{}{}", date, time);
        self.next_row.timestamp = match self.csv_config.timestamp_format() {
            Some(fmt) => {
                let naive_dt = NaiveDateTime::parse_from_str(timestamp.as_ref(), fmt)?;
                self.csv_config.timezone().from_local_datetime(&naive_dt).unwrap().timestamp() as Nanos
            },
            None => match timestamp.parse::<Nanos>() {
                Ok(t) => t,
                Err(_) => return Err(Error::from(format!("Cannot parse timestamp value - {:?}. \
                        If the csv file has header, please turn on csv header option.", timestamp)))
            }
        };

        Ok(())
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

impl <R: io::Read> Source for CSVSource<R> {
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
