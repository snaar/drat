use csv;
use std::io;

use crate::chopper::chopper::Source;
use crate::chopper::types::{FieldType, FieldValue, Header, Nanos, Row};
use crate::error::CliResult;

pub struct CSVReader<R> {
    reader: csv::Reader<R>,
    header: Header,
    timestamp_column: usize,
    next_row: Row,
    has_next_row: bool,
}

impl <R: io::Read> CSVReader<R> {
    pub fn new(mut reader: csv::Reader<R>, timestamp_column: usize) -> CliResult<Self> {
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
        // if field name is not given, assign default name - "col_x"
        if !reader.has_headers() {
            for i in 0..field_count {
                field_names.push(format!("col_{}", i));
            }
        }
        let timestamp: Nanos = 0;
        let field_values: Vec<FieldValue> = vec![FieldValue::None; field_count];
        let next_row = Row { timestamp, field_values };

        let field_types: Vec<FieldType> = vec![FieldType::String; field_count];
        let header: Header = Header::new(field_names, field_types);
        let mut csv_reader = CSVReader { reader, header, timestamp_column, next_row, has_next_row: true };

        // update next_row with first row
        csv_reader.update_row(first_row)?;

        Ok(csv_reader)
    }

    fn update_row(&mut self, next_record: csv::StringRecord) -> CliResult<()> {
        let mut current_column = 0;
        for i in next_record.iter() {
            if current_column == self.timestamp_column {
                self.next_row.timestamp = i.parse::<Nanos>().unwrap();
            }
            self.next_row.field_values[current_column] = FieldValue::String(i.to_string());
            current_column += 1;
        }
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

impl <R: io::Read> Source for CSVReader<R> {
    fn header(&self) -> &Header {
        &self.header
    }

    fn next_row(&mut self) -> CliResult<Option<Row>> {
        self.next_row()
    }
}
