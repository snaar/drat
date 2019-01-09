use csv;
use std::io;
use std::process;

use crate::read::dr;
use crate::read::types::{Row, FieldValue, Nanos};

pub struct CSVReader<R> {
    reader: csv::Reader<R>,
    header: Vec<String>,
    timestamp_column: usize,
    current_row: Row,
}

impl <R: io::Read> CSVReader<R> {
    pub fn new(mut reader: csv::Reader<R>, timestamp_column: usize) -> Self {
        let mut header: Vec<String>;
        let mut field_values: Vec<FieldValue> = Vec::new();
        let timestamp = 0 as Nanos;

        match reader.has_headers() {
            true => {
                let header_record = reader.headers().unwrap();
                header = Vec::with_capacity(header_record.len());
                for i in header_record {
                    header.push(i.to_string());
                }
            }
            false => {
                header = Vec::with_capacity(0);
            }
        }

        // get the first row without advancing the pointer => to get the number of columns
        let first_row = match reader.records().peekable().next().unwrap() {
            Ok(r) => r,
            Err(err) => {
                werr!("{}", err);
                process::exit(1);
            }
        };
        let mut count = 0;
        for _i in first_row.iter() {
            if count != timestamp_column {
                field_values.push(FieldValue::None);
            }
            count += 1;
        }

        let current_row = Row { timestamp, field_values };

        CSVReader { reader, header, timestamp_column, current_row }
    }

    fn next_row(&mut self) -> Option<Row> {
        let next_record = self.reader.records().next();
        if next_record.is_none() {
            return None
        }
        let next_record = next_record.unwrap().unwrap();

        let mut index = 0;
        let mut column_count = 0;
        for i in next_record.iter() {
            if column_count == self.timestamp_column {
                self.current_row.timestamp = i.parse::<Nanos>().unwrap();
            }
            else {
                self.current_row.field_values[index] = FieldValue::String(i.to_string());
                index += 1;
            }
            column_count += 1;
        }
        Some(self.current_row.clone())
    }
}

impl <R: io::Read> dr::Reader for CSVReader<R> {
    fn header(&self) -> &Vec<String> {
        &self.header
    }

    fn next_row(&mut self) -> Option<Row> {
        self.next_row()
    }
}
