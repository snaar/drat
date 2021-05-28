use std::cmp::Ordering;

use crate::chopper::sink::{DataSink, DynHeaderSink};
use crate::chopper::types::{FieldValue, Header, Row};
use crate::error::{CliResult, Error};

pub struct RowFilterGreaterValueConfig {
    column_name: String,
    value: FieldValue,
}

pub struct RowFilterGreaterValue {
    column_name: String,
    column_index: Option<usize>,
    value: FieldValue,
}

impl RowFilterGreaterValue {
    pub fn new(column_name: &str, value: FieldValue) -> Box<dyn DynHeaderSink> {
        let config = RowFilterGreaterValueConfig {
            column_name: column_name.to_string(),
            value,
        };
        Box::new(config) as Box<dyn DynHeaderSink>
    }
}

impl DynHeaderSink for RowFilterGreaterValueConfig {
    fn process_header(self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>> {
        let field_names = header.field_names();
        for i in 0..field_names.len() {
            if field_names[i].eq_ignore_ascii_case(self.column_name.as_str()) {
                // set column index
                let filter = RowFilterGreaterValue {
                    column_name: self.column_name,
                    column_index: Some(i),
                    value: self.value,
                };
                return Ok(Box::new(filter));
            }
        }
        Err(Error::from(format!(
            "RowFilterGreaterValueConfig -- field name [{}] not found",
            self.column_name
        )))
    }
}

impl DataSink for RowFilterGreaterValue {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> CliResult<()> {
        match self.column_index {
            Some(i) => {
                let row = io_rows.get(0).unwrap();
                let field_value: &FieldValue = row.field_values.get(i).unwrap();
                if Some(Ordering::Greater) != field_value.partial_cmp(&self.value) {
                    io_rows.clear();
                    return Ok(());
                }
            }
            None => return Err(Error::from("RowFilterGreaterValue -- missing column index")),
        }
        Ok(())
    }
}
