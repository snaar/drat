use std::cmp::Ordering;
use std::process;
use crate::dr::dr::{DataSink, HeaderSink};
use crate::dr::graph::PinId;
use crate::dr::types::{FieldValue, Header, Row};
use crate::result::{CliResult};

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
    pub fn new(column_name: String, value: FieldValue) -> Box<dyn HeaderSink> {
        let config = RowFilterGreaterValueConfig { column_name, value };
        Box::new(config) as Box<dyn HeaderSink>
    }
}

impl HeaderSink for RowFilterGreaterValueConfig {
    fn process_header(self: Box<Self>, header: &mut Header) -> Box<dyn DataSink> {
        let field_names = header.field_names();
        for i in 0..field_names.len() {
            if field_names[i].eq_ignore_ascii_case(self.column_name.as_str()) {
                // set column index
                let filter = RowFilterGreaterValue { column_name: self.column_name, column_index: Some(i), value: self.value};
                return filter.boxed()
            }
        }
        write_error!("Error: field name -- {} not found for RowFilterGreaterValue", self.column_name)
    }
}

impl DataSink for RowFilterGreaterValue {
    fn write_row(&mut self, row: Row) -> Option<Row> {
        match self.column_index {
            Some(i) => {
                let field_value: &FieldValue = row.field_values.get(i).unwrap();
                if Some(Ordering::Greater) != field_value.partial_cmp(&self.value) {
                    return None
                }
            }
            None => write_error!("Error: missing column index")
        }
        Some(row)
    }

    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> Option<Row> {
        self.write_row(row)
    }

    fn flush(&mut self) -> CliResult<()> {
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
