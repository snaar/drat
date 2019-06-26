use crate::chopper::chopper::{DataSink, HeaderSink};
use crate::chopper::header_graph::PinId;
use crate::chopper::types::{FieldValue, Header, Row};
use crate::error::{CliResult, Error};

pub struct RowFilterEqualValueConfig {
    column_name: String,
    value: FieldValue,
}

pub struct RowFilterEqualValue {
    column_name: String,
    column_index: Option<usize>,
    value: FieldValue,
}

impl RowFilterEqualValue {
    pub fn new(column_name: String, value: FieldValue) -> Box<dyn HeaderSink> {
        let config = RowFilterEqualValueConfig { column_name, value };
        Box::new(config) as Box<dyn HeaderSink>
    }
}

impl HeaderSink for RowFilterEqualValueConfig {
    fn process_header(self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>> {
        let field_names = header.field_names();
        for i in 0..field_names.len() {
            if field_names[i].eq_ignore_ascii_case(self.column_name.as_str()) {
                // set column index
                let filter =
                    RowFilterEqualValue {
                        column_name: self.column_name,
                        column_index: Some(i),
                        value: self.value
                    };
                return Ok(filter.boxed())
            }
        }
        Err(Error::from(
            format!("RowFilterEqualValueConfig -- field name -- {} not found", self.column_name)))
    }
}

impl DataSink for RowFilterEqualValue {
    fn write_row(&mut self, row: Row) -> CliResult<Option<Row>> {
        match self.column_index {
            Some(i) => {
                let field_value: &FieldValue = row.field_values.get(i).unwrap();
                if !field_value.eq(&self.value) {
                    return Ok(None)
                }
            }
            None =>
                return Err(Error::from("RowFilterEqualValue -- missing column index"))
        }
        Ok(Some(row))
    }

    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> CliResult<Option<Row>> {
        self.write_row(row)
    }

    fn flush(&mut self) -> CliResult<()> {
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
