use crate::chopper::chopper::{DataSink, HeaderSink};
use crate::chopper::types::{Header, Row};
use crate::error::{CliResult, Error};

pub struct ColumnFilterDeleteConfig {
    column_name: String,
}

pub struct ColumnFilterDelete {
    column_index: usize,
}

impl ColumnFilterDelete {
    pub fn new(column_name: &str) -> Box<dyn HeaderSink> {
        let config = ColumnFilterDeleteConfig {
            column_name: column_name.to_string(),
        };
        Box::new(config) as Box<dyn HeaderSink>
    }
}

impl HeaderSink for ColumnFilterDeleteConfig {
    // TODO: figure out better way to remove elements
    fn process_header(self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>> {
        let field_names = header.field_names();
        let mut i = 0;
        while i != field_names.len() {
            if field_names[i].eq_ignore_ascii_case(self.column_name.as_str()) {
                // remove column
                header.field_names_mut().remove(i);
                header.field_types_mut().remove(i);
                // return data filter with the column index
                let data_sink = ColumnFilterDelete { column_index: i };
                return Ok(data_sink.boxed());
            }
            i += 1;
        }
        Err(Error::from(format!(
            "ColumnFilterDeleteConfig -- field name [{}] not found",
            self.column_name
        )))
    }
}

impl DataSink for ColumnFilterDelete {
    fn write_row(&mut self, mut row: Row) -> CliResult<Option<Row>> {
        row.field_values.remove(self.column_index);
        Ok(Some(row))
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
