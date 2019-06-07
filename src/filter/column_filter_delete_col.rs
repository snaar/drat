use std::process;
use crate::dr::dr::{DataSink, HeaderSink};
use crate::dr::graph::PinId;
use crate::dr::types::{Header, Row};
use crate::result::{CliResult};

pub struct ColumnFilterDeleteConfig {
    column_name: String,
}

pub struct ColumnFilterDelete {
    column_index: usize,
}

impl ColumnFilterDelete {
    pub fn new(column_name: String) -> Box<dyn HeaderSink> {
        let config = ColumnFilterDeleteConfig { column_name };
        Box::new(config) as Box<dyn HeaderSink>
    }
}

impl HeaderSink for ColumnFilterDeleteConfig {
    // TODO: figure out better way to remove elements
    fn process_header(self: Box<Self>, header: &mut Header) -> Box<dyn DataSink> {
        let field_names = header.field_names();
        let mut i = 0;
        while i != field_names.len() {
            if field_names[i].eq_ignore_ascii_case(self.column_name.as_str()) {
                // remove column
                header.field_names_mut().remove(i);
                header.field_types_mut().remove(i);
                // return data filter with the column index
                let data_sink = ColumnFilterDelete { column_index: i };
                return data_sink.boxed()
            }
            i += 1;
        }
        write_error!("Error: field name -- {} not found for RowFilterEqualValue", self.column_name)
    }
}

impl DataSink for ColumnFilterDelete {
    fn write_row(&mut self, mut row: Row) -> Option<Row> {
        row.field_values.remove(self.column_index);
        Some(row)
    }

    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> Option<Row> {
        self.write_row(row)
    }

    fn flush(&mut self) -> CliResult<()> {
        Ok(())
    }

    fn boxed(self) -> Box<DataSink> {
        Box::new(self)
    }
}
