use crate::chopper::error::{ChopperResult, Error};
use crate::chopper::sink::{DataSink, DynHeaderSink};
use crate::chopper::types::{Header, Row};

pub struct ColumnFilterDeleteConfig {
    column_name: String,
}

pub struct ColumnFilterDelete {
    column_index: usize,
}

impl ColumnFilterDelete {
    pub fn new(column_name: &str) -> Box<dyn DynHeaderSink> {
        let config = ColumnFilterDeleteConfig {
            column_name: column_name.to_string(),
        };
        Box::new(config) as Box<dyn DynHeaderSink>
    }
}

impl DynHeaderSink for ColumnFilterDeleteConfig {
    // TODO: figure out better way to remove elements
    fn process_header(self: Box<Self>, header: &mut Header) -> ChopperResult<Box<dyn DataSink>> {
        let field_names = header.field_names();
        let mut i = 0;
        while i != field_names.len() {
            if field_names[i].eq_ignore_ascii_case(self.column_name.as_str()) {
                // remove column
                header.field_names_mut().remove(i);
                header.field_types_mut().remove(i);
                // return data filter with the column index
                let data_sink = ColumnFilterDelete { column_index: i };
                return Ok(Box::new(data_sink));
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
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> ChopperResult<()> {
        let row = io_rows.get_mut(0).unwrap();
        row.field_values.remove(self.column_index);
        Ok(())
    }
}
