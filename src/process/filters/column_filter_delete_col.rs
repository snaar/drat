use std::process;
use crate::dr::dr::{Header, HeaderSink, DataSink};
use crate::dr::types::Row;
use crate::result::{CliResult};

pub struct ColumnFilterDelete {
    header_sink: Option<Box<dyn HeaderSink>>,
    data_sink: Option<Box<dyn DataSink>>,
    column_name: String,
    column_index: Option<usize>,
}

impl ColumnFilterDelete {
    pub fn new(header_sink: Box<dyn HeaderSink>, column_name: String) -> Self {
        ColumnFilterDelete { header_sink: Some(header_sink), data_sink: None, column_name, column_index: None }
    }

    fn add_data_sink(&mut self, data_sink: Box<dyn DataSink>) {
        self.data_sink = Some(data_sink)
    }

    fn filter(&mut self, mut row: Row) -> CliResult<()> {
        match self.column_index {
            Some(i) => {
                row.field_values.remove(i);
                self.data_sink.as_mut().unwrap_or_else(|| {
                    write_error!("Error: missing data sink. please check if header is written");
                    process::exit(1)}).write_row(row)?;
                Ok(())
            }
            None => {
                write_error!("Error: missing column index");
                process::exit(1)
            }
        }
    }
}

impl HeaderSink for ColumnFilterDelete {
    // TODO: figure out better way to remove elements
    fn write_header(mut self: Box<Self>, header: &Header) -> Box<dyn DataSink> {
        let mut header = header.clone();
        let field_names = header.get_field_names();
        let mut i = 0;
        while i != field_names.len() {
            if field_names[i].eq_ignore_ascii_case(self.column_name.as_str()) {
                // remove column
                header.get_field_names_mut().remove(i);
                header.get_field_types_mut().remove(i);
                // set column index
                self.column_index = Some(i);
                // pass header to next header sink
                let header_sink = self.header_sink.take();
                let data_sink = header_sink.unwrap().write_header(&header);
                self.add_data_sink(data_sink);
                return self.boxed()
            }
            i += 1;
        }
        write_error!("Error: field name -- {} not found for RowFilterEqualValue", self.column_name);
        process::exit(1)
    }
}

impl DataSink for ColumnFilterDelete {
    fn write_row(&mut self, row: Row) -> CliResult<()> {
        self.filter(row)
    }

    fn flush(&mut self) -> CliResult<()> {
        self.data_sink.as_mut().unwrap().flush()?;
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
