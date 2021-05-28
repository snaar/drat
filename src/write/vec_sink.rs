use crate::chopper::sink::{DataSink, DynDataSink, DynHeaderSink};
use crate::chopper::types::{Header, Row};
use crate::error::CliResult;

pub struct VecSink {
    pub header: Option<Header>,
    pub rows: Vec<Row>,
}

impl VecSink {
    pub fn new() -> VecSink {
        VecSink {
            header: None,
            rows: Vec::new(),
        }
    }
}

impl DynHeaderSink for VecSink {
    fn process_header(mut self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DynDataSink>> {
        self.header = Some(header.clone());
        Ok(self)
    }
}

impl DataSink for VecSink {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> CliResult<()> {
        self.rows.append(io_rows);
        Ok(())
    }
}

impl DynDataSink for VecSink {
    fn boxed(self) -> Box<dyn DynDataSink> {
        Box::new(self)
    }
}
