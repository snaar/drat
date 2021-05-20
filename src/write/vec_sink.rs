use crate::chopper::chopper::{DataSink, HeaderSink};
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

impl HeaderSink for VecSink {
    fn process_header(mut self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>> {
        self.header = Some(header.clone());
        Ok(self)
    }
}

impl DataSink for VecSink {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> CliResult<()> {
        self.rows.append(io_rows);
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
