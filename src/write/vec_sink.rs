use crate::chopper::sink::{DataSink, DynHeaderSink, TypedDataSink, TypedHeaderSink};
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

impl TypedHeaderSink<Self, Self> for VecSink {
    fn process_header(mut self, header: &mut Header) -> CliResult<Self> {
        self.header = Some(header.clone());
        Ok(self)
    }
}

impl TypedDataSink<Self> for VecSink {
    fn inner(self) -> VecSink {
        self
    }
}

impl DynHeaderSink for VecSink {
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
}
