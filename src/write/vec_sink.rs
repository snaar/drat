use crate::chopper::error::ChopperResult;
use crate::chopper::sink::{DataSink, DynHeaderSink, TypedHeaderSink};
use crate::chopper::types::{Header, Row};

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

impl TypedHeaderSink<Self> for VecSink {
    fn process_header(mut self, header: &mut Header) -> ChopperResult<Self> {
        self.header = Some(header.clone());
        Ok(self)
    }
}

impl DynHeaderSink for VecSink {
    fn process_header(
        mut self: Box<Self>,
        header: &mut Header,
    ) -> ChopperResult<Box<dyn DataSink>> {
        self.header = Some(header.clone());
        Ok(self)
    }
}

impl DataSink for VecSink {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> ChopperResult<()> {
        self.rows.append(io_rows);
        Ok(())
    }
}
