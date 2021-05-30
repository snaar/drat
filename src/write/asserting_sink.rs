use crate::chopper::error::ChopperResult;
use crate::chopper::sink::{DataSink, DynHeaderSink};
use crate::chopper::types::{Header, Row};

pub struct AssertingSink {
    header: Header,
    rows: Vec<Row>,
    current_row: usize,
}

impl AssertingSink {
    pub fn new(header: Header, rows: Vec<Row>) -> AssertingSink {
        AssertingSink {
            header,
            rows,
            current_row: 0,
        }
    }
}

impl DynHeaderSink for AssertingSink {
    fn process_header(self: Box<Self>, header: &mut Header) -> ChopperResult<Box<dyn DataSink>> {
        assert_eq!(header, &self.header);
        Ok(Box::new(*self))
    }
}

impl DataSink for AssertingSink {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> ChopperResult<()> {
        assert_ne!(self.rows.len(), self.current_row);
        assert_eq!(io_rows.len(), 1);
        assert_eq!(io_rows[0], self.rows[self.current_row]);
        self.current_row += 1;
        Ok(())
    }

    fn flush(&mut self) -> ChopperResult<()> {
        assert_eq!(self.rows.len(), self.current_row);
        Ok(())
    }
}
