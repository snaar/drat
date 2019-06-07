use std::fmt;

use crate::dr::graph::PinId;
use crate::dr::types::{Header, Row};
use crate::result::CliResult;

pub trait DRDriver {
    fn drive(&mut self);
}

pub trait Source {
    fn header(&self) -> &Header;
    fn next_row(&mut self) -> Option<Row>;
}

//TODO better debug format?
impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "source field names: {:?}", self.header().field_names())
    }
}

pub trait HeaderSink {
    fn process_header(self: Box<Self>, header: &mut Header) -> Box<dyn DataSink>;
}

pub trait DataSink {
    fn write_row(&mut self, row: Row) -> Option<Row> {
        Some(row)
    }
    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> Option<Row> {
        //TODO: check pin_id?
        self.write_row(row)
    }
    fn flush(&mut self) -> CliResult<()>;
    fn finish(self: Box<Self>) -> CliResult<()> {
        Ok(())
    }
    fn boxed(self) -> Box<dyn DataSink>;
}

pub trait MuxHeaderSink {
    fn check_header(&mut self, pin_id: PinId, header: & Header) -> CliResult<()>;
    fn process_header(&mut self) -> Header;
    fn get_data_sink(self: Box<Self>) -> Box<dyn DataSink>;
    fn pin_num(&self) -> usize;
}
