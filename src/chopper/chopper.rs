use std::fmt;

use crate::chopper::header_graph::HeaderCountTracker;
use crate::chopper::types::{ChainId, Header, Row};
use crate::error::CliResult;

pub trait ChopperDriver {
    fn drive(&mut self) -> CliResult<()>;
}

pub trait Source {
    fn header(&self) -> &Header;
    fn next_row(&mut self) -> CliResult<Option<Row>>;
}

//TODO better debug format?
impl fmt::Debug for dyn Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "source field names: {:?}", self.header().field_names())
    }
}

pub trait HeaderSink {
    fn process_header(self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>>;
}

pub trait DataSink {
    fn write_row(&mut self, row: Row) -> CliResult<Option<Row>> {
        Ok(Some(row))
    }

    fn flush(&mut self) -> CliResult<()> {
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink>;
}

pub trait MergeHeaderSink {
    fn check_header(&mut self, header: &Header) -> CliResult<()>;
    fn process_header(&mut self) -> Header;
    fn get_data_sink(self: Box<Self>) -> CliResult<Box<dyn DataSink>>;
    fn get_new_header_count_tracker(&self) -> HeaderCountTracker;
}

pub trait SplitHeaderSink {
    fn chain_ids(&mut self) -> &mut Vec<ChainId>;
    fn get_new_header_count_tracker(&self) -> HeaderCountTracker;
}
