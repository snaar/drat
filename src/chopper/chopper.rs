use std::fmt;

use crate::chopper::header_graph::{ChainId, NumOfHeaderToProcess, PinId};
use crate::chopper::types::{Header, Row};
use crate::error::CliResult;

pub trait ChopperDriver {
    fn drive(&mut self) -> CliResult<()>;
}

pub trait Source {
    fn header(&self) -> &Header;
    fn next_row(&mut self) -> CliResult<Option<Row>>;
    fn has_native_timestamp_column(&self) -> bool;
}

//TODO better debug format?
impl fmt::Debug for Source {
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

    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> CliResult<Option<Row>> {
        self.write_row(row)
    }

    fn flush(&mut self) -> CliResult<()>;

    fn finish(self: Box<Self>) -> CliResult<()> {
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink>;
}

pub trait MergeHeaderSink {
    fn check_header(&mut self, pin_id: PinId, header: &Header) -> CliResult<()>;
    fn process_header(&mut self) -> Header;
    fn get_data_sink(self: Box<Self>) -> CliResult<Box<dyn DataSink>>;
    fn pin_num(&self) -> usize;
    fn num_of_header_to_process(&self) -> NumOfHeaderToProcess;
}

pub trait SplitHeaderSink {
    fn chain_ids(&mut self) -> &mut Vec<ChainId>;
    fn num_of_header_to_process(&self) -> NumOfHeaderToProcess;
}
