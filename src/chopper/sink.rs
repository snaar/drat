use crate::chopper::error::ChopperResult;
use crate::chopper::header_graph::HeaderCountTracker;
use crate::chopper::types::{ChainId, Header, Row};

pub trait TypedHeaderSink<D: DataSink> {
    fn process_header(self, header: &mut Header) -> ChopperResult<D>;
}

pub trait DynHeaderSink {
    fn process_header(self: Box<Self>, header: &mut Header) -> ChopperResult<Box<dyn DataSink>>;
}

pub trait DataSink {
    /// io_rows vec is used as both input and output;
    /// io_rows vec is guaranteed to have exactly 1 element as input;
    /// data sink impl is not constrained on number of elements in io_rows for output purposes by
    /// explicitly allowing 0, 1, or more elements to be output and passed to the next
    /// node in chain;
    /// data sink impl can mutate the given row for output or provide its own instead;
    /// default implementation simply leaves the input row unchanged
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> ChopperResult<()>;

    fn flush(&mut self) -> ChopperResult<()> {
        Ok(())
    }
}

pub trait MergeHeaderSink {
    fn check_header(&mut self, header: &Header) -> ChopperResult<()>;
    fn process_header(&mut self) -> Header;
    fn get_data_sink(self: Box<Self>) -> ChopperResult<Box<dyn DataSink>>;
    fn get_new_header_count_tracker(&self) -> HeaderCountTracker;
}

pub trait SplitHeaderSink {
    fn chain_ids(&mut self) -> &mut Vec<ChainId>;
    fn get_new_header_count_tracker(&self) -> HeaderCountTracker;
}
