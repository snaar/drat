use crate::chopper::header_graph::HeaderCountTracker;
use crate::chopper::sink::{DataSink, MergeHeaderSink};
use crate::chopper::types::{Header, Row};
use crate::error::{CliResult, Error};

pub struct MergeJoin {
    merge_source_count: usize,
    header: Option<Header>,
}

impl MergeJoin {
    pub fn new(merge_source_count: usize) -> CliResult<Box<dyn MergeHeaderSink>> {
        if merge_source_count <= 0 {
            return Err(Error::from(
                "MergeJoin -- number of inputs must be at least 1",
            ));
        }
        let merge = MergeJoin {
            merge_source_count,
            header: None,
        };
        Ok(Box::new(merge) as Box<dyn MergeHeaderSink>)
    }

    fn add_header(&mut self, header: &Header) {
        self.header = Some(header.clone());
    }
}

impl MergeHeaderSink for MergeJoin {
    fn check_header(&mut self, header: &Header) -> CliResult<()> {
        match &self.header {
            Some(h) => {
                if !header.eq(h) {
                    return Err(Error::from("MuxHeaderSink -- wrong header"));
                }
            }
            None => self.add_header(header),
        }
        Ok(())
    }

    fn process_header(&mut self) -> Header {
        self.header.take().unwrap()
    }

    fn get_data_sink(self: Box<Self>) -> CliResult<Box<dyn DataSink>> {
        if self.header.is_some() {
            return Err(Error::from(
                "MuxHeaderSink -- all the headers must be processed before returning DataSink",
            ));
        }
        Ok(Box::new(*self))
    }

    fn get_new_header_count_tracker(&self) -> HeaderCountTracker {
        HeaderCountTracker {
            unprocessed_count: self.merge_source_count,
        }
    }
}

//TODO figure out if this even needs to be a DataSink
impl DataSink for MergeJoin {
    fn write_row(&mut self, _io_rows: &mut Vec<Row>) -> CliResult<()> {
        Ok(())
    }
}
