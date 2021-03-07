use crate::chopper::chopper::{DataSink, MergeHeaderSink};
use crate::chopper::header_graph::HeaderCountTracker;
use crate::chopper::types::{Header, PinId};
use crate::error::{CliResult, Error};

pub struct MergeJoin {
    input_pin_count: usize,
    header: Option<Header>,
}

impl MergeJoin {
    pub fn new(input_pin_count: usize) -> CliResult<Box<dyn MergeHeaderSink>> {
        if input_pin_count <= 0 {
            return Err(Error::from(
                "MergeJoin -- number of inputs must be at least 1",
            ));
        }
        let merge = MergeJoin {
            input_pin_count,
            header: None,
        };
        Ok(Box::new(merge) as Box<dyn MergeHeaderSink>)
    }

    fn add_header(&mut self, header: &Header) {
        self.header = Some(header.clone());
    }
}

impl MergeHeaderSink for MergeJoin {
    fn check_header(&mut self, _pin_id: PinId, header: &Header) -> CliResult<()> {
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
        Ok(self.boxed())
    }

    fn get_new_header_count_tracker(&self) -> HeaderCountTracker {
        HeaderCountTracker {
            unprocessed_count: self.input_pin_count,
        }
    }
}

impl DataSink for MergeJoin {
    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
