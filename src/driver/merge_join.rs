use crate::chopper::chopper::{DataSink, MergeHeaderSink};
use crate::chopper::header_graph::{NumOfHeaderToProcess, PinId};
use crate::chopper::types::{Header, Row};
use crate::error::{CliResult, Error};

pub struct MergeJoin {
    input_pin_num: usize,
    header: Option<Header>,
}

impl MergeJoin {
    pub fn new(input_pin_num: usize) -> CliResult<Box<dyn MergeHeaderSink>> {
        if input_pin_num <= 0 {
            return Err(Error::from("MergeJoin -- number of inputs must be at least 1"));
        }
        let merge = MergeJoin { input_pin_num, header: None };
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
                // TODO: without cloning?
                if !header.eq(h) {
                    return Err(Error::from("MuxHeaderSink -- wrong header"));
                }
            }
            None => self.add_header(header)
        }
        Ok(())
    }

    fn process_header(&mut self) -> Header {
        self.header.take().unwrap()
    }

    fn get_data_sink(self: Box<Self>) -> CliResult<Box<dyn DataSink>> {
        if self.header.is_some() {
            return Err(Error::from(
                "MuxHeaderSink -- all the headers must be processed before returning DataSink"));
        }
        Ok(self.boxed())
    }

    fn pin_num(&self) -> usize {
        self.input_pin_num
    }

    fn num_of_header_to_process(&self) -> NumOfHeaderToProcess {
        NumOfHeaderToProcess { counter: self.pin_num() }
    }
}

impl DataSink for MergeJoin {
    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> CliResult<Option<Row>> {
        self.write_row(row)
    }

    fn flush(&mut self) -> CliResult<()> {
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
