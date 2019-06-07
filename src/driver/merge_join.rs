use std::process;
use crate::dr::dr::{DataSink, MuxHeaderSink};
use crate::dr::graph::PinId;
use crate::dr::types::{Header, Row};
use crate::result::{CliResult};

pub struct MergeJoin {
    input_pin_num: usize,
    header: Option<Header>,
}

impl MergeJoin {
    pub fn new(input_num: usize) -> Box<dyn MuxHeaderSink> {
        let merge = MergeJoin { input_pin_num: input_num, header: None };
        Box::new(merge) as Box<dyn MuxHeaderSink>
    }

    fn add_header(&mut self, header: &Header) -> CliResult<()> {
        self.header = Some(header.clone());
        Ok(())
    }
}

impl MuxHeaderSink for MergeJoin {
    fn check_header(&mut self, _pin_id: PinId, header: &Header) -> CliResult<()> {
        match &self.header {
            Some(h) => {
                // TODO: without cloning?
                if !header.eq(h) {
                    write_error!("Error: wrong header passed to MuxHeaderSink.");
                }
            }
            None => self.add_header(header)?
        }
        Ok(())
    }

    fn process_header(&mut self) -> Header {
        self.header.take().unwrap()
    }

    fn get_data_sink(self: Box<Self>) -> Box<dyn DataSink> {
        if self.header.is_some() {
            write_error!("Error: MergeJoin -- all the headers must be processed before returning DataSink");
        }
        self.boxed()
    }

    fn pin_num(&self) -> usize {
        self.input_pin_num
    }
}

impl DataSink for MergeJoin {
    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> Option<Row> {
        self.write_row(row)
    }

    fn flush(&mut self) -> CliResult<()> {
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
