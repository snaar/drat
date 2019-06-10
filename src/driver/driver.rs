use std::process;
use crate::args;
use crate::dr::dr::{DRDriver, Source};
use crate::dr::graph::{ChainId, DataGraph, HeaderGraph, DataNode, NodeId, PinId};
use crate::dr::types::{Header, Row};
use crate::driver::source_row_buffer::SourceRowBuffer;
use crate::result::{self, CliResult};

pub struct Driver {
    sources: Vec<Box<Source>>,
    data_graph: DataGraph,
    date_range: args::DataRange,
}

impl Driver {
    pub fn new(sources: Vec<Box<Source>>, header_graph: HeaderGraph, date_range: args::DataRange, headers: Vec<Header>) -> Self {
        let data_graph = header_graph.process_header(headers);
        Driver { sources, data_graph, date_range }
    }

    // all the sources are processed at the same time, but a row with min timestamp is output first.
    fn drive(&mut self) -> CliResult<()> {
        if self.sources.len() > self.data_graph.len() {
            write_error!("Error: GenericDriver - the number of sources and the number of sinks don't match.");
        }
        let mut row_buffers = self.get_row_buffers();

        // sort and output
        let mut buffer_len = row_buffers.len();
        while buffer_len > 0 {
            // get the row with min timestamp and write
            let buffer_index = Self::get_next_buffer_index(&row_buffers);
            let next_row_buffer = &mut row_buffers[buffer_index];
            let row = next_row_buffer.row().clone().unwrap();
            let chain_id = next_row_buffer.chain_id();
            self.process_row(chain_id, 0, 0, row)?;

            // remove the row buffer if it reaches the end of the file
            loop {
                if !row_buffers[buffer_index].next(&self.date_range) {
                    self.flush(chain_id)?;
                    row_buffers.remove(buffer_index);
                }
                break;
            }
            // update row buffer length
            buffer_len = row_buffers.len();
         }
        Ok(())
    }

    fn get_row_buffers(&mut self) -> Vec<SourceRowBuffer> {
        let mut row_buffers: Vec<SourceRowBuffer> = Vec::with_capacity(self.sources.len());
        for i in 0..self.sources.len() {
            let source = self.sources.pop().unwrap();
            row_buffers.push(SourceRowBuffer::new(source, i));
        }
        row_buffers
    }

    // index of the row buffer that has a row with min timestamp
    fn get_next_buffer_index(row_buffers: &Vec<SourceRowBuffer>) -> usize {
        let min = row_buffers
            .iter()
            .enumerate()
            .min_by(|&(_, i1), &(_, i2)|
                i1.timestamp().cmp(&i2.timestamp())).unwrap();
        min.0
    }

    fn process_row(&mut self, mut chain_id: ChainId, mut node_id: NodeId, mut pin_id: PinId, mut row: Row) -> CliResult<()> {
        let chain = self.data_graph.get_mut_chain(chain_id);
        while node_id < chain.nodes().len() {
            match chain.node(node_id) {
                DataNode::DataSink(sink) => {
                    match sink.write_row_to_pin(pin_id, row) {
                        Some(r) => {
                            row = r;
                            node_id += 1;
                        },
                        None => break
                    }
                }
                DataNode::Merge(new_chain_id, new_pin_id) => {
                    chain_id = *new_chain_id;
                    node_id = 0;
                    pin_id = *new_pin_id;
                    self.process_row(chain_id, node_id, pin_id, row)?;
                    break
                }
                DataNode::Split(..) => write_error!("Error: Split hasn't been implemented yet."),
            }
        }
        Ok(())
    }

    pub fn flush(&mut self, mut chain_id: ChainId) -> CliResult<()> {
        let mut node_id = 0;
        loop {
            let chain = self.data_graph.get_mut_chain(chain_id);
            if chain.nodes().len() <= node_id {
                break
            }
            match chain.node(node_id) {
                DataNode::DataSink(sink) => {
                    sink.flush()?;
                    node_id += 1
                },
                DataNode::Merge(new_chain_id, _pin_id) => {
                    chain_id = *new_chain_id;
                    node_id = 0
                },
                DataNode::Split(..) => write_error!("Error: Split hasn't been implemented yet."),
            }
        }
        Ok(())
    }
}

impl DRDriver for Driver {
    fn drive(&mut self) {
        result::handle_drive_error(self.drive())
    }
}
