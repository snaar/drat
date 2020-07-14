use crate::chopper::chopper::{ChopperDriver, Source};
use crate::chopper::data_graph::{DataGraph, DataNode};
use crate::chopper::header_graph::{ChainId, HeaderGraph, NodeId, PinId};
use crate::chopper::types::{Header, Row, TimestampRange};
use crate::driver::source_row_buffer::SourceRowBuffer;
use crate::error::{CliResult, Error};

pub struct Driver {
    sources: Vec<Box<dyn Source>>,
    data_graph: DataGraph,
    timestamp_range: TimestampRange,
}

impl Driver {
    pub fn new(
        sources: Vec<Box<dyn Source>>,
        header_graph: HeaderGraph,
        timestamp_range: TimestampRange,
        headers: Vec<Header>,
    ) -> CliResult<Self> {
        if sources.len() > header_graph.len() {
            return Err(Error::from(
                "Driver -- not enough header chains for sources. \
                each source should have at least one header chain.",
            ));
        }
        let data_graph = header_graph.process_header(headers)?;
        Ok(Driver {
            sources,
            data_graph,
            timestamp_range,
        })
    }

    fn drive(&mut self) -> CliResult<()> {
        let mut row_buffers = self.get_row_buffers()?;

        // sort and output
        // all the sources are processed at the same time, but a row with min timestamp is output first
        let mut buffer_len = row_buffers.len();
        while buffer_len > 0 {
            // get the row with min timestamp and write
            let buffer_index = Self::get_next_buffer_index(&row_buffers);
            let next_row_buffer = &mut row_buffers[buffer_index];
            let row = next_row_buffer.row().clone().unwrap();
            let chain_id = next_row_buffer.chain_id();
            Self::process_row(&mut self.data_graph, chain_id, 0, 0, row)?;

            // remove the row buffer if it reaches the end of the file
            loop {
                if !row_buffers[buffer_index].has_next(&self.timestamp_range)? {
                    self.flush(chain_id, 0)?;
                    row_buffers.remove(buffer_index);
                }
                break;
            }
            // update row buffer length
            buffer_len = row_buffers.len();
        }
        Ok(())
    }

    fn get_row_buffers(&mut self) -> CliResult<Vec<SourceRowBuffer>> {
        let mut row_buffers: Vec<SourceRowBuffer> = Vec::with_capacity(self.sources.len());
        for i in 0..self.sources.len() {
            let source = self.sources.pop().unwrap();
            row_buffers.push(SourceRowBuffer::new(source, i, &self.timestamp_range)?);
        }
        Ok(row_buffers)
    }

    // index of the row buffer that has a row with min timestamp
    fn get_next_buffer_index(row_buffers: &Vec<SourceRowBuffer>) -> usize {
        let min = row_buffers
            .iter()
            .enumerate()
            .min_by(|&(_, i1), &(_, i2)| i1.timestamp().cmp(&i2.timestamp()))
            .unwrap();
        min.0
    }

    fn process_row(
        data_graph: &mut DataGraph,
        mut chain_id: ChainId,
        mut node_id: NodeId,
        mut pin_id: PinId,
        mut row: Row,
    ) -> CliResult<()> {
        let chain = data_graph.get_mut_chain(chain_id);
        while node_id < chain.nodes().len() {
            match chain.node(node_id) {
                DataNode::DataSink(sink) => match sink.write_row_to_pin(pin_id, row)? {
                    Some(r) => {
                        row = r;
                        node_id += 1;
                    }
                    None => break,
                },
                DataNode::Merge(new_chain_id, new_pin_id) => {
                    chain_id = *new_chain_id;
                    node_id = 0;
                    pin_id = *new_pin_id;
                    Self::process_row(data_graph, chain_id, node_id, pin_id, row)?;
                    break;
                }
                DataNode::Split(chain_ids) => {
                    if pin_id < chain_ids.len() {
                        let new_chain_id = chain_ids[pin_id];
                        pin_id += 1;
                        Self::process_row(data_graph, new_chain_id, 0, pin_id, row.clone())?;
                        Self::process_row(data_graph, chain_id, 0, pin_id, row.clone())?;
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn flush(&mut self, mut chain_id: ChainId, mut pin_id: PinId) -> CliResult<()> {
        let mut node_id = 0;
        let chain = self.data_graph.get_mut_chain(chain_id);
        while chain.nodes().len() > node_id {
            match chain.node(node_id) {
                DataNode::DataSink(sink) => {
                    sink.flush()?;
                    node_id += 1
                }
                DataNode::Merge(new_chain_id, _pin_id) => {
                    chain_id = *new_chain_id;
                    self.flush(chain_id, 0)?;
                    break;
                }
                DataNode::Split(chain_ids) => {
                    if pin_id < chain_ids.len() {
                        let new_chain_id = chain_ids[pin_id];
                        pin_id += 1;
                        self.flush(new_chain_id, 0)?;
                        self.flush(chain_id, pin_id)?;
                    }
                    break;
                }
            }
        }
        Ok(())
    }
}

impl ChopperDriver for Driver {
    fn drive(&mut self) -> CliResult<()> {
        self.drive()
    }
}
