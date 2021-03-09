use crate::chopper::chopper::{ChopperDriver, Source};
use crate::chopper::data_graph::{DataGraph, DataNode};
use crate::chopper::header_graph::HeaderGraph;
use crate::chopper::types::{ChainId, Header, NodeId, Row, TimestampRange};
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
            self.process_row(chain_id, 0, row)?;

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

    fn process_row(&mut self, chain_id: ChainId, node_id: NodeId, mut row: Row) -> CliResult<()> {
        let chain_node_count = self.data_graph.get_chain_node_count(chain_id);
        for node_id in node_id..chain_node_count {
            match self.data_graph.get_chain_node_mut(chain_id, node_id) {
                DataNode::DataSink(sink) => match sink.write_row(row)? {
                    Some(r) => {
                        row = r;
                    }
                    None => return Ok(()),
                },
                DataNode::Merge(next_chain_id) => {
                    let next_chain_id = *next_chain_id;
                    return self.process_row(next_chain_id, 0, row);
                }
                DataNode::Split(chain_ids) => {
                    for next_chain_id in chain_ids.clone() {
                        self.process_row(next_chain_id, 0, row.clone())?;
                    }
                    // that's right, continue processing current chain
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self, chain_id: ChainId, node_id: NodeId) -> CliResult<()> {
        let chain_node_count = self.data_graph.get_chain_node_count(chain_id);
        for node_id in node_id..chain_node_count {
            match self.data_graph.get_chain_node_mut(chain_id, node_id) {
                DataNode::DataSink(sink) => {
                    sink.flush()?;
                }
                DataNode::Merge(next_chain_id) => {
                    let next_chain_id = *next_chain_id;
                    return self.flush(next_chain_id, 0);
                }
                DataNode::Split(chain_ids) => {
                    for next_chain_id in chain_ids.clone() {
                        self.flush(next_chain_id, 0)?;
                    }
                    // that's right, continue processing current chain
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
