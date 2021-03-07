use crate::chopper::chopper::SplitHeaderSink;
use crate::chopper::header_graph::HeaderCountTracker;
use crate::chopper::types::ChainId;

pub struct Split {
    chain_ids: Vec<ChainId>,
}

impl Split {
    pub fn new(chain_ids: Vec<ChainId>) -> Box<dyn SplitHeaderSink> {
        let split = Split { chain_ids };
        Box::new(split) as Box<dyn SplitHeaderSink>
    }
}

impl SplitHeaderSink for Split {
    fn chain_ids(&mut self) -> &mut Vec<usize> {
        &mut self.chain_ids
    }

    fn get_new_header_count_tracker(&self) -> HeaderCountTracker {
        HeaderCountTracker {
            unprocessed_count: self.chain_ids.len(),
        }
    }
}
