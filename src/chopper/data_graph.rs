use crate::chopper::chopper::DataSink;
use crate::chopper::types::{ChainId, PinId};
use crate::error::{CliResult, Error};

pub enum DataNode {
    DataSink(Box<dyn DataSink>),
    Merge(ChainId, PinId),
    Split(Vec<ChainId>),
}

pub struct DataChain {
    nodes: Vec<DataNode>,
}

pub struct DataGraph {
    data_chains: Vec<DataChain>,
}

impl DataChain {
    pub fn new() -> Self {
        let nodes: Vec<DataNode> = Vec::new();
        DataChain { nodes }
    }

    pub fn node(&mut self, node_id: usize) -> &mut DataNode {
        let node = self.nodes.get_mut(node_id).unwrap();
        node
    }

    pub fn nodes(&mut self) -> &mut Vec<DataNode> {
        &mut self.nodes
    }
}

impl DataGraph {
    pub fn new(chain_len: usize) -> Self {
        let mut data_chains: Vec<DataChain> = Vec::with_capacity(chain_len);
        for _i in 0..chain_len {
            data_chains.push(DataChain::new());
        }
        DataGraph { data_chains }
    }

    pub fn len(&self) -> usize {
        self.data_chains.len()
    }

    pub fn get_mut_chain(&mut self, chain_id: ChainId) -> &mut DataChain {
        self.data_chains.get_mut(chain_id).unwrap()
    }

    pub fn add_node(&mut self, node: DataNode, chain_id: ChainId) -> CliResult<()> {
        match self.data_chains.get_mut(chain_id) {
            Some(c) => c.nodes.push(node),
            None => {
                return Err(Error::from(format!(
                    "DataGraph -- index out of bound. \
                    ChainId: [{}], DataGraph size: [{:?}]",
                    chain_id,
                    self.data_chains.len()
                )))
            }
        };
        Ok(())
    }
}
