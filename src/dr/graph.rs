use std::process;

use crate::dr::dr::{DataSink, HeaderSink, MuxHeaderSink};
use crate::dr::types::Header;
use crate::result::CliResult;

pub type ChainId = usize;
pub type NodeId = usize;
pub type PinId = usize;

// number of pin headers need to be processed for Mux
pub struct PinsWithoutHeader {
    pub counter: usize,
}

pub enum HeaderNode {
    HeaderSink(Box<dyn HeaderSink>),
    MuxHeaderSink(Box<dyn MuxHeaderSink>, PinsWithoutHeader),
    Merge(ChainId, PinId),
    Split(Vec<ChainId>)
}

pub enum DataNode {
    DataSink(Box<dyn DataSink>),
    Merge(ChainId, PinId),
    Split(Vec<ChainId>)
}

pub struct HeaderChain {
    nodes: Vec<HeaderNode>
}

pub struct DataChain {
    nodes: Vec<DataNode>
}

pub struct HeaderGraph {
    header_chains: Vec<HeaderChain>,
}

pub struct DataGraph {
    data_chains: Vec<DataChain>,
}

impl HeaderChain {
    pub fn new(nodes: Vec<HeaderNode>) -> Self {
        HeaderChain { nodes }
    }

    pub fn node(&mut self, node_id: usize) -> &mut HeaderNode {
        let node = self.nodes.get_mut(node_id).unwrap();
        node
    }

    pub fn nodes(&mut self) -> &mut Vec<HeaderNode> {
        &mut self.nodes
    }
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

impl HeaderGraph {
    pub fn new(header_chains: Vec<HeaderChain>) -> Self {
        HeaderGraph { header_chains }
    }

    pub fn add_chain(&mut self, chain: HeaderChain) -> CliResult<()> {
        self.header_chains.push(chain);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.header_chains.len()
    }

    pub fn get_mut_chain(&mut self, chain_id: ChainId) -> &mut HeaderChain {
        self.header_chains.get_mut(chain_id).unwrap()
    }

    pub fn process_header(mut self, mut header: Vec<Header>) -> DataGraph {
        // initialize an empty data_graph
        let mut data_graph = DataGraph::new(self.header_chains.len());

        for i in 0..header.len() {
            self = self.process_header_re(&mut data_graph, i, 0, header.get_mut(i).unwrap());
        }
        data_graph
    }

    fn process_header_re(mut self, data_graph: &mut DataGraph, chain_id: ChainId, pin_id: PinId, header: &mut Header) -> Self {
        let chain: &mut HeaderChain = self.header_chains.get_mut(chain_id).unwrap();
        // check the first node of the chain.
        if let HeaderNode::MuxHeaderSink(..) = chain.nodes().get_mut(0).unwrap() {
            // if Mux, check/process the header without removing the chain.
            // pass to match_remove_node only when all the Mux pin headers are processed.
            self.check_mux_header(data_graph, chain_id, pin_id, header)
        } else {
            // if not Mux, remove the chain, process header for all the nodes, and get DataSinks.
            self.match_remove_chain(data_graph, chain_id, pin_id, header)
        }
    }

    fn swap_remove(&mut self, chain_id: usize) -> HeaderChain {
        let empty_chain = HeaderChain::new(Vec::new());
        self.header_chains.push(empty_chain);
        self.header_chains.swap_remove(chain_id)
    }

    fn match_remove_chain(mut self, data_graph: &mut DataGraph, chain_id: ChainId, _pin_id: PinId, header: &mut Header) -> Self {
        let chain = self.swap_remove(chain_id);

        // process header for all the nodes in the chain, and get DataSinks.
        for node in chain.nodes {
            let data_node: DataNode;
            match node {
                HeaderNode::HeaderSink(hs) => {
                    data_node = DataNode::DataSink(hs.process_header(header));
                    data_graph.add_node(data_node, chain_id).unwrap();
                },
                HeaderNode::MuxHeaderSink(mhs, _c) => {
                    data_node = DataNode::DataSink(mhs.get_data_sink());
                    data_graph.add_node(data_node, chain_id).unwrap();
                },
                // move to the chain that has MuxHeaderSink
                HeaderNode::Merge(new_chain_id, new_pin_id) => {
                    data_graph.add_node(DataNode::Merge(new_chain_id, new_pin_id), chain_id).unwrap();
                    self = self.check_mux_header(data_graph, new_chain_id, new_pin_id, header);
                },
                HeaderNode::Split(..) => write_error!("Error: Split hasn't been implemented in graph."),
            }
        }
        self
    }

    fn check_mux_header(mut self, data_graph: &mut DataGraph, chain_id: ChainId, pin_id: PinId, header: &mut Header) -> Self {
        let node: &mut HeaderNode = self.get_mut_chain(chain_id).nodes.get_mut(0).unwrap();
        match node {
            HeaderNode::MuxHeaderSink(mhs, c) => {
                if c.counter == 0 {
                    write_error!("Error: MuxHeaderSink should have at least one PinsWithoutHeaderCounter");
                }
                mhs.check_header(pin_id, header).unwrap();
                c.counter = c.counter-1;
                // finished processing all the pin headers. next get DataSink for Mux and remove Mux node.
                if c.counter == 0 {
                    let mut header = mhs.process_header();
                    self = self.match_remove_chain(data_graph, chain_id, pin_id, &mut header)
                }
            },
            _ => write_error!("Error: wrong node passed to check_mux_header.")
        };
        self
    }
}

impl DataGraph {
    fn new(chain_len: usize) -> Self {
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

    fn add_node(&mut self, node: DataNode, chain_id: ChainId) -> CliResult<()> {
        self.data_chains.get_mut(chain_id).unwrap_or_else(|| { write_error!("Error: wrong chain len for data graph.");
        }).nodes.push(node);
        Ok(())
    }
}
