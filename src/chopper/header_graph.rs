use crate::chopper::chopper::{HeaderSink, MergeHeaderSink, SplitHeaderSink};
use crate::chopper::data_graph::{DataGraph, DataNode};
use crate::chopper::types::{ChainId, Header};
use crate::error::{CliResult, Error};

pub struct HeaderCountTracker {
    pub unprocessed_count: usize,
}

pub enum HeaderNode {
    HeaderSink(Box<dyn HeaderSink>),
    MergeHeaderSink(Box<dyn MergeHeaderSink>, HeaderCountTracker),
    SplitHeaderSink(Box<dyn SplitHeaderSink>, HeaderCountTracker),
    Merge(ChainId),
}

pub struct HeaderChain {
    nodes: Vec<HeaderNode>,
}

pub struct HeaderGraph {
    header_chains: Vec<HeaderChain>,
}

impl HeaderChain {
    pub fn new(nodes: Vec<HeaderNode>) -> Self {
        HeaderChain { nodes }
    }

    pub fn get_mut_nodes(&mut self) -> &mut Vec<HeaderNode> {
        &mut self.nodes
    }

    pub fn add_nodes(&mut self, node: HeaderNode) {
        self.nodes.push(node)
    }
}

impl HeaderGraph {
    pub fn new(header_chains: Vec<HeaderChain>) -> Self {
        HeaderGraph { header_chains }
    }

    pub fn len(&self) -> usize {
        self.header_chains.len()
    }

    pub fn get_mut_chain(&mut self, chain_id: ChainId) -> Option<&mut HeaderChain> {
        self.header_chains.get_mut(chain_id)
    }

    pub fn process_header(mut self, mut header: Vec<Header>) -> CliResult<DataGraph> {
        // initialize an empty data_graph
        let mut data_graph = DataGraph::new(self.header_chains.len());

        for i in 0..header.len() {
            self = self.process_header_re(&mut data_graph, i, header.get_mut(i).unwrap())?;
        }
        Ok(data_graph)
    }

    fn process_header_re(
        mut self,
        data_graph: &mut DataGraph,
        chain_id: ChainId,
        header: &mut Header,
    ) -> CliResult<Self> {
        let chain: &mut HeaderChain = self.header_chains.get_mut(chain_id).unwrap();
        // check the first node of the chain.
        if let HeaderNode::MergeHeaderSink(..) = chain.get_mut_nodes().get_mut(0).unwrap() {
            // if MergeHeaderSink, check/process the header without removing the chain
            // pass to match_remove_chain only when all the Merge headers are processed
            self.check_merge_header(data_graph, chain_id, header)
        } else {
            // if not, remove the chain, process header for all the nodes, and get DataSinks
            self.match_remove_chain(data_graph, chain_id, header)
        }
    }

    fn swap_remove(&mut self, chain_id: usize) -> HeaderChain {
        let empty_chain = HeaderChain::new(Vec::new());
        self.header_chains.push(empty_chain);
        self.header_chains.swap_remove(chain_id)
    }

    fn match_remove_chain(
        mut self,
        data_graph: &mut DataGraph,
        chain_id: ChainId,
        header: &mut Header,
    ) -> CliResult<Self> {
        let chain = self.swap_remove(chain_id);
        // process header for all the nodes in the chain, and get DataSinks
        for node in chain.nodes {
            let data_node: DataNode;
            match node {
                HeaderNode::HeaderSink(hs) => {
                    data_node = DataNode::DataSink(hs.process_header(header)?);
                    data_graph.add_node(data_node, chain_id).unwrap();
                }
                HeaderNode::MergeHeaderSink(mhs, _) => {
                    data_node = DataNode::DataSink(mhs.get_data_sink()?);
                    data_graph.add_node(data_node, chain_id)?;
                }
                HeaderNode::SplitHeaderSink(mut shs, mut header_count_tracker) => {
                    if header_count_tracker.unprocessed_count <= 0 {
                        return Err(Error::from(
                            "HeaderGraph -- unprocessed header count must be at least 1",
                        ));
                    }
                    for i in shs.chain_ids() {
                        self = self.match_remove_chain(data_graph, *i, &mut header.clone())?;
                        header_count_tracker.unprocessed_count -= 1;
                    }
                    data_node = DataNode::Split(shs.chain_ids().clone());
                    data_graph.add_node(data_node, chain_id)?;
                }
                // move to the chain that has MergeHeaderSink
                HeaderNode::Merge(new_chain_id) => {
                    data_graph.add_node(DataNode::Merge(new_chain_id), chain_id)?;
                    self = self.check_merge_header(data_graph, new_chain_id, header)?;
                }
            }
        }
        Ok(self)
    }

    fn check_merge_header(
        mut self,
        data_graph: &mut DataGraph,
        chain_id: ChainId,
        header: &mut Header,
    ) -> CliResult<Self> {
        let node: &mut HeaderNode = match self.get_mut_chain(chain_id) {
            Some(c) => c.get_mut_nodes().get_mut(0).unwrap(),
            None => {
                return Err(Error::from(format!(
                    "HeaderGraph -- ChainId[{}] index out of bounds",
                    chain_id
                )))
            }
        };
        match node {
            HeaderNode::MergeHeaderSink(mhs, header_count_tracker) => {
                if header_count_tracker.unprocessed_count <= 0 {
                    return Err(Error::from(
                        "HeaderGraph -- unprocessed header count must be at least 1",
                    ));
                }

                mhs.check_header(header)?;
                header_count_tracker.unprocessed_count -= 1;

                // finished processing all the headers
                // next get DataSink for MergeHeaderSink and remove Merge node
                if header_count_tracker.unprocessed_count == 0 {
                    let mut header = mhs.process_header();
                    self = self.match_remove_chain(data_graph, chain_id, &mut header)?
                }
            }
            _ => return Err(Error::from("HeaderGraph -- expected MergeHeaderSink")),
        };
        Ok(self)
    }
}
