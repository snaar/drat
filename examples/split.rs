use chopper::chopper::chopper::{ChopperDriver, Source};
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, ChainId, Header};
use chopper::driver::{driver::Driver, split::Split};
use chopper::error::{self, CliResult};
use chopper::input::input_factory::InputFactory;
use chopper::write::factory;

fn main() {
    error::handle_drive_error(split());
}

fn split() -> CliResult<()> {
    setup_test_graph()?.drive()
}

fn setup_test_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input = "./examples/files/hundred.dc";
    let inputs = vec![input];
    let output_1 = None;
    let output_2 = None;

    // source reader and headers
    let mut input_factory = InputFactory::new_without_csv(None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // filter and split chain 0
    let chain_ids: Vec<ChainId> = vec![1, 2];
    let split = Split::new(chain_ids);
    let header_count_tracker = split.get_new_header_count_tracker();
    let node_split_sink = HeaderNode::SplitHeaderSink(split, header_count_tracker);
    let chain_0 = HeaderChain::new(vec![node_split_sink]);

    // sink chain 1
    let header_sink_1 = factory::new_header_sink(output_1, None)?;
    let node_output_1 = HeaderNode::HeaderSink(header_sink_1);
    let chain_1 = HeaderChain::new(vec![node_output_1]);

    // sink chain 2
    let header_sink_2 = factory::new_header_sink(output_2, None)?;
    let node_output_2 = HeaderNode::HeaderSink(header_sink_2);
    let chain_2 = HeaderChain::new(vec![node_output_2]);

    let graph = HeaderGraph::new(vec![chain_0, chain_1, chain_2]);
    Ok(Box::new(Driver::new(
        sources,
        graph,
        types::TIMESTAMP_RANGE_ALL,
        headers,
    )?))
}
