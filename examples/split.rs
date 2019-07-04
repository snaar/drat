extern crate chopper_lib;

use chopper_lib::chopper::chopper::{ChDriver, Source};
use chopper_lib::chopper::header_graph::{ChainId, HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{self, Header};
use chopper_lib::driver::{driver::Driver, split::Split};
use chopper_lib::error::{self, CliResult};
use chopper_lib::source::{source_factory::BosuSourceFactory};
use chopper_lib::transport::transport_factory::TransportFactory;
use chopper_lib::transport::file::FileInput;
use chopper_lib::transport::http::Http;
use chopper_lib::write::factory;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let transport_factories: Vec<Box<TransportFactory>> = vec![Box::new(http), Box::new(file)];
    error::handle_drive_error(split(transport_factories));
}

fn split(transport_factories: Vec<Box<TransportFactory>>) -> CliResult<()> {
    let mut driver = setup_test_graph(transport_factories)?;
    driver.drive()
}

fn setup_test_graph(transport_factories: Vec<Box<TransportFactory>>) -> CliResult<Box<ChDriver>> {
    let input = "./examples/files/hundred.dc";
    let inputs = vec![input];
    let output_1 = None;
    let output_2 = None;

    // source reader and headers
    let mut bosu_source_factory
        = BosuSourceFactory::new(None, None, transport_factories)?;
    let mut sources: Vec<Box<Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = bosu_source_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // filter and split chain 1
    let chain_ids: Vec<ChainId> = vec![1, 2];
    let split = Split::new(chain_ids);
    let header_to_process = split.num_of_header_to_process();
    let node_split_sink = HeaderNode::SplitHeaderSink(split, header_to_process);
    let chain_1 = HeaderChain::new(vec![node_split_sink]);

    // sink chain 2
    let header_sink_1 = factory::new_header_sink(output_1)?;
    let node_output_1 = HeaderNode::HeaderSink(header_sink_1);
    let chain_2 = HeaderChain::new(vec![node_output_1]);

    // sink chain 3
    let header_sink_2 = factory::new_header_sink(output_2)?;
    let node_output_2 = HeaderNode::HeaderSink(header_sink_2);
    let chain_3 = HeaderChain::new(vec![node_output_2]);

    let graph = HeaderGraph::new(vec![chain_1, chain_2, chain_3]);
    Ok(Box::new(
        Driver::new(sources, graph, types::DATA_RANGE_DEFAULT, headers)?))
}
