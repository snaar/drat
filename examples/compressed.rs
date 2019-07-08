use chopper_lib::chopper::chopper::{ChDriver, Source};
use chopper_lib::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{self, Header};
use chopper_lib::driver::driver::Driver;
use chopper_lib::error::{self, CliResult};
use chopper_lib::source::{csv_config::CSVConfig, source_factory::BosuSourceFactory};
use chopper_lib::transport::file::FileInput;
use chopper_lib::transport::http::Http;
use chopper_lib::transport::transport_factory::TransportFactory;
use chopper_lib::write::factory;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let transport_factories: Vec<Box<TransportFactory>> = vec![Box::new(http), Box::new(file)];
    error::handle_drive_error(compressed_example(transport_factories));
}

fn compressed_example(transport_factories: Vec<Box<TransportFactory>>) -> CliResult<()> {
    let mut driver = setup_graph(transport_factories)?;
    driver.drive()
}

fn setup_graph(transport_factories: Vec<Box<TransportFactory>>) -> CliResult<Box<ChDriver>> {
    let csv_config = CSVConfig::new(",", true, 0, true)?;
    let input = "./examples/files/uspop_time.csv.gz";
    let inputs = vec![input];
    let output = None;

    let mut bosu_source_factory
        = BosuSourceFactory::new(Some(csv_config), None, transport_factories)?;
    let mut sources: Vec<Box<Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source
            = bosu_source_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    let header_sink = factory::new_header_sink(output)?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node_output]);

    let graph = HeaderGraph::new(vec![chain]);
    Ok(Box::new(
        Driver::new(sources, graph, types::DATA_RANGE_DEFAULT, headers)?))
}
