use chopper::chopper::driver::ChopperDriver;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, Header};
use chopper::driver::driver::Driver;
use chopper::error::CliResult;
use chopper::input::input_factory::InputFactory;
use chopper::source::csv_configs::{
    CSVOutputConfig, TimestampColConfig, TimestampConfig, TimestampFmtConfig,
};
use chopper::source::csv_input_config::CSVInputConfig;
use chopper::source::source::Source;
use chopper::util::tz::ChopperTz;
use chopper::write::factory;

fn main() -> CliResult<()> {
    setup_compressed_example_graph()?.drive()
}

fn setup_compressed_example_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Index(0),
        TimestampFmtConfig::Auto,
        ChopperTz::new_always_fails(),
    );
    let csv_config = CSVInputConfig::new(ts_config);
    let input = "./examples/files/uspop_time.csv.gz";
    let inputs = vec![input];
    let output = None;

    let mut input_factory = InputFactory::new(csv_config, None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(output, Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node_output]);

    let graph = HeaderGraph::new(vec![chain]);
    Ok(Box::new(Driver::new(
        sources,
        graph,
        types::TIMESTAMP_RANGE_ALL,
        headers,
    )?))
}
