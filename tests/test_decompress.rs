use chrono_tz::America::New_York;

use chopper::chopper::driver::ChopperDriver;
use chopper::chopper::error::ChopperResult;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, Header};
use chopper::driver::driver::Driver;
use chopper::input::input_factory::InputFactoryBuilder;
use chopper::source::csv_input_config::CSVInputConfig;
use chopper::source::csv_timestamp_config::{
    TimestampColConfig, TimestampConfig, TimestampFmtConfig,
};
use chopper::source::source::Source;
use chopper::util::file::are_contents_same;
use chopper::util::tz::ChopperTz;
use chopper::write::factory::OutputFactory;

#[test]
fn test_decompress() {
    let input = "./tests/input/time_city.csv.gz";
    let inputs = vec![input];
    let output = "./tests/output/test_decompress.csv";
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Name("DateTime".to_owned()),
        TimestampFmtConfig::Auto,
        ChopperTz::from(New_York),
    );
    test(inputs, output, ts_config).unwrap();

    assert!(are_contents_same(output, "./tests/reference/test_decompress.csv").unwrap());
}

fn test(inputs: Vec<&str>, output: &str, ts_config: TimestampConfig) -> ChopperResult<()> {
    setup_graph(inputs, output, ts_config)?.drive()
}

fn setup_graph(
    inputs: Vec<&str>,
    output: &str,
    ts_config: TimestampConfig,
) -> ChopperResult<Box<dyn ChopperDriver>> {
    let csv_input_config = CSVInputConfig::new(ts_config);
    let mut input_factory = InputFactoryBuilder::new()
        .with_csv_input_config(csv_input_config)
        .build()?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    let header_sink = OutputFactory::new().new_header_sink(Some(output))?;
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
