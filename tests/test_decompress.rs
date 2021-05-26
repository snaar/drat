use chrono_tz::America::New_York;

use chopper::chopper::chopper::ChopperDriver;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, Header};
use chopper::cli::util::YesNoAuto;
use chopper::driver::driver::Driver;
use chopper::error::CliResult;
use chopper::input::input_factory::InputFactory;
use chopper::source::csv_configs::{CSVInputConfig, CSVOutputConfig, TimestampFmtConfig};
use chopper::source::csv_configs::{TimestampColConfig, TimestampConfig};
use chopper::source::source::Source;
use chopper::util::file::are_contents_same;
use chopper::util::tz::ChopperTz;
use chopper::write::factory;

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

fn test(inputs: Vec<&str>, output: &str, ts_config: TimestampConfig) -> CliResult<()> {
    setup_graph(inputs, output, ts_config)?.drive()
}

fn setup_graph(
    inputs: Vec<&str>,
    output: &str,
    ts_config: TimestampConfig,
) -> CliResult<Box<dyn ChopperDriver>> {
    let csv_input_config = CSVInputConfig::new(None, YesNoAuto::Auto, ts_config)?;
    let mut input_factory = InputFactory::new(csv_input_config, None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output), Some(csv_output_config))?;
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
