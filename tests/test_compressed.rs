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
fn test_compressed() {
    setup_compressed_test_graph().unwrap().drive().unwrap();
    assert!(are_contents_same(
        "./tests/output/test_compressed.csv",
        "./tests/reference/test_compressed.csv",
    )
    .unwrap());
}

fn setup_compressed_test_graph() -> ChopperResult<Box<dyn ChopperDriver>> {
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Index(0),
        TimestampFmtConfig::Auto,
        ChopperTz::new_from_str("America/New_York", None)?,
    );
    let csv_input_config = CSVInputConfig::new(ts_config);
    let input = "./tests/input/uspop_time.csv.gz";
    let inputs = vec![input];
    let output = Some("./tests/output/test_compressed.csv");

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

    let header_sink = OutputFactory::new().new_header_sink(output)?;
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
