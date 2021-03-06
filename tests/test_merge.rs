use chrono_tz::America::New_York;

use chopper::chopper::driver::ChopperDriver;
use chopper::chopper::error::ChopperResult;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, Header};
use chopper::driver::driver::Driver;
use chopper::driver::merge_join::MergeJoin;
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
fn test_merge() {
    test().unwrap();
    assert!(are_contents_same(
        "./tests/output/test_merge.csv",
        "./tests/reference/test_merge.csv",
    )
    .unwrap());
}

fn test() -> ChopperResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> ChopperResult<Box<dyn ChopperDriver>> {
    let input_1 = "./tests/input/time_city.csv";
    let input_2 = "./tests/input/time_city.csv";
    let inputs = vec![input_1, input_2];
    let output = "./tests/output/test_merge.csv";

    // source reader and headers
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Name("DateTime".to_owned()),
        TimestampFmtConfig::Auto,
        ChopperTz::from(New_York),
    );
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

    // source chain 0
    let node_merge = HeaderNode::Merge(2);
    let chain_0 = HeaderChain::new(vec![node_merge]);

    // source chain 1
    let node_merge = HeaderNode::Merge(2);
    let chain_1 = HeaderChain::new(vec![node_merge]);

    // merge/sink chain 2
    let merge = MergeJoin::new(2)?;
    let header_count_tracker = merge.get_new_header_count_tracker();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, header_count_tracker);
    let header_sink = OutputFactory::new().new_header_sink(Some(output))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_2 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![chain_0, chain_1, chain_2]);

    Ok(Box::new(Driver::new(
        sources,
        graph,
        types::TIMESTAMP_RANGE_ALL,
        headers,
    )?))
}
