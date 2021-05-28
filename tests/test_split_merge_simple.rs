use chrono_tz::America::New_York;

use chopper::chopper::driver::ChopperDriver;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, ChainId, Header};
use chopper::driver::driver::Driver;
use chopper::driver::merge_join::MergeJoin;
use chopper::driver::split::Split;
use chopper::error::CliResult;
use chopper::input::input_factory::InputFactory;
use chopper::source::csv_configs::{CSVOutputConfig, TimestampFmtConfig};
use chopper::source::csv_configs::{TimestampColConfig, TimestampConfig};
use chopper::source::csv_input_config::CSVInputConfig;
use chopper::source::source::Source;
use chopper::util::file::are_contents_same;
use chopper::util::tz::ChopperTz;
use chopper::write::factory;

#[test]
fn test_split_merge_simple() {
    test().unwrap();
    assert!(are_contents_same(
        "./tests/output/test_split_merge_simple.csv",
        "./tests/reference/test_split_merge_simple.csv",
    )
    .unwrap());
}

fn test() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input = "./tests/input/time_city.csv";
    let inputs = vec![input];
    let output = "./tests/output/test_split_merge_simple.csv";

    // source reader and headers
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Name("DateTime".to_owned()),
        TimestampFmtConfig::Auto,
        ChopperTz::from(New_York),
    );
    let csv_input_config = CSVInputConfig::new(ts_config);
    let mut input_factory = InputFactory::new(csv_input_config, None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    /*
            ┌─► chain1 ─┐
    chain0 ─┤           ├─► chain3
            └─► chain2 ─┘
    */

    // chain 0 - split single input into two
    let split_targets: Vec<ChainId> = vec![1, 2];
    let split = Split::new(split_targets);
    let header_count_tracker = split.get_new_header_count_tracker();
    let node_split_sink = HeaderNode::SplitHeaderSink(split, header_count_tracker);
    let chain_0 = HeaderChain::new(vec![node_split_sink]);

    // chain 1 - merge back into chain 3
    let node_merge = HeaderNode::Merge(3);
    let chain_1 = HeaderChain::new(vec![node_merge]);

    // chain 2 - merge back into chain 3
    let node_merge = HeaderNode::Merge(3);
    let chain_2 = HeaderChain::new(vec![node_merge]);

    // chain 3 - accept merge from chain 1 and 2 and write result out
    let merge = MergeJoin::new(2)?;
    let header_count_tracker = merge.get_new_header_count_tracker();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, header_count_tracker);
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_3 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![chain_0, chain_1, chain_2, chain_3]);

    Ok(Box::new(Driver::new(
        sources,
        graph,
        types::TIMESTAMP_RANGE_ALL,
        headers,
    )?))
}
