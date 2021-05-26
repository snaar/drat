use chrono_tz::America::New_York;

use chopper::chopper::chopper::ChopperDriver;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, ChainId, Header};
use chopper::cli::util::YesNoAuto;
use chopper::driver::driver::Driver;
use chopper::driver::merge_join::MergeJoin;
use chopper::driver::split::Split;
use chopper::error::CliResult;
use chopper::input::input_factory::InputFactory;
use chopper::source::csv_configs::{CSVInputConfig, CSVOutputConfig, TimestampFmtConfig};
use chopper::source::csv_configs::{TimestampColConfig, TimestampConfig};
use chopper::source::source::Source;
use chopper::util::file::are_contents_same;
use chopper::util::tz::ChopperTz;
use chopper::write::factory;

#[test]
fn test_split_merge_complex() {
    test().unwrap();
    assert!(are_contents_same(
        "./tests/output/test_split_merge_complex_chain2.csv",
        "./tests/reference/test_split_merge_complex_chain2.csv",
    )
    .unwrap());
    assert!(are_contents_same(
        "./tests/output/test_split_merge_complex_chain3.csv",
        "./tests/reference/test_split_merge_complex_chain3.csv",
    )
    .unwrap());
    assert!(are_contents_same(
        "./tests/output/test_split_merge_complex_chain4.csv",
        "./tests/reference/test_split_merge_complex_chain4.csv",
    )
    .unwrap());
    assert!(are_contents_same(
        "./tests/output/test_split_merge_complex_chain5.csv",
        "./tests/reference/test_split_merge_complex_chain5.csv",
    )
    .unwrap());
    assert!(are_contents_same(
        "./tests/output/test_split_merge_complex_chain7.csv",
        "./tests/reference/test_split_merge_complex_chain7.csv",
    )
    .unwrap());
}

fn test() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input = "./tests/input/time_city.csv";
    let inputs = vec![input];
    let output2 = "./tests/output/test_split_merge_complex_chain2.csv";
    let output3 = "./tests/output/test_split_merge_complex_chain3.csv";
    let output4 = "./tests/output/test_split_merge_complex_chain4.csv";
    let output5 = "./tests/output/test_split_merge_complex_chain5.csv";
    let output7 = "./tests/output/test_split_merge_complex_chain7.csv";

    // source reader and headers
    let ts_config = TimestampConfig::new(
        TimestampColConfig::Name("DateTime".to_owned()),
        TimestampFmtConfig::Auto,
        ChopperTz::from(New_York),
    );
    let csv_input_config = CSVInputConfig::new(None, YesNoAuto::Auto, ts_config)?;
    let mut input_factory = InputFactory::new(csv_input_config, None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    /*
                                         ┌─► chain3 (file)
                                         │
             ┌─► chain1 ─────────────────┴─► chain4 (tee to a file) ─┐
     chain0 ─┤                                                       ├─► chain7 (file)
             └─► chain2 (tee to a file) ─┬─► chain5 (file)           │
                                         │                           │
                                         └─► chain6 ─────────────────┘
    */

    // chain 0
    let split_targets: Vec<ChainId> = vec![1, 2];
    let split = Split::new(split_targets);
    let header_count_tracker = split.get_new_header_count_tracker();
    let node_split_sink = HeaderNode::SplitHeaderSink(split, header_count_tracker);
    let chain_0 = HeaderChain::new(vec![node_split_sink]);

    // chain 1
    let split_targets: Vec<ChainId> = vec![3, 4];
    let split = Split::new(split_targets);
    let header_count_tracker = split.get_new_header_count_tracker();
    let node_split_sink = HeaderNode::SplitHeaderSink(split, header_count_tracker);
    let chain_1 = HeaderChain::new(vec![node_split_sink]);

    // chain 2
    let split_targets: Vec<ChainId> = vec![5, 6];
    let split = Split::new(split_targets);
    let header_count_tracker = split.get_new_header_count_tracker();
    let node_split_sink = HeaderNode::SplitHeaderSink(split, header_count_tracker);
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output2), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_2 = HeaderChain::new(vec![node_split_sink, node_output]);

    // chain 3
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output3), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_3 = HeaderChain::new(vec![node_output]);

    // chain 4
    let node_merge = HeaderNode::Merge(7);
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output4), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_4 = HeaderChain::new(vec![node_merge, node_output]);

    // chain 5
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output5), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_5 = HeaderChain::new(vec![node_output]);

    // chain 6
    let node_merge = HeaderNode::Merge(7);
    let chain_6 = HeaderChain::new(vec![node_merge]);

    // chain 7
    let merge = MergeJoin::new(2)?;
    let header_count_tracker = merge.get_new_header_count_tracker();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, header_count_tracker);
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(Some(output7), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_7 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![
        chain_0, chain_1, chain_2, chain_3, chain_4, chain_5, chain_6, chain_7,
    ]);

    Ok(Box::new(Driver::new(
        sources,
        graph,
        types::TIMESTAMP_RANGE_ALL,
        headers,
    )?))
}
