use chrono_tz::America::New_York;
use same_file::is_same_file;

use chopper_lib::chopper::chopper::{ChopperDriver, Source};
use chopper_lib::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{self, Header};
use chopper_lib::driver::driver::Driver;
use chopper_lib::driver::merge_join::MergeJoin;
use chopper_lib::error::{self, CliResult};
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::source::csv_configs::{CSVInputConfig, CSVOutputConfig, OUTPUT_DELIMITER_DEFAULT};
use chopper_lib::source::csv_configs::{TimestampCol, TimestampConfig};
use chopper_lib::write::factory;

#[test]
fn test_merge() {
    error::handle_drive_error(test());
    assert!(is_same_file
        ("./tests/output/test_merge.csv",
         "./tests/reference/merge.csv",
        ).unwrap());
}

fn test() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input_1 = "./tests/input/time_city.csv";
    let input_2 = "./tests/input/time_city.csv";
    let inputs = vec![input_1, input_2];
    let output = "./tests/output/test_merge.csv";

    // source reader and headers
    let ts_config = TimestampConfig::new
        (TimestampCol::Timestamp(0), None, New_York);
    let input_config = CSVInputConfig::new
        (None, true, ts_config)?;
    let mut input_factory
        = InputFactory::new(None, Some(input_config), None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // source chain 1
    let node_merge = HeaderNode::Merge(2, 0);
    let chain_1 = HeaderChain::new(vec![node_merge]);

    // source chain 2
    let node_merge_2 = HeaderNode::Merge(2, 1);
    let chain_2 = HeaderChain::new(vec![node_merge_2]);

    // merge/sink chain 3
    let merge = MergeJoin::new(2)?;
    let num_of_header_to_process = merge.num_of_header_to_process();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, num_of_header_to_process);
    let csv_output_config = CSVOutputConfig::new(OUTPUT_DELIMITER_DEFAULT, true);
    let header_sink = factory::new_header_sink
        (Some(output), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_3 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![chain_1, chain_2, chain_3]);

    Ok(Box::new(
        Driver::new(sources, graph, types::TIMESTAMP_RANGE_DEFAULT, headers)?))
}
