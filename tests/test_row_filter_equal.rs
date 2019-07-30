use chrono_tz::America::New_York;
use same_file::is_same_file;

use chopper_lib::chopper::chopper::{ChopperDriver, Source};
use chopper_lib::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{self, Header};
use chopper_lib::driver::driver::Driver;
use chopper_lib::driver::merge_join::MergeJoin;
use chopper_lib::error::{self, CliResult};
use chopper
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::source::csv_configs::{self, CSVInputConfig, CSVOutputConfig, DELIMITER_DEFAULT};
use chopper_lib::write::factory;

#[test]
fn test_row_filter_equal() {
    error::handle_drive_error(filter());
}

fn filter() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input = "./tests/files/input_time_city.csv";
    let inputs = vec![input];
    let output = "./tests/files/test_merge_output.csv";

    // source reader and headers
    let input_config = CSVInputConfig::new
        (csv_configs::DELIMITER_DEFAULT,
         true,
         0,
         None,
         None,
         None,
         New_York
        )?;
    let mut input_factory
        = InputFactory::new(Some(input_config), None, None)?;
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

    // merge/sink chain 2
    let merge = MergeJoin::new(2)?;
    let num_of_header_to_process = merge.num_of_header_to_process();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, num_of_header_to_process);
    let csv_output_config = CSVOutputConfig::new(DELIMITER_DEFAULT, true);
    let header_sink = factory::new_header_sink
        (Some(output.to_string()), Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_2 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![chain_1, chain_2]);
    Ok(Box::new(
        Driver::new(sources, graph, types::TIMESTAMP_RANGE_DEFAULT, headers)?))
}
