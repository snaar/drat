use chopper_lib::chopper::chopper::{ChDriver, Source};
use chopper_lib::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{self, FieldValue, Header};
use chopper_lib::driver::driver::Driver;
use chopper_lib::driver::merge_join::MergeJoin;
use chopper_lib::error::{self, CliResult};
use chopper_lib::filter::row_filter_equal_value::RowFilterEqualValue;
use chopper_lib::filter::row_filter_greater_value::RowFilterGreaterValue;
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::source::csv_configs::{CSVOutputConfig, DELIMITER_DEFAULT};
use chopper_lib::write::factory;

fn main() {
    error::handle_drive_error(filter_and_merge());
}

fn filter_and_merge() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<ChDriver>> {
    let input_1 = "./examples/files/million.dc";
    let input_2 = "./examples/files/million.dc";
    let inputs = vec![input_1, input_2];
    let output = None;
    let column_int = "an_int".to_string();
    let value_1 = FieldValue::Int(999950);
    let column_double = "a_double".to_string();
    let value_2 = FieldValue::Double(50.0);

    // source reader and headers
    let mut input_factory
        = InputFactory::new(None, None, None)?;
    let mut sources: Vec<Box<Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // source chain 1
    let filter_greater
        = RowFilterGreaterValue::new(column_int, value_1);
    let node_filter = HeaderNode::HeaderSink(filter_greater);
    let node_merge = HeaderNode::Merge(2, 0);
    let chain_1 = HeaderChain::new(vec![node_filter, node_merge]);

    // source chain 2
    let filter_equal
        = RowFilterEqualValue::new(column_double, value_2);
    let node_filter_2 = HeaderNode::HeaderSink(filter_equal);
    let node_merge_2 = HeaderNode::Merge(2, 1);
    let chain_2 = HeaderChain::new(vec![node_filter_2, node_merge_2]);

    // merge/sink chain 3
    let merge = MergeJoin::new(2)?;
    let num_of_header_to_process = merge.num_of_header_to_process();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, num_of_header_to_process);
    let csv_output_config = CSVOutputConfig::new(DELIMITER_DEFAULT, true);
    let header_sink = factory::new_header_sink(output, Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_3 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![chain_1, chain_2, chain_3]);
    Ok(Box::new(
        Driver::new(sources, graph, types::DATA_RANGE_DEFAULT, headers)?))
}
