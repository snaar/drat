use chopper::chopper::chopper::{ChopperDriver, Source};
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, FieldValue, Header};
use chopper::driver::driver::Driver;
use chopper::driver::merge_join::MergeJoin;
use chopper::error::{self, CliResult};
use chopper::filter::row_filter_equal_value::RowFilterEqualValue;
use chopper::filter::row_filter_greater_value::RowFilterGreaterValue;
use chopper::input::input_factory::InputFactory;
use chopper::source::csv_configs::CSVOutputConfig;
use chopper::write::factory;

fn main() {
    error::handle_drive_error(filter_and_merge());
}

fn filter_and_merge() -> CliResult<()> {
    setup_graph()?.drive()
}

fn setup_graph() -> CliResult<Box<dyn ChopperDriver>> {
    let input_1 = "./examples/files/million.dc";
    let input_2 = "./examples/files/million.dc";
    let inputs = vec![input_1, input_2];
    let output = None;
    let column_int = "an_int";
    let value_1 = FieldValue::Int(999950);
    let column_double = "a_double";
    let value_2 = FieldValue::Double(50.0);

    // source reader and headers
    let mut input_factory = InputFactory::new_without_csv(None, None)?;
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    for i in inputs {
        let source = input_factory.create_source_from_path(i)?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // source chain 0
    let filter_greater = RowFilterGreaterValue::new(column_int, value_1);
    let node_filter = HeaderNode::HeaderSink(filter_greater);
    let node_merge = HeaderNode::Merge(2, 0);
    let chain_0 = HeaderChain::new(vec![node_filter, node_merge]);

    // source chain 1
    let filter_equal = RowFilterEqualValue::new(column_double, value_2);
    let node_filter = HeaderNode::HeaderSink(filter_equal);
    let node_merge = HeaderNode::Merge(2, 1);
    let chain_1 = HeaderChain::new(vec![node_filter, node_merge]);

    // merge/sink chain 2
    let merge = MergeJoin::new(2)?;
    let num_of_header_to_process = merge.num_of_header_to_process();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, num_of_header_to_process);
    let csv_output_config = CSVOutputConfig::new_default();
    let header_sink = factory::new_header_sink(output, Some(csv_output_config))?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_2 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![chain_0, chain_1, chain_2]);
    Ok(Box::new(Driver::new(
        sources,
        graph,
        types::TIMESTAMP_RANGE_DEFAULT,
        headers,
    )?))
}
