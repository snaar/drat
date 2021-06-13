use chopper::chopper::driver::ChopperDriver;
use chopper::chopper::error::ChopperResult;
use chopper::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper::chopper::types::{self, FieldValue, Header};
use chopper::driver::driver::Driver;
use chopper::driver::merge_join::MergeJoin;
use chopper::filter::row_filter_equal_value::RowFilterEqualValue;
use chopper::filter::row_filter_greater_value::RowFilterGreaterValue;
use chopper::input::input_factory::InputFactoryBuilder;
use chopper::source::source::Source;
use chopper::util::dc_factory::DCFactory;
use chopper::write::factory::OutputFactory;

fn main() -> ChopperResult<()> {
    setup_filter_and_merge_graph()?.drive()
}

fn setup_filter_and_merge_graph() -> ChopperResult<Box<dyn ChopperDriver>> {
    let input_1 = "./examples/files/hundred.dc";
    let input_2 = "./examples/files/hundred.dc";
    let inputs = vec![input_1, input_2];
    let output = None;
    let column_int = "an_int";
    let value_1 = FieldValue::Int(95);
    let column_double = "a_double";
    let value_2 = FieldValue::Double(50.0);

    // source reader and headers
    let mut input_factory = InputFactoryBuilder::new()
        .with_dc_factory(Some(DCFactory::default()))
        .build()?;
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
    let node_merge = HeaderNode::Merge(2);
    let chain_0 = HeaderChain::new(vec![node_filter, node_merge]);

    // source chain 1
    let filter_equal = RowFilterEqualValue::new(column_double, value_2);
    let node_filter = HeaderNode::HeaderSink(filter_equal);
    let node_merge = HeaderNode::Merge(2);
    let chain_1 = HeaderChain::new(vec![node_filter, node_merge]);

    // merge/sink chain 2
    let merge = MergeJoin::new(2)?;
    let header_count_tracker = merge.get_new_header_count_tracker();
    let node_merge_sink = HeaderNode::MergeHeaderSink(merge, header_count_tracker);
    let header_sink = OutputFactory::new().new_header_sink(output)?;
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
