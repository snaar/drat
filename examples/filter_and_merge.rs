use chopper_lib::args;
use chopper_lib::chopper::chopper::{DRDriver, Source};
use chopper_lib::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::{FieldValue, Header};
use chopper_lib::driver::merge_join::MergeJoin;
use chopper_lib::error::{self, CliResult};
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::input::file::FileInput;
use chopper_lib::input::http::Http;
use chopper_lib::driver::driver::Driver;
use chopper_lib::filter::row_filter_equal_value::RowFilterEqualValue;
use chopper_lib::filter::row_filter_greater_value::RowFilterGreaterValue;
use chopper_lib::write::factory;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let vec: Vec<Box<InputFactory>> = vec![Box::new(http), Box::new(file)];
    error::handle_drive_error(filter_and_merge(vec));
}

pub fn filter_and_merge(input_factories: Vec<Box<InputFactory>>) -> CliResult<()> {
    let cli_args = parse_args()?;
    let args = args::Args {cli_args, input_factories};
    setup_graph(args)?.drive()
}

pub fn parse_args() -> CliResult<args::CliArgs> {
    let input_1 = "./examples/files/million.dc".to_string();
    let input_2 = "./examples/files/million.dc".to_string();
    let inputs = vec![input_1, input_2];
    Ok(args::CliArgs::new(inputs, None, None, None)?)
}

pub fn setup_graph(mut args: args::Args) -> CliResult<Box<DRDriver>> {
    let column_int = "an_int".to_string();
    let value_1 = FieldValue::Int(999950);
    let column_double = "a_double".to_string();
    let value_2 = FieldValue::Double(50.0);
    // sink writer
    let output = None;

    // source reader and headers
    let source_configs = args.create_configs()?;
    let size = source_configs.len();
    let mut sources: Vec<Box<Source>> = Vec::with_capacity(size);
    let mut headers: Vec<Header> = Vec::with_capacity(size);
    for mut s in source_configs {
        let source = s.reader()?;
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
    let header_sink = factory::new_header_sink(output)?;
    let node_output = HeaderNode::HeaderSink(header_sink);
    let chain_3 = HeaderChain::new(vec![node_merge_sink, node_output]);

    let graph = HeaderGraph::new(vec![chain_1, chain_2, chain_3]);
    Ok(Box::new(
        Driver::new(sources, graph, args.cli_args.data_range, headers)?))
}
