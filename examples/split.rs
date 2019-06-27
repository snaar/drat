extern crate chopper_lib;
use chopper_lib::args::{Args, CliArgs};
use chopper_lib::chopper::chopper::{DRDriver, Source};
use chopper_lib::chopper::header_graph::{ChainId, HeaderChain, HeaderGraph, HeaderNode};
use chopper_lib::chopper::types::Header;
use chopper_lib::error::{self, CliResult};
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::input::file::FileInput;
use chopper_lib::input::http::Http;
use chopper_lib::driver::{driver::Driver, split::Split};
use chopper_lib::write::factory;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let vec: Vec<Box<InputFactory>> = vec![
        Box::new(http),
        Box::new(file)];
    error::handle_drive_error(split(vec));
}


pub fn split(input_factories: Vec<Box<InputFactory>>) -> CliResult<()> {
    let cli_args = parse_args()? ;
    let args = Args {cli_args, input_factories};
    setup_test_graph(args)?.drive()
}

pub fn parse_args() -> CliResult<CliArgs> {
    let input = "./examples/files/hundred.dc".to_string();
    let inputs = vec![input];
    Ok(CliArgs::new(inputs, None, None, None)?)
}

pub fn setup_test_graph(mut args: Args) -> CliResult<Box<DRDriver>> {
    // sink writer
    let output_1 = None;
    let output_2 = None;

    // source reader and headers
    let source_configs = args.create_configs()?;
    let mut sources: Vec<Box<Source>> = Vec::with_capacity(source_configs.len());
    let mut headers: Vec<Header> = Vec::with_capacity(source_configs.len());
    for mut s in source_configs {
        let source = s.reader()?;
        headers.push(source.header().clone());
        sources.push(source);
    }

    // filter and split chain 1
    let chain_ids: Vec<ChainId> = vec![1, 2];
    let split = Split::new(chain_ids);
    let header_to_process = split.num_of_header_to_process();
    let node_split_sink = HeaderNode::SplitHeaderSink(split, header_to_process);
    let chain_1 = HeaderChain::new(vec![node_split_sink]);

    // sink chain 2
    let header_sink_1 = factory::new_header_sink(output_1)?;
    let node_output_1 = HeaderNode::HeaderSink(header_sink_1);
    let chain_2 = HeaderChain::new(vec![node_output_1]);

    // sink chain 3
    let header_sink_2 = factory::new_header_sink(output_2)?;
    let node_output_2 = HeaderNode::HeaderSink(header_sink_2);
    let chain_3 = HeaderChain::new(vec![node_output_2]);

    let graph = HeaderGraph::new(vec![chain_1, chain_2, chain_3]);
    Ok(Box::new(
        Driver::new(sources, graph, args.cli_args.data_range, headers)?))
}
