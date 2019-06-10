use clap::crate_version;

use crate::args;
use crate::dr::dr::{DRDriver, Source};
use crate::dr::graph::{HeaderChain, HeaderGraph, HeaderNode, PinsWithoutHeader};
use crate::dr::types::{FieldValue, Header};
use crate::input::input_factory::InputFactory;
use crate::driver::{driver::Driver, merge_join::MergeJoin};
use crate::filter::row_filter_greater_value::RowFilterGreaterValue;
use crate::source_config;
use crate::util::csv_util;
use crate::write::factory;

pub fn drat_manual(input_factories: Vec<Box<InputFactory>>) {
    let cli_args = parse_args();
    let args = args::Args { cli_args, input_factories };
    let column_string = "a_string".to_string();
    let value = "200".to_string();
    let mut driver = setup_graph_with_filters(args, column_string, value);
//    let mut driver = setup_graph_without_filter(args);
    driver.drive();
}

pub fn parse_args() -> args::CliArgs {
    let input_1 = "million.dc".to_string();
    let input_2 = "million2.dc".to_string();
    let inputs = vec![input_1, input_2];
    let output = None;
    let has_headers = false;
    let delimiter = csv_util::parse_into_delimiter(",").unwrap();
    let timestamp_column = 0;
    let csv_config = source_config::CSVConfig::new(delimiter, has_headers, timestamp_column);
    let data_range = args::DataRange { begin: None, end: None };

    args::CliArgs {
        inputs,
        data_range,
        output,
        csv_config,
    }
}

pub fn setup_graph_with_filters(mut args: args::Args, column_string: String, value: String) -> Box<DRDriver> {
    // source reader and headers
    let source_configs = args.create_configs().unwrap();
    let mut sources: Vec<Box<Source+'static>> = Vec::with_capacity(source_configs.len());
    let mut headers: Vec<Header> = Vec::with_capacity(source_configs.len());
    for mut s in source_configs {
        let source = s.reader().unwrap();
        headers.push(source.header().clone());
        sources.push(source);
    }

    // sink writer
    let output = None;

    // source chain 1
    let filter_greater_value = RowFilterGreaterValue::new(column_string, FieldValue::String(value));
    let node_1 = HeaderNode::HeaderSink(filter_greater_value);
    let node_2 = HeaderNode::Merge(2, 0);
    let chain_1 = HeaderChain::new(vec![node_1, node_2]);

    // source chain 2
    let node_3 = HeaderNode::Merge(2, 1);
    let chain_2 = HeaderChain::new(vec![node_3]);

    // merge/sink chain 3
    let merge = MergeJoin::new(2);
    let pin_without_header = PinsWithoutHeader {counter: merge.pin_num()};
    let node_4 = HeaderNode::MuxHeaderSink(merge, pin_without_header);
    let header_sink = factory::new_header_sink(output);
    let node_5 = HeaderNode::HeaderSink(header_sink);
    let chain_3 = HeaderChain::new(vec![node_4, node_5]);

    let graph = HeaderGraph::new(vec![chain_1, chain_2, chain_3]);
    Box::new(Driver::new(sources, graph, args.cli_args.data_range, headers))
}

pub fn setup_graph_without_filter(mut args: args::Args) -> Box<DRDriver> {
    let source_configs = args.create_configs().unwrap();
    let mut sources: Vec<Box<Source+'static>> = Vec::with_capacity(source_configs.len());
    let mut headers: Vec<Header> = Vec::with_capacity(source_configs.len());
    for mut s in source_configs {
        let source = s.reader().unwrap();
        headers.push(source.header().clone());
        sources.push(source);
    }

    // sink writer
    let output = None;

    let header_sink = factory::new_header_sink(output);
    let node = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node]);

    let graph = HeaderGraph::new(vec![chain]);
    Box::new(Driver::new(sources, graph, args.cli_args.data_range, headers))
}
