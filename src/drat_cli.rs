use clap::{Arg, App};
use clap::crate_version;

use crate::args;
use crate::dr::dr::{DRDriver, Source};
use crate::dr::graph::{HeaderChain, HeaderGraph, HeaderNode};
use crate::dr::types::Header;
use crate::input::input_factory::InputFactory;
use crate::driver::driver::Driver;
use crate::filter::row_filter_greater_value::RowFilterGreaterValue;
use crate::source_config;
use crate::util::csv_util;
use crate::write::factory;

pub fn drat_cli(input_factories: Vec<Box<InputFactory>>) {
    let cli_args = parse_cli_args();
    let args = args::Args { cli_args, input_factories };
    let mut driver = setup_graph(args);
    driver.drive();
}

pub fn parse_cli_args() -> args::CliArgs {
    let matches = App::new("drat")
        .version(crate_version!())
        .about("drat is a simple streaming time series tool")
        .arg(Arg::with_name("begin")
            .short("b")
            .long("begin")
            .help("set begin timestamp (inclusive)")
            .takes_value(true)
            .value_name("TIMESTAMP"))
        .arg(Arg::with_name("end")
            .short("e")
            .long("end")
            .help("set end timestamp (exclusive)")
            .takes_value(true)
            .value_name("TIMESTAMP"))
        .arg(Arg::with_name("timestamp_column")
            .short("t")
            .long("timestamp-column")
            .help("csv only: specify the timestamp column [default: 0]")
            .takes_value(true)
            .value_name("ARG"))
        .arg(Arg::with_name("input")
            .help("sets the input files to use; \n\
            if missing, stdin will be used")
            .multiple(true))
        .arg(Arg::with_name("has_headers")
            .long("has-headers")
            .help("csv only: input files have headers"))
        .arg(Arg::with_name("output")
            .long("output")
            .short("o")
            .help("output to a file")
            .takes_value(true)
            .value_name("FILE"))
        .arg(Arg::with_name("delimiter")
            .long("delimiter")
            .short("d")
            .help("csv only: field/column delimiter")
            .takes_value(true)
            .default_value(",")
            .value_name("ARG"))
        .get_matches();

    let begin: Option<u64> = match matches.value_of("begin") {
        None => None,
        Some(b) => Some(b.parse::<u64>().unwrap())
    };
    let end: Option<u64> = match matches.value_of("end") {
        None => None,
        Some(e) => Some(e.parse::<u64>().unwrap())
    };
    let timestamp_column: usize = match matches.value_of("timestamp_column") {
        None => 0,
        Some(t) => t.parse::<usize>().unwrap()
    };
    let inputs = match matches.values_of("input") {
        None => vec![],
        Some(i) => {
            let mut inputs_vec: Vec<String> = Vec::new();
            for s in i {
                inputs_vec.push(s.to_string());
            }
            inputs_vec
        },
    };

    let output = match matches.value_of("output") {
        None => None,
        Some(s) => Some(s.to_string())
    };
    let has_headers = matches.is_present("has_headers");
    let delimiter = matches.value_of("delimiter").unwrap();
    let delimiter = csv_util::parse_into_delimiter(delimiter).unwrap();
    let csv_config = source_config::CSVConfig::new(delimiter, has_headers, timestamp_column);
    let data_range = args::DataRange { begin, end };

    args::CliArgs {
        inputs,
        data_range,
        output,
        csv_config,
    }
}

pub fn setup_graph(mut args: args::Args) -> Box<DRDriver> {
    let source_configs = args.create_configs().unwrap();
    let mut sources: Vec<Box<Source+'static>> = Vec::with_capacity(source_configs.len());
    let mut headers: Vec<Header> = Vec::with_capacity(source_configs.len());
    for mut s in source_configs {
        let source = s.reader().unwrap();
        headers.push(source.header().clone());
        sources.push(source);
    }

    // sink writer
    let output = args.cli_args.output.clone();

    let header_sink = factory::new_header_sink(output);
    let node = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node]);

    let graph = HeaderGraph::new(vec![chain]);
    Box::new(Driver::new(sources, graph, args.cli_args.data_range, headers))
}
