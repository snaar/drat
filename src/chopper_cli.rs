use clap::{Arg, App};
use clap::crate_version;

use crate::chopper::chopper::{ChDriver, Source};
use crate::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use crate::chopper::types::{DataRange, Header};
use crate::driver::driver::Driver;
use crate::error::{self, CliResult};
use crate::source::{csv_config::{self, CSVConfig}, source_factory::BosuSourceFactory};
use crate::transport::transport_factory::TransportFactory;
use crate::write::factory;

pub fn chopper_cli(transport_factories: Vec<Box<TransportFactory>>) -> CliResult<()> {
    let mut driver = parse_cli_args(transport_factories)?;
    driver.drive()
}

pub fn parse_cli_args(transport_factories: Vec<Box<TransportFactory>>) -> CliResult<Box<ChDriver>> {
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
        .arg(Arg::with_name("timestamp_column_index")
            .short("t")
            .long("timestamp-column-index")
            .help("csv only: specify the timestamp column index [default: 0]")
            .takes_value(true)
            .value_name("ARG"))
        .arg(Arg::with_name("transport")
            .help("sets the transport files to use; \n\
            if missing, stdin will be used")
            .multiple(true))
        .arg(Arg::with_name("output")
            .long("output")
            .short("o")
            .help("output to a file")
            .takes_value(true)
            .value_name("FILE"))
        .arg(Arg::with_name("has_headers")
            .long("has-headers")
            .help("csv only: transport files have headers"))
        .arg(Arg::with_name("delimiter")
            .long("delimiter")
            .short("d")
            .help("csv only: field/column delimiter")
            .takes_value(true)
            .default_value(",")
            .value_name("ARG"))
        .arg(Arg::with_name("print_timestamp")
            .long("print-timestamp")
            .short("p")
            .help("csv only: print timestamp as first column"))
        .arg(Arg::with_name("backtrace")
            .long("backtrace")
            .help("print backtrace"))
        .get_matches();

    if matches.is_present("backtrace") {
        error::turn_on_backtrace()
    }
    let begin: Option<u64> = match matches.value_of("begin") {
        None => None,
        Some(b) => Some(b.parse::<u64>().unwrap())
    };
    let end: Option<u64> = match matches.value_of("end") {
        None => None,
        Some(e) => Some(e.parse::<u64>().unwrap())
    };
    let data_range = DataRange { begin, end };
    let timestamp_column_index: usize = match matches.value_of("timestamp_column_index") {
        None => csv_config::TIMESTAMP_COL_DEFAULT,
        Some(t) => t.parse::<usize>().unwrap()
    };
    let inputs = match matches.values_of("transport") {
        None => None,
        Some(i) => {
            let mut inputs_vec: Vec<&str> = Vec::new();
            for s in i {
                inputs_vec.push(s);
            }
            Some(inputs_vec)
        },
    };
    let outputs = match matches.value_of("output") {
        None => None,
        Some(s) => Some(s.to_string())
    };
    let has_headers = matches.is_present("has_headers");
    let delimiter = matches.value_of("delimiter").unwrap();
    let print_timestamp = matches.is_present("print_timestamp");
    let csv_config
        = CSVConfig::new(delimiter, has_headers, timestamp_column_index, print_timestamp)?;

    setup_graph(inputs, outputs, transport_factories, data_range, csv_config)
}

pub fn setup_graph(inputs: Option<Vec<&str>>,
                   output: Option<String>,
                   transport_factories: Vec<Box<TransportFactory>>,
                   data_range: DataRange, csv_config: CSVConfig) -> CliResult<Box<ChDriver>> {

    let mut bosu_source_factory
        = BosuSourceFactory::new(Some(csv_config), None, transport_factories)?;
    let mut sources: Vec<Box<Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    match inputs {
        Some(inputs) => {
            for i in inputs {
                let source = bosu_source_factory.create_source_from_path(i)?;
                headers.push(source.header().clone());
                sources.push(source);
            }
        }
        None => {
            // default source factory is csv
            let source
                = bosu_source_factory.create_source_from_stdin("csv")?;
            headers.push(source.header().clone());
            sources.push(source);
        }
    }

    // sink writer
    let header_sink = factory::new_header_sink(output)?;
    let node = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node]);
    let graph = HeaderGraph::new(vec![chain]);

    Ok(Box::new(Driver::new(sources, graph, data_range, headers)?))
}
