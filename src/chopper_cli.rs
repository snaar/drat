use clap::{App, Arg};
use clap::crate_version;

use crate::chopper::chopper::{ChDriver, Source};
use crate::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use crate::chopper::types::{DataRange, Header};
use crate::driver::driver::Driver;
use crate::error::{self, CliResult};
use crate::input::input_factory::InputFactory;
use crate::source::{csv_configs::{self, CSVInputConfig, CSVOutputConfig}, source_factory::SourceFactory};
use crate::transport::transport_factory::TransportFactory;
use crate::util::csv_util;
use crate::write::factory;

pub fn chopper_cli(transport_factories: Option<Vec<Box<TransportFactory>>>,
                   source_factories: Option<Vec<Box<SourceFactory>>>) -> CliResult<()> {
    let mut driver = parse_cli_args(transport_factories, source_factories)?;
    driver.drive()
}

pub fn parse_cli_args(transport_factories: Option<Vec<Box<TransportFactory>>>,
                      source_factories: Option<Vec<Box<SourceFactory>>>) -> CliResult<Box<ChDriver>> {
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
        .arg(Arg::with_name("backtrace")
            .long("backtrace")
            .help("print backtrace"))
        .arg(Arg::with_name("input")
            .help("sets the input files to use; \n\
            if missing, stdin will be used")
            .multiple(true))
        .arg(Arg::with_name("output")
            .long("output")
            .short("o")
            .help("output to a file")
            .takes_value(true)
            .value_name("FILE"))
        .arg(Arg::with_name("csv_input_delimiter")
            .long("csv-in-delimiter")
            .help("csv only: input field/column delimiter")
            .takes_value(true)
            .default_value(",")
            .value_name("ARG"))
        .arg(Arg::with_name("csv_output_delimiter")
            .long("csv-out-delimiter")
            .help("csv only: output field/column delimiter")
            .takes_value(true)
            .default_value(",")
            .value_name("ARG"))
        .arg(Arg::with_name("csv_has_header")
            .long("csv-has-header")
            .help("csv only: input files have header"))
        .arg(Arg::with_name("csv_print_timestamp")
            .long("csv-print-timestamp")
            .help("csv only: print timestamp as first column. default - 'auto'")
            .takes_value(true)
            .default_value("auto")
            .possible_values(&["true", "false", "auto"])
            .case_insensitive(true)
            .value_name("ARG"))
        .arg(Arg::with_name("csv_timestamp_column_index")
            .long("csv-timestamp")
            .help("csv only: specify the timestamp column index [default: 0]")
            .takes_value(true)
            .value_name("ARG"))
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
    let inputs = match matches.values_of("input") {
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
    let input_delimiter = matches.value_of("csv_input_delimiter").unwrap();
    let output_delimiter = matches.value_of("csv_output_delimiter").unwrap();
    let has_header = matches.is_present("csv_has_header");
    let print_timestamp = match matches.value_of("csv_print_timestamp").unwrap() {
        "auto" => None,
        "true" => Some(true),
        "false" => Some(false),
        _ => unreachable!()
    };
    let timestamp_column_index: usize = match matches.value_of("csv_timestamp_column_index") {
        None => csv_configs::TIMESTAMP_COL_DEFAULT,
        Some(t) => t.parse::<usize>().unwrap()
    };
    let csv_input_config
        = CSVInputConfig::new(input_delimiter, has_header, timestamp_column_index)?;

    setup_graph(inputs,
                outputs,
                transport_factories,
                source_factories,
                data_range,
                csv_input_config,
                output_delimiter,
                print_timestamp)
}

fn setup_graph(inputs: Option<Vec<&str>>,
               output: Option<String>,
               transport_factories: Option<Vec<Box<TransportFactory>>>,
               source_factories: Option<Vec<Box<SourceFactory>>>,
               data_range: DataRange,
               csv_input_config: CSVInputConfig,
               csv_output_delimiter: &str,
               csv_output_print_timestamp: Option<bool>) -> CliResult<Box<ChDriver>> {
    // get sources and headers
    let mut input_factory
        = InputFactory::new(Some(csv_input_config), source_factories, transport_factories)?;
    let mut sources: Vec<Box<Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    match inputs {
        Some(inputs) => {
            for i in inputs {
                let source = input_factory.create_source_from_path(i)?;
                headers.push(source.header().clone());
                sources.push(source);
            }
        }
        None => {
            // default source factory is csv
            let source
                = input_factory.create_source_from_stdin("csv")?;
            headers.push(source.header().clone());
            sources.push(source);
        }
    }

    let csv_output_config = match csv_output_print_timestamp {
        Some(b) => CSVOutputConfig::new(csv_output_delimiter, b),
        None => csv_util::create_csv_output_config_from_source(&mut sources, csv_output_delimiter)
    };

    // create header graph
    let header_sink = factory::new_header_sink(output, Some(csv_output_config))?;
    let node = HeaderNode::HeaderSink(header_sink);
    let chain = HeaderChain::new(vec![node]);
    let graph = HeaderGraph::new(vec![chain]);

    Ok(Box::new(Driver::new(sources, graph, data_range, headers)?))
}
