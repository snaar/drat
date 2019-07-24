use crate::chopper::chopper::{ChopperDriver, Source};
use crate::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use crate::chopper::types::{DataRange, Header};
use crate::cli_app::CliApp;
use crate::driver::driver::Driver;
use crate::error::{self, CliResult};
use crate::input::input_factory::InputFactory;
use crate::source::{csv_configs::{self, CSVInputConfig, CSVOutputConfig}, source_factory::SourceFactory};
use crate::transport::transport_factory::TransportFactory;
use crate::util::{csv_util, timestamp_util};
use crate::write::factory;

pub fn chopper_cli(transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
                   source_factories: Option<Vec<Box<dyn SourceFactory>>>) -> CliResult<()> {
    let mut driver = parse_cli_args(transport_factories, source_factories)?;
    driver.drive()
}

pub fn parse_cli_args(transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
                      source_factories: Option<Vec<Box<dyn SourceFactory>>>) -> CliResult<Box<dyn ChopperDriver>>
{
    let matches = CliApp.create_cli_app().get_matches();
    if matches.is_present("backtrace") {
        error::turn_on_backtrace()
    }
    let time_zone = timestamp_util::parse_time_zone(matches.value_of("csv_time_zone"));
    let data_range
        = DataRange::new(matches.value_of("begin"), matches.value_of("end"), time_zone.as_str())?;

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

    // csv config
    let input_delimiter = matches.value_of("csv_input_delimiter").unwrap();
    let output_delimiter = matches.value_of("csv_output_delimiter").unwrap();
    let has_header = matches.is_present("csv_has_header");
    let print_timestamp = match matches.value_of("csv_print_timestamp").unwrap() {
        "auto" => None,
        "true" => Some(true),
        "false" => Some(false),
        _ => unreachable!()
    };
    let timestamp_col_date: usize = match matches.value_of("csv_timestamp_col_date") {
        None => csv_configs::TIMESTAMP_COL_DATE_DEFAULT,
        Some(i) => i.parse::<usize>().unwrap()
    };
    let timestamp_col_time: Option<usize> = match matches.value_of("csv_timestamp_col_time") {
        None => None,
        Some(i) => Some(i.parse::<usize>().unwrap())
    };
    let timestamp_format_date: Option<&str> = matches.value_of("csv_timestamp_format_date");
    let timestamp_format_time: Option<&str> = matches.value_of("csv_timestamp_format_time");

    let csv_input_config
        = CSVInputConfig::new(input_delimiter,
                              has_header,
                              timestamp_col_date,
                              timestamp_col_time,
                              timestamp_format_date,
                              timestamp_format_time,
                              time_zone)?;
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
               transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
               source_factories: Option<Vec<Box<dyn SourceFactory>>>,
               data_range: DataRange,
               csv_input_config: CSVInputConfig,
               csv_output_delimiter: &str,
               csv_output_print_timestamp: Option<bool>) -> CliResult<Box<dyn ChopperDriver>>
{
    // get sources and headers
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    let mut input_factory
        = InputFactory::new(
        Some(csv_input_config),
        source_factories,
        transport_factories)?;
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
