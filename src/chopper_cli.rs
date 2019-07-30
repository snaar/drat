use std::collections::HashMap;

use chrono_tz::Tz;
use clap::ArgMatches;

use crate::chopper::chopper::{ChopperDriver, Source};
use crate::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use crate::chopper::types::{Header, TimestampRange};
use crate::cli_app::CliApp;
use crate::driver::{driver::Driver, merge_join::MergeJoin};
use crate::error::{self, CliResult};
use crate::input::input_factory::InputFactory;
use crate::source::{csv_configs::{self, CSVInputConfig, CSVOutputConfig}, source_factory::SourceFactory};
use crate::transport::transport_factory::TransportFactory;
use crate::util::{csv_util, timestamp_util};
use crate::write::factory;

pub fn chopper_cli(transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
                   source_factories: Option<Vec<Box<dyn SourceFactory>>>,
                   timezone_map: Option<HashMap<&str, Tz>>) -> CliResult<()>
{
    let mut driver = parse_cli_args(transport_factories, source_factories, timezone_map)?;
    driver.drive()
}

pub fn parse_cli_args(transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
                      source_factories: Option<Vec<Box<dyn SourceFactory>>>,
                      timezone_map: Option<HashMap<&str, Tz>>) -> CliResult<Box<dyn ChopperDriver>>
{
    let matches = CliApp.create_cli_app().get_matches();
    if matches.is_present("backtrace") {
        error::turn_on_backtrace()
    }

    let timezone: Tz = match matches.value_of("timezone") {
        None => timestamp_util::DEFAULT_ZONE,
        Some(t) =>  match timezone_map {
            None => t.parse().unwrap(),
            Some(map) => match map.get(t) {
                None => t.parse().unwrap(),
                Some(tz) => *tz
            }
        }
    };
    let timestamp_range= TimestampRange::new(
        matches.value_of("begin"),
        matches.value_of("end"),
        timezone)?;

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

    // csv only
    let csv_input_config = parse_csv_config(&matches, timezone)?;
    let output_delimiter = matches.value_of("csv_output_delimiter").unwrap();
    let print_timestamp = match matches.value_of("csv_print_timestamp").unwrap() {
        "auto" => None,
        "true" => Some(true),
        "false" => Some(false),
        _ => unreachable!()
    };

    setup_graph(inputs,
                outputs,
                transport_factories,
                source_factories,
                timestamp_range,
                csv_input_config,
                output_delimiter,
                print_timestamp)
}

fn setup_graph(inputs: Option<Vec<&str>>,
               output: Option<String>,
               transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
               source_factories: Option<Vec<Box<dyn SourceFactory>>>,
               timestamp_range: TimestampRange,
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

    let csv_output_config = match csv_output_print_timestamp {
        Some(b) => CSVOutputConfig::new(csv_output_delimiter, b),
        None => csv_util::create_csv_output_config_from_source(&mut sources, csv_output_delimiter)
    };

    let mut header_nodes: Vec<HeaderNode> = Vec::new();
    let mut chains: Vec<HeaderChain> = Vec::new();
    match inputs {
        Some(inputs) => {
            for i in 0..inputs.len() {
                let source = input_factory.create_source_from_path(inputs.get(i).unwrap())?;
                headers.push(source.header().clone());
                sources.push(source);
                // add Merge to chains if multiple input files
                if inputs.len() > 1 {
                    let merge = HeaderNode::Merge(inputs.len(), i);
                    let chain = HeaderChain::new(vec![merge]);
                    chains.push(chain);
                }
            }
            // add MergeHeaderSink as first header node if multiple input files
            if inputs.len() > 1 {
                let merge = MergeJoin::new(inputs.len())?;
                let num_of_header_to_process = merge.num_of_header_to_process();
                let node_merge_sink = HeaderNode::MergeHeaderSink(merge, num_of_header_to_process);
                header_nodes.push(node_merge_sink);
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
    let header_sink = factory::new_header_sink(output, Some(csv_output_config))?;
    let node_hs = HeaderNode::HeaderSink(header_sink);
    header_nodes.push(node_hs);
    chains.push(HeaderChain::new(header_nodes));
    let graph = HeaderGraph::new(chains);

    Ok(Box::new(Driver::new(sources, graph, timestamp_range, headers)?))
}

fn parse_csv_config(matches: &ArgMatches, timezone: Tz) -> CliResult<CSVInputConfig> {
    let input_delimiter = matches.value_of("csv_input_delimiter").unwrap();
    let has_header = matches.is_present("csv_has_header");
    let timestamp_col_date: usize = match matches.value_of("csv_timestamp_col_date") {
        None => csv_configs::TIMESTAMP_COL_DATE_DEFAULT,
        Some(i) => i.parse::<usize>().unwrap()
    };
    let timestamp_col_time: Option<usize> = match matches.value_of("csv_timestamp_col_time") {
        None => None,
        Some(i) => Some(i.parse::<usize>().unwrap())
    };
    let timestamp_fmt_date: Option<&str> = matches.value_of("csv_timestamp_format_date");
    let timestamp_fmt_time: Option<&str> = matches.value_of("csv_timestamp_format_time");

    CSVInputConfig::new(input_delimiter,
                        has_header,
                        timestamp_col_date,
                        timestamp_col_time,
                        timestamp_fmt_date,
                        timestamp_fmt_time,
                        timezone)
}
