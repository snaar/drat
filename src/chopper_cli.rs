use std::collections::HashMap;

use chrono_tz::Tz;
use clap::{value_t, ArgMatches};

use crate::chopper::chopper::{ChopperDriver, Source};
use crate::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use crate::chopper::types::{Header, TimestampRange};
use crate::cli::util::YesNoAuto;
use crate::cli_app::CliApp;
use crate::driver::{driver::Driver, merge_join::MergeJoin};
use crate::error::{self, CliResult};
use crate::input::input::{Input, InputFormat, InputType};
use crate::input::input_factory::InputFactory;
use crate::source::csv_configs::{CSVInputConfig, CSVOutputConfig, TimestampCol, TimestampConfig};
use crate::source::source_factory::SourceFactory;
use crate::transport::transport_factory::TransportFactory;
use crate::util::{csv_util, timestamp_util};
use crate::write::factory;

pub fn chopper_cli(
    transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
    source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    timezone_map: Option<HashMap<&str, Tz>>,
) -> CliResult<()> {
    let mut driver = parse_cli_args(transport_factories, source_factories, timezone_map)?;
    driver.drive()
}

pub fn parse_cli_args(
    transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
    source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    timezone_map: Option<HashMap<&str, Tz>>,
) -> CliResult<Box<dyn ChopperDriver>> {
    let matches = CliApp.create_cli_app().get_matches();

    if matches.is_present("backtrace") {
        error::turn_on_backtrace()
    }

    let timezone: Tz = match matches.value_of("timezone") {
        None => timestamp_util::DEFAULT_ZONE,
        Some(t) => match timezone_map {
            None => t.parse().unwrap(),
            Some(map) => match map.get(t) {
                None => t.parse().unwrap(),
                Some(tz) => *tz,
            },
        },
    };

    let timestamp_range =
        TimestampRange::new(matches.value_of("begin"), matches.value_of("end"), timezone)?;

    let mut input_formats: Vec<InputFormat> = Vec::new();
    match matches.values_of("format") {
        None => input_formats.push(InputFormat::Auto),
        Some(formats) => {
            for format in formats {
                input_formats.push(if format == "auto" {
                    InputFormat::Auto
                } else {
                    InputFormat::Extension(format.to_string())
                });
            }
        }
    };
    debug_assert!(!input_formats.is_empty());
    let last_format = input_formats.last().unwrap();

    let mut inputs: Vec<Input> = Vec::new();
    match matches.values_of("input") {
        //TODO: add check here that there is only one format provided
        None => inputs.push(Input {
            input: InputType::StdIn,
            format: input_formats[0].clone(),
        }),
        //TODO: add check here that number of formats provided is no more than number of inputs
        // it's okay if it's less, because last format will be used for rest of inputs
        Some(input_strings) => {
            for (i, str) in input_strings.enumerate() {
                inputs.push(Input {
                    input: InputType::Path(str.to_string()),
                    format: if i < input_formats.len() {
                        input_formats[i].clone()
                    } else {
                        last_format.clone()
                    },
                });
            }
        }
    };
    debug_assert!(!inputs.is_empty());

    let output = matches.value_of("output");

    // csv only
    let csv_input_config = parse_csv_config(&matches, timezone)?;
    let csv_output_delimiter = matches.value_of("csv_output_delimiter").unwrap();
    let csv_output_print_timestamp = match matches.value_of("csv_print_ts").unwrap() {
        "auto" => None,
        "yes" => Some(true),
        "no" => Some(false),
        _ => unreachable!(),
    };

    setup_graph(
        inputs,
        output,
        transport_factories,
        source_factories,
        timestamp_range,
        csv_input_config,
        csv_output_delimiter,
        csv_output_print_timestamp,
    )
}

fn setup_graph(
    inputs: Vec<Input>,
    output: Option<&str>,
    transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
    source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    timestamp_range: TimestampRange,
    csv_input_config: CSVInputConfig,
    csv_output_delimiter: &str,
    csv_output_print_timestamp: Option<bool>,
) -> CliResult<Box<dyn ChopperDriver>> {
    // get sources and headers
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    let mut input_factory = InputFactory::new(
        Some(csv_input_config),
        source_factories,
        transport_factories,
    )?;

    let csv_output_config = match csv_output_print_timestamp {
        Some(b) => CSVOutputConfig::new(csv_output_delimiter, b),
        None => csv_util::create_csv_output_config_from_source(&mut sources, csv_output_delimiter),
    };

    let mut header_nodes: Vec<HeaderNode> = Vec::new();
    let mut chains: Vec<HeaderChain> = Vec::new();

    for (i, input) in inputs.iter().enumerate() {
        let source = input_factory.create_source_from_input(&input)?;
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

    let header_sink = factory::new_header_sink(output, Some(csv_output_config))?;
    let node_hs = HeaderNode::HeaderSink(header_sink);
    header_nodes.push(node_hs);
    chains.push(HeaderChain::new(header_nodes));
    let graph = HeaderGraph::new(chains);

    Ok(Box::new(Driver::new(
        sources,
        graph,
        timestamp_range,
        headers,
    )?))
}

fn parse_csv_config(matches: &ArgMatches, timezone: Tz) -> CliResult<CSVInputConfig> {
    let input_delimiter = matches.value_of("csv_input_delimiter");
    let has_header = value_t!(matches, "csv_input_has_header", YesNoAuto)?;

    // timestamp config
    let mut ts_fmt: Option<String> = None;
    let ts_col = match matches.value_of("csv_ts_col_date") {
        None => {
            // format
            match matches.value_of("csv_ts_fmt") {
                None => (),
                Some(fmt) => ts_fmt = Some(fmt.to_string()),
            }
            // col
            let ts = matches.value_of("csv_ts_col").unwrap();
            TimestampCol::Timestamp(ts.parse::<usize>().unwrap())
        }
        Some(d) => {
            // format
            match matches.value_of("csv_ts_fmt_date") {
                None => (),
                Some(d) => {
                    let t = matches.value_of("csv_ts_fmt_time").unwrap();
                    ts_fmt = Some(format!("{}{}", d, t))
                }
            };
            // col
            let date = d.parse::<usize>().unwrap();
            let time = match matches.value_of("csv_ts_col_time") {
                Some(t) => t.parse::<usize>().unwrap(),
                None => unreachable!(),
            };
            TimestampCol::DateAndTime(date, time)
        }
    };
    let ts_config = TimestampConfig::new(ts_col, ts_fmt, timezone);

    CSVInputConfig::new(input_delimiter, has_header, ts_config)
}
