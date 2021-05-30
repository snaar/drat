use std::collections::HashMap;

use chrono_tz::Tz;
use clap::{value_t, ArgMatches};

use crate::chopper::driver::ChopperDriver;
use crate::chopper::header_graph::{HeaderChain, HeaderGraph, HeaderNode};
use crate::chopper::types::{Header, TimestampRange};
use crate::cli::util::YesNoAuto;
use crate::cli_app::CliApp;
use crate::driver::{driver::Driver, merge_join::MergeJoin};
use crate::error::CliResult;
use crate::input::input::{Input, InputFormat, InputType};
use crate::input::input_factory::InputFactory;
use crate::source::csv_configs::{
    CSVOutputConfig, TimestampColConfig, TimestampConfig, TimestampFmtConfig, TimestampStyle,
};
use crate::source::csv_input_config::CSVInputConfig;
use crate::source::csv_timestamp::TimestampUnits;
use crate::source::source::Source;
use crate::source::source_factory::SourceFactory;
use crate::transport::streaming::streaming_transport::StreamingTransport;
use crate::util::tz::ChopperTz;
use crate::write::factory;

pub fn chopper_cli(
    streaming_transports: Option<Vec<Box<dyn StreamingTransport>>>,
    source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    timezone_map: Option<HashMap<&str, Tz>>,
) -> CliResult<()> {
    let mut driver = parse_cli_args(streaming_transports, source_factories, timezone_map)?;
    driver.drive()
}

pub fn parse_cli_args(
    streaming_transports: Option<Vec<Box<dyn StreamingTransport>>>,
    source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    timezone_map: Option<HashMap<&str, Tz>>,
) -> CliResult<Box<dyn ChopperDriver>> {
    let matches = CliApp.create_cli_app().get_matches();

    let timezone = ChopperTz::new_from_cli_arg(matches.value_of("timezone"), timezone_map);

    let timestamp_range = TimestampRange::new(
        matches.value_of("begin"),
        matches.value_of("end"),
        &timezone,
    )?;

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
    assert!(!input_formats.is_empty());
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
    assert!(!inputs.is_empty());

    let output = matches.value_of("output");

    // csv only
    let csv_input_config = parse_csv_input_config(&matches, timezone.clone())?;
    let csv_output_config = parse_csv_output_config(&matches, timezone);

    setup_graph(
        inputs,
        output,
        streaming_transports,
        source_factories,
        timestamp_range,
        csv_input_config,
        csv_output_config,
    )
}

fn setup_graph(
    inputs: Vec<Input>,
    output: Option<&str>,
    streaming_transports: Option<Vec<Box<dyn StreamingTransport>>>,
    source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    timestamp_range: TimestampRange,
    csv_input_config: CSVInputConfig,
    csv_output_config: CSVOutputConfig,
) -> CliResult<Box<dyn ChopperDriver>> {
    // get sources and headers
    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut headers: Vec<Header> = Vec::new();
    let mut input_factory =
        InputFactory::new(csv_input_config, source_factories, streaming_transports)?;

    let mut header_nodes: Vec<HeaderNode> = Vec::new();
    let mut chains: Vec<HeaderChain> = Vec::new();

    for input in &inputs {
        let source = input_factory.create_source_from_input(&input)?;
        headers.push(source.header().clone());
        sources.push(source);

        // add Merge to chains if multiple input files;
        // there is one chain per input file with chain ids from 0 to (inputs.len()-1);
        // there is going to be last chain with id (inputs.len()) added later,
        // where the sink goes, so all the inputs will be merged into this last sink chain,
        // hence inputs.len() as target chain id for the merge
        if inputs.len() > 1 {
            let merge = HeaderNode::Merge(inputs.len());
            let chain = HeaderChain::new(vec![merge]);
            chains.push(chain);
        }
    }

    // add MergeHeaderSink as first header node if multiple input files
    if inputs.len() > 1 {
        let merge = MergeJoin::new(inputs.len())?;
        let header_count_tracker = merge.get_new_header_count_tracker();
        let node_merge_sink = HeaderNode::MergeHeaderSink(merge, header_count_tracker);
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

fn parse_csv_input_config(matches: &ArgMatches, timezone: ChopperTz) -> CliResult<CSVInputConfig> {
    let input_delimiter = matches.value_of("csv_in_delimiter");
    let has_header = value_t!(matches, "csv_in_has_header", YesNoAuto)?;

    let ts_fmt = match matches.value_of("csv_in_ts_fmt_date") {
        None => match matches.value_of("csv_in_ts_fmt") {
            None => match matches.value_of("csv_in_epoch") {
                None => TimestampFmtConfig::Auto,
                Some(units) => TimestampFmtConfig::Epoch(TimestampUnits::from_str(units)),
            },
            Some(fmt) => TimestampFmtConfig::Explicit(fmt.to_string()),
        },
        Some(date_fmt) => {
            // we should always have time format if we have date format
            let time_fmt = matches.value_of("csv_in_ts_fmt_time").unwrap();
            TimestampFmtConfig::DateTimeExplicit(date_fmt.to_string(), time_fmt.to_string())
        }
    };

    let ts_col = match matches.value_of("csv_in_ts_col_date") {
        None => match matches.value_of("csv_in_ts_col") {
            None => TimestampColConfig::Auto,
            Some(ts) => match ts.parse::<usize>() {
                Ok(i) => TimestampColConfig::Index(i),
                Err(_) => TimestampColConfig::Name(ts.to_string()),
            },
        },
        Some(date_col) => {
            // we should always have time column if we have date column
            let time_col = matches.value_of("csv_in_ts_col_time").unwrap();
            match date_col.parse::<usize>() {
                Ok(d_idx) => match time_col.parse::<usize>() {
                    Ok(t_idx) => TimestampColConfig::DateTimeIndex(d_idx, t_idx),
                    Err(_) => {
                        TimestampColConfig::DateTimeName(date_col.to_owned(), time_col.to_owned())
                    }
                },
                Err(_) => {
                    TimestampColConfig::DateTimeName(date_col.to_owned(), time_col.to_owned())
                }
            }
        }
    };
    let ts_config = TimestampConfig::new(ts_col, ts_fmt, timezone);

    Ok(CSVInputConfig::new(ts_config)
        .with_delimiter(input_delimiter)?
        .with_header(has_header))
}

fn parse_csv_output_config(matches: &ArgMatches, timezone: ChopperTz) -> CSVOutputConfig {
    let csv_out_delimiter = matches.value_of("csv_out_delimiter").unwrap();
    let csv_out_print_time_col = match matches.value_of("csv_out_print_time_col").unwrap() {
        "yes" => true,
        "no" => false,
        _ => unreachable!(),
    };

    let time_col_name = match matches.value_of("csv_out_time_col_name") {
        None => None,
        Some(name) => Some(name.to_owned()),
    };

    let time_col_style = if matches.is_present("csv_out_time_fmt_epoch") {
        TimestampStyle::Epoch
    } else {
        TimestampStyle::HumanReadable
    };

    let time_col_units = match matches.value_of("csv_out_time_col_units") {
        None => TimestampUnits::Nanos,
        Some(units) => TimestampUnits::from_str(units),
    };

    CSVOutputConfig::new(
        csv_out_delimiter,
        csv_out_print_time_col,
        time_col_name,
        time_col_style,
        time_col_units,
        timezone,
    )
}
