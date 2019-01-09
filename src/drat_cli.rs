use clap::{Arg, App};
use clap::crate_version;

use crate::args;
use crate::input::input_factory::InputFactory;
use crate::process::command::Command;
use crate::util;

pub fn drat_cli(input_factories: Vec<Box<InputFactory>>) {
    let matches = App::new("drat")
        .version(crate_version!())
        .about("drat is a simple streaming time series tool")
        .arg(Arg::with_name("begin")
            .short("b")
            .long("begin")
            .help("Set begin timestamp (inclusive)")
            .takes_value(true)
            .value_name("TIMESTAMP"))
        .arg(Arg::with_name("end")
            .short("e")
            .long("end")
            .help("Set end timestamp (exclusive)")
            .takes_value(true)
            .value_name("TIMESTAMP"))
        .arg(Arg::with_name("timestamp_column")
            .short("t")
            .long("timestamp-column")
            .help("Specify the timestamp column [default: 0]")
            .takes_value(true)
            .value_name("ARG"))
        .arg(Arg::with_name("INPUT")
            .help("Sets the input files to use; \n\
            if missing, stdin will be used")
            .multiple(true))
        .arg(Arg::with_name("has_headers")
            .long("has-headers")
            .help("Input files have headers"))
        .arg(Arg::with_name("output")
            .long("output")
            .short("o")
            .help("Output to a file")
            .takes_value(true)
            .value_name("FILE"))
        .arg(Arg::with_name("delimiter")
            .long("delimiter")
            .short("d")
            .help("Field/column delimiter")
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
    let inputs: Vec<&str> = match matches.values_of("INPUT") {
        None => vec![],
        Some(t) => t.collect(),
    };

    let output = matches.value_of("output");
    let has_headers = matches.is_present("has_headers");

    let delimiter = matches.value_of("delimiter").unwrap();
    let delimiter = util::parse_into_delimiter(delimiter).unwrap();

    let argv = args::Args {
        inputs,
        input_factories,
        begin,
        end,
        timestamp_column,
        output,
        has_headers,
        delimiter,
    };

    match argv.inputs.len() {
        0 | 1 => Command::Read,
        _ => Command::Collate,
    }.run(argv);
}
