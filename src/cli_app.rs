use clap::{App, Arg};
use clap::crate_version;

pub struct CliApp;

impl CliApp {
    pub fn create_cli_app(&self) -> App {
        let app = App::new("chopper")
            .version(crate_version!())
            .about("chopper is a simple streaming time series tool")
            .arg(Arg::with_name("input")
                .help("sets the input files to use; \nif missing, stdin will be used")
                .multiple(true))
            .arg(Arg::with_name("output")
                .long("output")
                .short("o")
                .help("output to a file")
                .takes_value(true)
                .value_name("file"))
            .arg(Arg::with_name("fallback_file_ext")
                .long("fallback-file-ext")
                .help("file format extention to assume when cannot deduce input file format")
                .takes_value(true)
                .default_value("csv")
                .value_name("ext"))
            .arg(Arg::with_name("timezone")
                .short("z")
                .long("timezone")
                .help("specify time zone for timestamp.")
                .takes_value(true)
                .case_insensitive(true)
                .value_name("arg"))
            .arg(Arg::with_name("begin")
                .short("b")
                .long("begin")
                .help("set begin timestamp (inclusive); \ndate: yyyymmdd, time: hh:mm:ss")
                .takes_value(true)
                .value_name("timestamp"))
            .arg(Arg::with_name("end")
                .short("e")
                .long("end")
                .help("set end timestamp (exclusive); \ndate: yyyymmdd, time: hh:mm:ss")
                .takes_value(true)
                .value_name("timestamp"))
            .arg(Arg::with_name("backtrace")
                .long("backtrace")
                .help("print backtrace"))

            //  below are csv only
            .arg(Arg::with_name("csv_input_delimiter")
                .long("csv-in-delimiter")
                .help("csv only: input field/column delimiter")
                .takes_value(true)
                .default_value(",")
                .value_name("arg"))
            .arg(Arg::with_name("csv_output_delimiter")
                .long("csv-out-delimiter")
                .help("csv only: output field/column delimiter")
                .takes_value(true)
                .default_value(",")
                .value_name("arg"))

            // has header
            .arg(Arg::with_name("csv_has_header")
                .long("csv-has-header")
                .help("csv only: input files have header"))

            // print timestamp
            .arg(Arg::with_name("csv_print_ts")
                .long("csv-print-ts")
                .help("csv only: print timestamp as first column")
                .takes_value(true)
                .default_value("auto")
                .possible_values(&["true", "false", "auto"])
                .case_insensitive(true)
                .value_name("arg"))

            // timestamp column
            .arg(Arg::with_name("csv_ts_col")
                .long("csv-ts")
                .help("csv only: specify the timestamp column index")
                .takes_value(true)
                .default_value("0")
                .value_name("arg")
                .conflicts_with_all(&["csv_ts_col_date", "csv_ts_col_time"]))
            // timestamp column date
            .arg(Arg::with_name("csv_ts_col_date")
                .long("csv-ts-date")
                .help("csv only: specify the timestamp date column index. \
                        \nused when date and time are in separate columns")
                .takes_value(true)
                .value_name("arg")
                .requires("csv_ts_col_time")
                .conflicts_with("csv_ts_col"))
            // timestamp column time
            .arg(Arg::with_name("csv_ts_col_time")
                .long("csv-ts-time")
                .help("csv only: specify the timestamp time column index. \
                        \nused when date and time are in separate columns")
                .takes_value(true)
                .value_name("arg")
                .requires("csv_ts_col_date")
                .conflicts_with("csv_ts_col"))

            // timestamp format
            .arg(Arg::with_name("csv_ts_fmt")
                .long("csv-ts-fmt")
                .help("csv only: specify the timestamp datetime format")
                .takes_value(true)
                .value_name("arg")
                .conflicts_with_all(&["csv_ts_fmt_date", "csv_ts_fmt_time"]))
            // timestamp format date
            .arg(Arg::with_name("csv_ts_fmt_date")
                .long("csv-ts-fmt-date")
                .help("csv only: specify the timestamp date format")
                .takes_value(true)
                .value_name("arg")
                .requires("csv_ts_fmt_time")
                .conflicts_with("csv_ts_fmt_datetime"))
            // timestamp format time
            .arg(Arg::with_name("csv_ts_fmt_time")
                .long("csv-ts-fmt-time")
                .help("csv only: specify the timestamp time format \
                        \n[default: %H:%M:%S]")
                .takes_value(true)
                .value_name("arg")
                .requires("csv_timestamp_format_date")
                .conflicts_with("csv_ts_fmt_datetime"));
        app
    }
}
