use clap::crate_version;
use clap::{App, Arg};

pub struct CliApp;

impl CliApp {
    pub fn create_cli_app(&self) -> App {
        let app = App::new("chopper")
            .version(crate_version!())
            .about("chopper is a simple streaming time series tool")
            .arg(
                Arg::with_name("input")
                    .help("sets the input files to use; \nif missing, stdin will be used")
                    .multiple(true),
            )
            .arg(
                Arg::with_name("output")
                    .long("output")
                    .short("o")
                    .help("output to a file")
                    .takes_value(true)
                    .value_name("file"),
            )
            .arg(
                Arg::with_name("format")
                    .short("f")
                    .long("format")
                    .help(
                        "input file format override list; \
                        applied in order of input file arguments; \
                        if there are more input files than file format overrides, \
                        last format override will be used for remaining files; \
                        controls stdin format as appropriate",
                    )
                    .takes_value(true)
                    .default_value("auto")
                    .multiple(true),
            )
            .arg(
                Arg::with_name("timezone")
                    .short("z")
                    .long("timezone")
                    .help("specify time zone for timestamp.")
                    .takes_value(true)
                    .case_insensitive(true)
                    .value_name("arg"),
            )
            .arg(
                Arg::with_name("begin")
                    .short("b")
                    .long("begin")
                    .help("set begin timestamp (inclusive); \ndate: yyyymmdd, time: hh:mm:ss")
                    .takes_value(true)
                    .value_name("timestamp"),
            )
            .arg(
                Arg::with_name("end")
                    .short("e")
                    .long("end")
                    .help("set end timestamp (exclusive); \ndate: yyyymmdd, time: hh:mm:ss")
                    .takes_value(true)
                    .value_name("timestamp"),
            )
            .arg(
                Arg::with_name("backtrace")
                    .long("backtrace")
                    .help("print backtrace"),
            )
            //  below are csv only
            .arg(
                Arg::with_name("csv_input_delimiter")
                    .long("csv-in-delimiter")
                    .help("csv input only: column delimiter [default: try each of ',\\t ']")
                    .takes_value(true)
                    .value_name("arg"),
            )
            .arg(
                Arg::with_name("csv_output_delimiter")
                    .long("csv-out-delimiter")
                    .help("csv output only: column delimiter")
                    .takes_value(true)
                    .default_value(",")
                    .value_name("arg"),
            )
            .arg(
                Arg::with_name("csv_input_has_header")
                    .long("csv-in-has-header")
                    .help("csv input only: has header row?")
                    .takes_value(true)
                    .default_value("auto")
                    .possible_values(&["yes", "no", "auto"])
                    .case_insensitive(true)
                    .value_name("arg"),
            )
            // print timestamp
            .arg(
                Arg::with_name("csv_print_ts")
                    .long("csv-print-ts")
                    .help("csv only: print timestamp as first column")
                    .takes_value(true)
                    .default_value("auto")
                    .possible_values(&["yes", "no", "auto"])
                    .case_insensitive(true)
                    .value_name("arg"),
            )
            // timestamp column
            .arg(
                Arg::with_name("csv_ts_col")
                    .long("csv-ts")
                    .help("csv only: specify the timestamp column index")
                    .takes_value(true)
                    .default_value("0")
                    .value_name("arg")
                    .conflicts_with_all(&["csv_ts_col_date", "csv_ts_col_time"]),
            )
            // timestamp column date
            .arg(
                Arg::with_name("csv_ts_col_date")
                    .long("csv-ts-date")
                    .help(
                        "csv only: specify the timestamp date column index. \
                        \nused when date and time are in separate columns",
                    )
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_ts_col_time")
                    .conflicts_with("csv_ts_col"),
            )
            // timestamp column time
            .arg(
                Arg::with_name("csv_ts_col_time")
                    .long("csv-ts-time")
                    .help(
                        "csv only: specify the timestamp time column index. \
                        \nused when date and time are in separate columns",
                    )
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_ts_col_date")
                    .conflicts_with("csv_ts_col"),
            )
            // timestamp format
            .arg(
                Arg::with_name("csv_ts_fmt")
                    .long("csv-ts-fmt")
                    .help("csv only: specify the timestamp datetime format")
                    .takes_value(true)
                    .value_name("arg")
                    .conflicts_with_all(&["csv_ts_fmt_date", "csv_ts_fmt_time"]),
            )
            // timestamp format date
            .arg(
                Arg::with_name("csv_ts_fmt_date")
                    .long("csv-ts-fmt-date")
                    .help("csv only: specify the timestamp date format")
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_ts_fmt_time")
                    .conflicts_with("csv_ts_fmt_datetime"),
            )
            // timestamp format time
            .arg(
                Arg::with_name("csv_ts_fmt_time")
                    .long("csv-ts-fmt-time")
                    .help(
                        "csv only: specify the timestamp time format \
                        \n[default: %H:%M:%S]",
                    )
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_timestamp_format_date")
                    .conflicts_with("csv_ts_fmt_datetime"),
            );
        app
    }
}
