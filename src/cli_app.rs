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
                    .help("output to a file; default output is stdout")
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
                        can use 'auto' to ask for file format autodetection; \
                        if there are more input files than file format overrides, \
                        last format override will be used for remaining files; \
                        controls stdin format as appropriate; if missing, will attempt \
                        to autodetect file format first using file name, then using file contents",
                    )
                    .takes_value(true)
                    .require_delimiter(true)
                    .value_name("f1[,f2[,etc]]"),
            )
            .arg(
                Arg::with_name("timezone")
                    .short("z")
                    .long("timezone")
                    .help(
                        "specify time zone to use when reading/writing fields with \
                        no explicit timezone; optional but chopper will fail if timezone \
                        ends up being required; if CHOPPER_TZ env var is present, \
                        then that will be used if cli arg is missing",
                    )
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
                Arg::with_name("csv_out_print_ts")
                    .long("csv-out-print-ts")
                    .help("csv output only: print timestamp as first column")
                    .takes_value(true)
                    .default_value("auto")
                    .possible_values(&["yes", "no", "auto"])
                    .case_insensitive(true)
                    .value_name("arg"),
            )
            // timestamp column
            .arg(
                Arg::with_name("csv_in_ts_col")
                    .long("csv-in-ts-col")
                    .help("csv input only: specify the timestamp column name or index")
                    .takes_value(true)
                    .default_value("0")
                    .value_name("arg")
                    .conflicts_with_all(&["csv_in_ts_col_date", "csv_in_ts_col_time"]),
            )
            // timestamp column date
            .arg(
                Arg::with_name("csv_in_ts_col_date")
                    .long("csv-in-ts-col-date")
                    .help(
                        "csv input only: specify the split timestamp date-only column name or index; \
                        \nused when date and time are in separate columns",
                    )
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_in_ts_col_time")
                    .conflicts_with("csv_in_ts_col"),
            )
            // timestamp column time
            .arg(
                Arg::with_name("csv_in_ts_col_time")
                    .long("csv-in-ts-col-time")
                    .help(
                        "csv input only: specify the split timestamp time-only column name or index; \
                        \nused when date and time are in separate columns",
                    )
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_in_ts_col_date")
                    .conflicts_with("csv_in_ts_col"),
            )
            // timestamp format
            .arg(
                Arg::with_name("csv_in_ts_fmt")
                    .long("csv-in-ts-fmt")
                    .help("csv input only: specify the timestamp column format")
                    .takes_value(true)
                    .value_name("arg")
                    .conflicts_with_all(&["csv_in_ts_fmt_date", "csv_in_ts_fmt_time"]),
            )
            // timestamp format date
            .arg(
                Arg::with_name("csv_in_ts_fmt_date")
                    .long("csv-in-ts-fmt-date")
                    .help("csv input only: specify the split timestamp date-only column format")
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_in_ts_fmt_time")
                    .conflicts_with("csv_in_ts_fmt"),
            )
            // timestamp format time
            .arg(
                Arg::with_name("csv_in_ts_fmt_time")
                    .long("csv-in-ts-fmt-time")
                    .help(
                        "csv input only: specify the split timestamp time-only column format \
                        \n[default: %H:%M:%S]",
                    )
                    .takes_value(true)
                    .value_name("arg")
                    .requires("csv_in_ts_fmt_date")
                    .conflicts_with("csv_in_ts_fmt"),
            );
        app
    }
}
