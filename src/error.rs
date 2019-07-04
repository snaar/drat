/* Some code in this file was adapted from public domain xsv project. */
use std::fmt;
use std::io;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};

use clap;
use csv;

macro_rules! write_error {
    ($($arg:tt)*) => ({
        use std::io::{Write, stderr};
        (writeln!(&mut stderr(), $($arg)*)).unwrap();
    });
}

pub type CliResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    CliParsing(clap::Error),
    Csv(csv::Error),
    Io(io::Error),
    Custom(String),
}

impl Error {
    // print error and exit process
    pub fn exit(&mut self) -> ! {
        match &self {
            Error::CliParsing(err) => err.exit(),
            Error::Csv(err) => write_error!("{}", err),
            Error::Io(ref err) if err.kind() == io::ErrorKind::BrokenPipe => {},
            Error::Io(err) => write_error!("{}", err),
            Error::Custom(s) => write_error!("Error: {}", s),
        }
        process::exit(1);
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Error::CliParsing(ref e) => e.fmt(f),
            Error::Csv(ref e) => e.fmt(f),
            Error::Io(ref e) => e.fmt(f),
            Error::Custom(s) => f.write_str(format!("Error: {}", s).as_str()),
        }
    }
}

impl From<clap::Error> for Error {
    fn from(err: clap::Error) -> Error {
        print_backtrace();
        Error::CliParsing(err)
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Error {
        print_backtrace();
        if !err.is_io_error() {
            return Error::Csv(err)
        }
        match err.into_kind() {
            csv::ErrorKind::Io(v) => From::from(v),
            _ => unreachable!(),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        print_backtrace();
        Error::Io(err)
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        print_backtrace();
        Error::Custom(err)
    }
}

impl<'a> From<&'a str> for Error {
    fn from(err: &'a str) -> Error {
        print_backtrace();
        Error::Custom(err.to_owned())
    }
}

pub fn handle_drive_error(cli_result: CliResult<()>) {
    match cli_result {
        Ok(()) => process::exit(0),
        Err(e) => {
            write_error!("");
            match e {
                Error::CliParsing(err) => write_error!("{}", err),
                Error::Csv(err) => write_error!("{}", err),
                Error::Io(ref err) if err.kind() == io::ErrorKind::BrokenPipe => {},
                Error::Io(err) => write_error!("{}", err),
                Error::Custom(s) => write_error!("Error: {} ", s),
            }
            process::exit(1)
        }
    }
}

static PRINT_BACKTRACE: AtomicBool = AtomicBool::new(false);

pub fn turn_on_backtrace() {
    PRINT_BACKTRACE.store(true, Ordering::Relaxed);
}

fn print_backtrace() {
    if !PRINT_BACKTRACE.load(Ordering::Relaxed) {
        return
    }
    use backtrace::Backtrace;
    let bt = Backtrace::new();
    eprintln!("{:?}", bt);
}
