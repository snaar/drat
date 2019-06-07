/* Some code in this file was adapted from public domain xsv project. */
use std::fmt;
use std::io;
use std::process;

use clap;
use csv;

pub type CliResult<T> = Result<T, CliError>;

#[derive(Debug)]
pub enum CliError {
    Flag(clap::Error),
    Csv(csv::Error),
    Io(io::Error),
    Data(String),
    Other(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CliError::Flag(ref e) => { e.fmt(f) }
            CliError::Csv(ref e) => { e.fmt(f) }
            CliError::Io(ref e) => { e.fmt(f) }
            CliError::Data(ref s) => { f.write_str(&**s) }
            CliError::Other(ref s) => { f.write_str(&**s) }
        }
    }
}

impl From<clap::Error> for CliError {
    fn from(err: clap::Error) -> CliError {
        CliError::Flag(err)
    }
}

impl From<csv::Error> for CliError {
    fn from(err: csv::Error) -> CliError {
        if !err.is_io_error() {
            return CliError::Csv(err);
        }
        match err.into_kind() {
            csv::ErrorKind::Io(v) => From::from(v),
            _ => unreachable!(),
        }
    }
}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> CliError {
        CliError::Io(err)
    }
}

impl From<String> for CliError {
    fn from(err: String) -> CliError {
        CliError::Other(err)
    }
}

impl<'a> From<&'a str> for CliError {
    fn from(err: &'a str) -> CliError {
        CliError::Other(err.to_owned())
    }
}

macro_rules! write_error {
    ($($arg:tt)*) => ({
        use std::io::Write;
        (writeln!(&mut ::std::io::stderr(), $($arg)*)).unwrap();
        process::exit(1);
    });
}

pub fn handle_drive_error(cli_result: CliResult<()>) {
    match cli_result {
        Ok(()) => process::exit(0),
        Err(CliError::Flag(err)) => err.exit(),
        Err(CliError::Csv(err)) => write_error!("{}", err),
        Err(CliError::Io(ref err)) if err.kind() == io::ErrorKind::BrokenPipe => {
        },
        Err(CliError::Io(err)) => write_error!("{}", err),
        Err(CliError::Data(msg)) => write_error!("{}", msg),
        Err(CliError::Other(msg)) => write_error!("{}", msg),
    }
}
