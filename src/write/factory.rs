use crate::chopper::chopper::HeaderSink;
use crate::error::{CliResult, Error};
use crate::write::csv_sink;
use crate::write::dc_sink;

pub fn new_header_sink(output: Option<String>) -> CliResult<Box<dyn HeaderSink +'static>> {
    let writer: Box<dyn HeaderSink +'static>;
    match output {
        Some(p) => {
            if p.ends_with("csv") {
                writer = Box::new(csv_sink::CSVSink::new(&Some(p))?);
            } else if p.ends_with("dc") {
                writer = Box::new(dc_sink::DCSink::new(&Some(p))?);
            } else {
                return Err(Error::from(format!("Error: file type -- {} is not supported", p)))
            }
        }
        None => { // if none use csv sink
            writer = Box::new(csv_sink::CSVSink::new(&None)?);
        }
    }
    Ok(writer)
}
