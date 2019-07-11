use crate::chopper::chopper::HeaderSink;
use crate::error::{CliResult, Error};
use crate::source::csv_configs::CSVOutputConfig;
use crate::write::csv_sink;
use crate::write::dc_sink;

pub fn new_header_sink(output: Option<String>,
                       csv_output_config: Option<CSVOutputConfig>) -> CliResult<Box<dyn HeaderSink +'static>> {
    let csv_output_config = match csv_output_config {
        Some(c) => c,
        None => CSVOutputConfig::new_default()
    };
    let writer: Box<dyn HeaderSink +'static>;
    match output {
        Some(p) => {
            if p.ends_with("csv") {
                writer = Box::new(csv_sink::CSVSink::new(&Some(p), csv_output_config)?);
            } else if p.ends_with("dc") {
                writer = Box::new(dc_sink::DCSink::new(&Some(p))?);
            } else {
                return Err(Error::from(format!("Error: file type -- {} is not supported", p)))
            }
        }
        None => { // if none use csv sink
            writer = Box::new(csv_sink::CSVSink::new(&None, csv_output_config)?);
        }
    }
    Ok(writer)
}
