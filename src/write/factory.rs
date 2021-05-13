use crate::chopper::chopper::HeaderSink;
use crate::error::{CliResult, Error};
use crate::source::csv_configs::CSVOutputConfig;
use crate::write::csv_sink;
use crate::write::dc_sink;

pub fn new_header_sink(
    output: Option<&str>,
    csv_output_config: Option<CSVOutputConfig>,
) -> CliResult<Box<dyn HeaderSink>> {
    let csv_output_config = match csv_output_config {
        Some(c) => c,
        None => CSVOutputConfig::new_default(),
    };

    let writer: Box<dyn HeaderSink> = match output {
        Some(p) => {
            let p = p.to_string();
            if p.ends_with("csv") {
                Box::new(csv_sink::CSVSink::new(&Some(p), csv_output_config)?)
            } else if p.ends_with("dc") {
                Box::new(dc_sink::DCSink::new(&Some(p))?)
            } else {
                return Err(Error::from(format!("file type -- {} is not supported", p)));
            }
        }
        None => {
            // if none use csv sink
            Box::new(csv_sink::CSVSink::new(&None, csv_output_config)?)
        }
    };
    Ok(writer)
}
