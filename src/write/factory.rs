use crate::chopper::sink::DynHeaderSink;
use crate::error::{CliResult, Error};
use crate::source::csv_configs::CSVOutputConfig;
use crate::util::path::buf_writer_from_file_path;
use crate::write::csv_sink;
use crate::write::dc_sink;

pub fn new_header_sink(
    output: Option<&str>,
    csv_output_config: Option<CSVOutputConfig>,
) -> CliResult<Box<dyn DynHeaderSink>> {
    let csv_output_config = match csv_output_config {
        Some(c) => c,
        None => CSVOutputConfig::new_default(),
    };

    let writer: Box<dyn DynHeaderSink> = match output {
        Some(p) => {
            let p = p.to_string();
            if p.ends_with("csv") {
                let writer = buf_writer_from_file_path(&Some(p))?;
                Box::new(csv_sink::CSVSink::new(writer, csv_output_config)?)
            } else if p.ends_with("dc") {
                let writer = buf_writer_from_file_path(&Some(p))?;
                Box::new(dc_sink::DCSink::new(writer)?)
            } else {
                return Err(Error::from(format!("file type -- {} is not supported", p)));
            }
        }
        None => {
            // if none use csv sink
            let writer = buf_writer_from_file_path(&None)?;
            Box::new(csv_sink::CSVSink::new(writer, csv_output_config)?)
        }
    };
    Ok(writer)
}
