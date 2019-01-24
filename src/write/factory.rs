use std::process;

use crate::dr::dr;
use crate::source_config::CSVConfig;
use crate::write::csv_sink;
use crate::write::dc_sink;

pub fn new_sink(output: &Option<&str>, header: &dr::Header, csv_config: &CSVConfig) -> Box<dr::Sink+'static> {
    let writer: Box<dr::Sink+'static>;
    match output {
        Some(p) => {
            if p.ends_with("csv") {
                writer = Box::new(csv_sink::CSVSink::new(&Some(p), header, csv_config.has_headers()));
            } else if p.ends_with("dc") {
                writer = Box::new(dc_sink::DCSink::new(&Some(p), header));
            } else {
                println!("Error: file type -- {} is not supported", p);
                process::exit(1);
            }
        }
        None => { // if none use csv sink
            writer = Box::new(csv_sink::CSVSink::new(&None, header, csv_config.has_headers()));
        }
    }
    writer
}