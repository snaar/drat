use std::process;

use crate::dr::dr::HeaderSink;
use crate::write::csv_sink;
use crate::write::dc_sink;

pub fn new_header_sink(output: Option<String>) -> Box<dyn HeaderSink +'static> {
    let writer: Box<dyn HeaderSink +'static>;
    match output {
        Some(p) => {
            if p.ends_with("csv") {
                writer = Box::new(csv_sink::CSVSink::new(&Some(p)));
            } else if p.ends_with("dc") {
                writer = Box::new(dc_sink::DCSink::new(&Some(p)));
            } else {
                println!("Error: file type -- {} is not supported", p);
                process::exit(1);
            }
        }
        None => { // if none use csv sink
            writer = Box::new(csv_sink::CSVSink::new(&None));
        }
    }
    writer
}
