use std::process;

use crate::dr::dr;
use crate::dr::types::Row;
use crate::source_config::SourceConfig;
use crate::process::driver::single_input_driver::{Action, SingleInputDriver};

pub struct FileRecord {
    reader: Box<dr::Source+'static>,
    timestamp: u64,
    current_row: Option<Row>,
}

impl FileRecord {
    pub fn new(mut conf: SourceConfig) -> Self {
        let mut reader: Box<dr::Source+'static> = conf.get_reader();
        let mut current_row = reader.next_row();
        if current_row.is_none() {
            panic!("Empty file!")
        };

        let timestamp = match current_row {
            Some(r) => {
                let timestamp = r.timestamp;
                current_row = Some(r);
                timestamp
            }
            None => {
                eprintln!("Error: cannot find timestamp");
                process::exit(1)
            }
        };

        FileRecord { reader, timestamp, current_row }
    }

    pub fn get_header(&self) -> &dr::Header {
        &self.reader.header()
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn get_current_row(&self) -> &Option<Row> {
        &self.current_row
    }

    pub fn next(&mut self, input_driver: &Option<SingleInputDriver>) -> bool {
        let mut next_row;
        match input_driver {
            None => {
                next_row = self.reader.next_row();
            }
            Some(d) => {
                let driver: SingleInputDriver = *d;
                loop {
                    next_row = self.reader.next_row();
                    match next_row {
                        None => break,
                        Some(r) => {
                            match driver.filter(r.timestamp) {
                                Action::Stop => {
                                    next_row = None;
                                    break
                                },
                                Action::Write => {
                                    next_row = Some(r);
                                    break
                                },
                                Action::Skip => continue,
                            }
                        }
                    }
                }
            }
        }
        match next_row {
            Some(r) => {
                self.timestamp = r.timestamp;
                self.current_row = Some(r);
                return true
            }
            None => {
                return false
            }
        }
    }
}
