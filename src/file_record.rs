use std::process;

use crate::read::dr;
use crate::config::Config;
use crate::read::types::{Row,};
use crate::read_filter::{Action, ReadFilter};

pub struct FileRecord {
    reader: Box<dr::Reader+'static>,
    header: Vec<String>,
    timestamp: u64,
    current_row: Option<Row>,
    timestamp_column: usize,
}

impl FileRecord {
    pub fn new(mut conf: Config, timestamp_column: usize) -> Self {
        let mut reader = match conf.reader() {
            Ok(r) => r,
            Err(err) => {
                werr!("Error: {}", err);
                process::exit(1);
            },
        };
        let header = reader.header().to_owned();
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

        FileRecord { reader, header, timestamp, current_row, timestamp_column }
    }

    pub fn get_header(&self) -> &Vec<String> {
        &self.header
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn get_current_row(&self) -> &Option<Row> {
        &self.current_row
    }

    pub fn next(&mut self, filter: &Option<ReadFilter>) -> bool {
        let mut next_row;
        match filter {
            None => {
                next_row = self.reader.next_row();
            }
            Some(f) => {
                let read_filter: ReadFilter = *f;
                loop {
                    next_row = self.reader.next_row();
                    match next_row {
                        None => break,
                        Some(r) => {
                            match read_filter.filter(r.timestamp) {
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
