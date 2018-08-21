use csv::{StringRecord, StringRecordsIntoIter};
use std::io::Read;
use std::process;

use config::Config;
use read_filter::{Action, ReadFilter};

pub struct FileRecord {
    header: Option<StringRecord>,
    timestamp: u64,
    current_row: StringRecord,
    timestamp_column: usize,
    iterator: StringRecordsIntoIter<Box<Read>>,
}

impl FileRecord {
    pub fn new(conf: Config, timestamp_column: usize) -> Self {
        let mut reader = match conf.reader() {
            Ok(r) => r,
            Err(err) => {
                werr!("Error: {}", err);
                process::exit(1);
            },
        };
        let header = Some(reader.headers().unwrap().clone());
        let mut iterator = reader.into_records();
        let current_row = match iterator.next() {
            Some(r) => r.unwrap(),
            None => panic!("Empty file!")
        };
        let timestamp = current_row.get(timestamp_column).unwrap().parse::<u64>().unwrap();

        FileRecord { header, timestamp, current_row, timestamp_column, iterator }
    }

    pub fn get_header(&self) -> &Option<StringRecord> {
        &self.header
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn get_current_row(&self) -> &StringRecord {
        &self.current_row
    }

    pub fn next(&mut self, filter: &Option<ReadFilter>) -> bool {
        let mut next_row = None;
        let iterator = &mut self.iterator;
        match filter {
            Some(f) => {
                for mut record in iterator {
                    let read_filter: ReadFilter = *f;
                    let row = match record {
                        Ok(r) => r,
                        Err(_e) => {
                            next_row = None;
                            break
                        }
                    };
                    let filter = read_filter.filter(&row);
                    if filter == Action::Stop {
                        break;
                    }
                    if filter == Action::Write {
                        next_row = Some(row);
                        break;
                    }
                }
            }
            None => {
                next_row = Some(iterator.next().unwrap().unwrap());
            }
        }
        let return_bool = match next_row {
            Some(i) => {
                self.current_row = i;
                self.timestamp = self.current_row.get(self.timestamp_column).unwrap().parse::<u64>().unwrap();
                true
            },
            None => {
                false
            }
        };
        return_bool
    }
}
