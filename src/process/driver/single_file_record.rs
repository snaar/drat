use std::process;

use crate::args::DataRange;
use crate::dr::dr;
use crate::dr::types::Row;
use crate::process::driver::input_driver;

pub struct SingleFileRecord<'a> {
    source: &'a mut Box<dr::Source+'static>,
    timestamp: u64,
    current_row: Option<Row>,
}

impl <'a> SingleFileRecord<'a> {
    pub fn new(source: &'a mut Box<dr::Source+'static>) -> Self {
        let mut current_row = source.next_row();
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

        SingleFileRecord { source, timestamp, current_row }
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn get_current_row(&self) -> &Option<Row> {
        &self.current_row
    }

    fn update_record(&mut self, next_row: Row) {
        self.timestamp = self.timestamp;
        self.current_row = Some(next_row);
    }

    pub fn next(&mut self, data_range: &DataRange) -> bool {
        let next_row = input_driver::match_next_row(self.source, &data_range);
        match next_row {
            Some(r) => {
                self.update_record(r);
                true
            }
            None => {
                false
            }
        }
    }
}
