use std::process;

use crate::args::DataRange;
use crate::dr::dr::Source;
use crate::dr::graph::ChainId;
use crate::dr::types::Nanos;
use crate::dr::types::Row;

pub struct SourceRowBuffer {
    source: Box<Source+'static>,
    chain_id: ChainId,
    timestamp: u64,
    row: Option<Row>,
}

impl SourceRowBuffer {
    pub fn new(mut source: Box<Source+'static>, chain_id: ChainId) -> Self {
        let mut row = source.next_row();
        if row.is_none() {
            panic!("Empty file!")
        };

        let timestamp = match row {
            Some(r) => {
                let timestamp = r.timestamp;
                println!("timestamp: {}", timestamp);
                row = Some(r);
                timestamp
            }
            None => write_error!("Error: cannot find timestamp")
        };
        SourceRowBuffer { source, chain_id, timestamp, row }
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn row(&self) -> &Option<Row> {
        &self.row
    }

    pub fn chain_id(&mut self) -> ChainId {
        self.chain_id
    }

    fn update_record(&mut self, next_row: Row) {
        self.timestamp = next_row.timestamp;
        self.row = Some(next_row);
    }

    pub fn next(&mut self, data_range: &DataRange) -> bool {
        let next_row = match_next_row(&mut self.source, &data_range);
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

#[derive(PartialEq)]
enum Action {
    Stop,
    Write,
    Skip,
}

fn filter_data_range(data_range: &DataRange, timestamp: Nanos) -> Action {
    if data_range.end.is_some() && timestamp >= data_range.end.unwrap() {
        return Action::Stop;
    }
    match data_range.begin.is_none() || timestamp >= data_range.begin.unwrap() {
        true => Action::Write,
        false => Action::Skip,
    }
}

fn match_next_row(source: &mut Box<Source+'static>, data_range: &DataRange) -> Option<Row> {
    let mut next_row: Option<Row> = None;
    loop {
        match source.next_row() {
            Some(r) => {
                match filter_data_range(data_range, r.timestamp) {
                    Action::Stop => {
                        break
                    },
                    Action::Write => {
                        next_row = Some(r);
                        break
                    },
                    Action::Skip => continue,
                }
            }
            None => break,
        }
    }
    next_row
}
