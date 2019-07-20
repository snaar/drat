use crate::chopper::chopper::Source;
use crate::chopper::header_graph::ChainId;
use crate::chopper::types::{DataRange, Nanos, Row};
use crate::error::CliResult;

pub struct SourceRowBuffer {
    source: Box<Source+'static>,
    chain_id: ChainId,
    timestamp: Nanos,
    row: Option<Row>,
}

impl SourceRowBuffer {
    pub fn new(mut source: Box<dyn Source>, chain_id: ChainId, data_range: &DataRange) -> CliResult<Self> {
        let mut row = match_next_row(&mut source, data_range)?;
        let timestamp = match &mut row {
            Some(r) => r.timestamp,
            None => 0 as Nanos
        };
        Ok(SourceRowBuffer { source, chain_id, timestamp, row })
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

    pub fn has_next(&mut self, data_range: &DataRange) -> CliResult<bool> {
        let next_row = match_next_row(&mut self.source, &data_range)?;
        match next_row {
            Some(r) => {
                self.update_record(r);
                Ok(true)
            }
            None => {
                Ok(false)
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

fn match_next_row(source: &mut Box<dyn Source>, data_range: &DataRange) -> CliResult<Option<Row>> {
    let mut next_row: Option<Row> = None;
    loop {
        match source.next_row()? {
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
    Ok(next_row)
}
