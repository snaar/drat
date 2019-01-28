use crate::args::DataRange;
use crate::dr::types::Nanos;

#[derive(PartialEq)]
pub enum Action {
    Stop,
    Write,
    Skip,
}

pub fn filter_data_range(data_range: &DataRange, timestamp: Nanos) -> Action {
    if data_range.end.is_some() && timestamp >= data_range.end.unwrap() {
        return Action::Stop;
    }
    match data_range.begin.is_none() || timestamp >= data_range.begin.unwrap() {
        true => Action::Write,
        false => Action::Skip,
    }
}
