use crate::args::DataRange;
use crate::dr::dr;
use crate::dr::types::Row;
use crate::process::driver::filter::{self, Action};
use crate::result::CliResult;

/* read, filter and output rows */
pub fn pump_rows(data_range: &DataRange, source: &mut Box<dr::Source+'static>, writer: &mut Box<dr::Sink>) -> CliResult<()> {
    loop {
        let next_row = source.next_row();
        match next_row {
            Some(r) => {
                match filter::filter_data_range(data_range, r.timestamp) {
                    filter::Action::Stop => break,
                    filter::Action::Write => writer.write_row(&r),
                    filter::Action::Skip => continue,
                }
            }
            None => break,
        }
    }
    Ok(())
}

pub fn match_next_row(source: &mut Box<dr::Source+'static>, data_range: &DataRange) -> Option<Row> {
    let mut next_row: Option<Row> = None;
    loop {
        match source.next_row() {
            Some(r) => {
                match filter::filter_data_range(data_range, r.timestamp) {
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
