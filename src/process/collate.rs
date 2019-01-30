use crate::args;
use crate::dr::dr;
use crate::process::driver::single_file_record::SingleFileRecord;
use crate::result::{self, CliResult};

pub struct Collate {
    sources: Vec<Box<dr::Source+'static>>,
    writer: Box<dr::Sink>,
    date_range: args::DataRange,
}

impl Collate {
    pub fn new(sources: Vec<Box<dr::Source+'static>>, writer: Box<dr::Sink>, date_range: args::DataRange) -> Self {
        Collate { sources, writer, date_range }
    }

    fn collate(&mut self) -> CliResult<()> {
        // creates file record for each file and add to vector
        let mut file_records = Vec::with_capacity(self.sources.len());
        for source in &mut self.sources {
            file_records.push(SingleFileRecord::new(source));
        }

        // sort, merge, and output
        let mut record_len = file_records.len();
        while record_len > 0 {
            let index = Self::get_min_index(&file_records);
            let row = file_records[index].get_current_row().clone().unwrap();
            self.writer.write_row(&row)?;

            loop {
                if !file_records[index].next(&self.date_range) {
                    file_records.remove(index);
                }
                break;
            }
            record_len = file_records.len();
        }

        self.writer.flush()?;
        Ok(())
    }

    fn get_min_index(file_records: &Vec<SingleFileRecord>) -> usize {
        let min = file_records
            .iter()
            .enumerate()
            .min_by(|&(_, i1), &(_, i2)|
                i1.get_timestamp().cmp(&i2.get_timestamp())).unwrap();
        min.0
    }
}

impl dr::DRDriver for Collate {
    fn drive(&mut self) {
        result::handle_drive_error(self.collate())
    }
}
