use crate::args;
use crate::dr::dr;
use crate::process::driver::input_driver;
use crate::result::{self, CliResult};

pub struct Read {
    source: Box<dr::Source+'static >,
    writer: Box<dr::Sink>,
    date_range: args::DataRange,
}

impl Read {
    pub fn new(source: Box<dr::Source + 'static>, writer: Box<dr::Sink>, date_range: args::DataRange) -> Self {
        Read { source, writer, date_range }
    }

    fn read(&mut self) -> CliResult<()> {
        input_driver::pump_rows(&self.date_range, &mut self.source, &mut self.writer)?;

        self.writer.flush()?;
        Ok(())
    }
}

impl dr::DRDriver for Read {
    fn drive(&mut self) {
        result::handle_drive_error(self.read());
    }
}
