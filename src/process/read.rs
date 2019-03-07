use crate::args;
use crate::dr::dr::{DataSink, DRDriver, Source};
use crate::process::driver::input_driver;
use crate::result::{self, CliResult};

pub struct Read {
    source: Box<Source+'static >,
    writer: Box<DataSink>,
    date_range: args::DataRange,
}

impl Read {
    pub fn new(source: Box<Source + 'static>, writer: Box<dyn DataSink>, date_range: args::DataRange) -> Self {
        Read { source, writer, date_range }
    }

    fn read(&mut self) -> CliResult<()> {
        input_driver::pump_rows(&self.date_range, &mut self.source, &mut self.writer)?;

        self.writer.flush()?;
        Ok(())
    }
}

impl DRDriver for Read {
    fn drive(&mut self) {
        result::handle_drive_error(self.read());
    }
}
