use std::io;
use std::path::Path;

use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::csv_configs::CSVInputConfig;
use crate::source::csv_source::CSVSource;
use crate::source::source_factory::SourceFactory;

pub struct CSVFactory {
    pub csv_input_config: CSVInputConfig
}

impl CSVFactory {
    pub fn new(csv_input_config: CSVInputConfig) -> Self {
        CSVFactory { csv_input_config }
    }
}

impl SourceFactory for CSVFactory {
    fn can_create_from(&self, path: &Path) -> bool {
        path.extension().unwrap().eq("csv")
    }

    fn create_source(&mut self, reader: Box<dyn io::Read>) -> CliResult<Box<dyn Source>> {
        Ok(Box::new(CSVSource::new(reader, &self.csv_input_config)?))
    }
}
