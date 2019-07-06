use std::io;
use std::path::PathBuf;

use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::csv_config::CSVConfig;
use crate::source::csv_source::CSVSource;
use crate::source::source_factory::SourceFactory;

pub struct CSVFactory {
    pub csv_config: CSVConfig
}

impl CSVFactory {
    pub fn new(csv_config: CSVConfig) -> Self {
        CSVFactory { csv_config }
    }
}

impl SourceFactory for CSVFactory {
    fn can_create_from(&self, path: &PathBuf) -> bool {
        path.extension().unwrap().eq("csv")
    }

    fn create_source(&mut self, reader: Box<io::Read>) -> CliResult<Box<Source+'static>> {
        Ok(Box::new(CSVSource::new(reader, &self.csv_config)?))
    }
}
