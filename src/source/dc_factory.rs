use std::io;
use std::path::PathBuf;

use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::{dc_source, source_factory::SourceFactory};

pub struct DCFactory;

impl DCFactory {
    pub fn new() -> DCFactory {
        DCFactory { }
    }
}

impl SourceFactory for DCFactory {
    fn can_create_from(&self, path: &PathBuf) -> bool {
        path.extension().unwrap().eq("dc")
    }

    fn create_source(&mut self, reader: Box<io::Read>) -> CliResult<Box<Source+'static>> {
        Ok(Box::new(dc_source::DCSource::new(reader)?))
    }
}
