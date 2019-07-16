use std::io;
use std::path::Path;

use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::{dc_source, source_factory::SourceFactory};

pub struct DCFactory;

impl SourceFactory for DCFactory {
    fn can_create_from(&self, path: &Path) -> bool {
        path.extension().unwrap().eq("dc")
    }

    fn create_source(&mut self, reader: Box<dyn io::Read>) -> CliResult<Box<dyn Source>> {
        Ok(Box::new(dc_source::DCSource::new(reader)?))
    }
}
