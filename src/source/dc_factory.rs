use std::io;

use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::{dc_source::DCSource, source_factory::SourceFactory};
use std::io::Read;

pub struct DCFactory;

impl SourceFactory for DCFactory {
    fn can_create_from_format(&self, format: &String) -> bool {
        format.ends_with(".dc")
    }

    fn can_create_from_previewer(&self, _previewer: &Box<dyn Read>) -> bool {
        return false;
    }

    fn create_source(&mut self, reader: Box<dyn io::Read>) -> CliResult<Box<dyn Source>> {
        Ok(Box::new(DCSource::new(reader)?))
    }
}
