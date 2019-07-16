use std::io;
use std::path::Path;

use crate::chopper::chopper::Source;
use crate::error::CliResult;

pub trait SourceFactory {
    fn can_create_from(&self, path: &Path) -> bool;
    fn create_source(&mut self, reader: Box<dyn io::Read>) -> CliResult<Box<dyn Source>>;
}
