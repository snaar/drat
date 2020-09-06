use std::io;

use crate::chopper::chopper::Source;
use crate::error::CliResult;

pub trait SourceFactory {
    fn can_create_from_format(&self, format: &String) -> bool;
    fn can_create_from_previewer(&self, previewer: &Box<dyn io::Read>) -> bool;
    fn create_source(&mut self, reader: Box<dyn io::Read>) -> CliResult<Box<dyn Source>>;
}
