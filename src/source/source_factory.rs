use std::io;
use std::path::PathBuf;

use crate::chopper::chopper::Source;
use crate::error::CliResult;

pub trait SourceFactory {
    fn can_create_from(&self, path: &PathBuf) -> bool;
    fn create_source(&mut self, reader: Box<io::Read>) -> CliResult<Box<Source+'static>>;
}
