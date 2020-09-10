use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::util::preview::Preview;

pub trait SourceFactory {
    fn can_create_from_format(&self, format: &String) -> bool;
    fn can_create_from_previewer(&self, previewer: &Box<dyn Preview>) -> bool;
    fn create_source(&mut self, previewer: Box<dyn Preview>) -> CliResult<Box<dyn Source>>;
}
