use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::{dc_source::DCSource, source_factory::SourceFactory};
use crate::util::preview::Preview;

pub struct DCFactory;

impl SourceFactory for DCFactory {
    fn can_create_from_format(&self, format: &String) -> bool {
        format.ends_with(".dc")
    }

    fn can_create_from_previewer(&self, _previewer: &Box<dyn Preview>) -> bool {
        return false;
    }

    fn create_source(&mut self, previewer: Box<dyn Preview>) -> CliResult<Box<dyn Source>> {
        let reader = previewer.get_reader();
        Ok(Box::new(DCSource::new(reader)?))
    }
}
