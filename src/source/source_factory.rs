use std::io::Read;

use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::util::reader::ChopperBufPreviewer;

pub trait SourceFactory {
    fn can_create_from_format(&self, format: &String) -> bool;
    fn can_create_from_previewer(&self, previewer: &ChopperBufPreviewer<Box<dyn Read>>) -> bool;
    fn create_source(
        &mut self,
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
    ) -> CliResult<Box<dyn Source>>;

    fn box_clone(&self) -> Box<dyn SourceFactory>;
}

impl Clone for Box<dyn SourceFactory> {
    fn clone(&self) -> Box<dyn SourceFactory> {
        self.box_clone()
    }
}
