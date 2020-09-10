use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::csv_configs::CSVInputConfig;
use crate::source::csv_source::CSVSource;
use crate::source::source_factory::SourceFactory;
use crate::util::preview::Preview;

pub struct CSVFactory {
    pub csv_input_config: CSVInputConfig,
}

impl CSVFactory {
    pub fn new(csv_input_config: CSVInputConfig) -> Self {
        CSVFactory { csv_input_config }
    }
}

impl SourceFactory for CSVFactory {
    fn can_create_from_format(&self, format: &String) -> bool {
        format.ends_with(".csv")
    }

    fn can_create_from_previewer(&self, _previewer: &Box<dyn Preview>) -> bool {
        return false;
    }

    fn create_source(&mut self, preview: Box<dyn Preview>) -> CliResult<Box<dyn Source>> {
        Ok(Box::new(CSVSource::new(preview, &self.csv_input_config)?))
    }
}
