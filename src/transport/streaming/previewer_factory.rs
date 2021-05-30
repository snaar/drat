use std::io::Read;
use std::path::Path;

use crate::chopper::error::ChopperResult;
use crate::transport::streaming::streaming_transport::StreamingTransport;
use crate::util::reader::ChopperBufPreviewer;

#[derive(Clone)]
pub struct PreviewerTransportFactory {
    transports: Vec<Box<dyn StreamingTransport>>,
}

impl PreviewerTransportFactory {
    pub fn new(transports: Vec<Box<dyn StreamingTransport>>) -> PreviewerTransportFactory {
        PreviewerTransportFactory { transports }
    }

    pub fn create_previewer(
        &mut self,
        path: &Path,
    ) -> ChopperResult<Option<ChopperBufPreviewer<Box<dyn Read>>>> {
        let mut reader: Option<Box<dyn Read>> = None;
        for factory in &mut self.transports.iter() {
            match factory.can_open(path) {
                false => continue,
                true => reader = Some(factory.open(path)?),
            }
        }
        Ok(match reader {
            None => None,
            Some(reader) => Some(ChopperBufPreviewer::new(reader)?),
        })
    }
}
