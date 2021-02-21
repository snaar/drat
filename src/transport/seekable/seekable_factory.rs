use std::io::BufReader;
use std::path::Path;

use crate::error::CliResult;
use crate::transport::seekable::seekable_transport::SeekableTransport;
use crate::transport::seekable::ReadSeek;

#[derive(Clone)]
pub struct SeekableTransportFactory {
    transports: Vec<Box<dyn SeekableTransport>>,
}

impl SeekableTransportFactory {
    pub fn new(transports: Vec<Box<dyn SeekableTransport>>) -> SeekableTransportFactory {
        SeekableTransportFactory { transports }
    }

    pub fn create_seekable(
        &mut self,
        path: &Path,
    ) -> CliResult<Option<BufReader<Box<dyn ReadSeek>>>> {
        let mut reader: Option<Box<dyn ReadSeek>> = None;
        for factory in &mut self.transports.iter() {
            match factory.can_open(path) {
                false => continue,
                true => reader = Some(factory.open(path)?),
            }
        }
        Ok(match reader {
            None => None,
            Some(reader) => Some(BufReader::new(reader)),
        })
    }
}
