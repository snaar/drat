use std::fs;
use std::io;
use std::path::Path;

use crate::transport::transport_factory::TransportFactory;

#[derive(Clone)]
pub struct FileInput;

impl TransportFactory for FileInput {
    fn can_open(&self, path: &Path) -> bool {
        path.exists()
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>> {
        match fs::File::open(path) {
            Ok(r) => {
                Ok(Box::new(r))
            },
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err))
            }
        }
    }

    fn box_clone(&self) -> Box<dyn TransportFactory> {
        Box::new((*self).clone())
    }

    fn factory_name(&self) -> &str {
        "file"
    }
}
