use std::fs;
use std::io;
use std::path::Path;

use crate::transport::streaming::streaming_transport::StreamingTransport;

#[derive(Clone)]
pub struct FileTransport;

impl StreamingTransport for FileTransport {
    fn can_open(&self, path: &Path) -> bool {
        path.exists()
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>> {
        match fs::File::open(path) {
            Ok(r) => Ok(Box::new(r)),
            Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err)),
        }
    }

    fn box_clone(&self) -> Box<dyn StreamingTransport> {
        Box::new((*self).clone())
    }

    fn name(&self) -> &str {
        "file"
    }
}
