use std::fs;
use std::io;
use std::path::Path;

use crate::transport::seekable::seekable_transport::SeekableTransport;
use crate::transport::seekable::ReadSeek;

#[derive(Clone)]
pub struct SeekableFileTransport;

impl SeekableTransport for SeekableFileTransport {
    fn can_open(&self, path: &Path) -> bool {
        path.exists()
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn ReadSeek>> {
        match fs::File::open(path) {
            Ok(r) => Ok(Box::new(r)),
            Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err)),
        }
    }

    fn box_clone(&self) -> Box<dyn SeekableTransport> {
        Box::new((*self).clone())
    }

    fn name(&self) -> &str {
        "file[seekable]"
    }
}
