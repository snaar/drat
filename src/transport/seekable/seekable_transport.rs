use std::path::Path;
use std::{fmt, io};

use crate::transport::seekable::ReadSeek;

pub trait SeekableTransport {
    fn can_open(&self, path: &Path) -> bool;
    /// returned reader should do minimal buffering, caller should do buffering if needed
    fn open(&self, path: &Path) -> io::Result<Box<dyn ReadSeek>>;

    fn box_clone(&self) -> Box<dyn SeekableTransport>;
    fn name(&self) -> &str;
}

impl Clone for Box<dyn SeekableTransport> {
    fn clone(&self) -> Box<dyn SeekableTransport> {
        self.box_clone()
    }
}

impl fmt::Debug for dyn SeekableTransport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.name())
    }
}
