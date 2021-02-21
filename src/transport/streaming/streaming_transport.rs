use std::fmt;
use std::io;
use std::path::Path;

pub trait StreamingTransport {
    fn can_open(&self, path: &Path) -> bool;
    /// returned reader should do minimal buffering, caller should do buffering if needed
    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>>;

    fn box_clone(&self) -> Box<dyn StreamingTransport>;
    fn name(&self) -> &str;
}

impl Clone for Box<dyn StreamingTransport> {
    fn clone(&self) -> Box<dyn StreamingTransport> {
        self.box_clone()
    }
}

impl fmt::Debug for dyn StreamingTransport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.name())
    }
}
