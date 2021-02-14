use std::fmt;
use std::io;
use std::path::Path;

use crate::transport::dir_read::DirRead;

pub trait TransportFactory {
    fn can_open(&self, path: &Path) -> bool;
    /// returned reader should do minimal buffering, caller should do buffering if needed
    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>>;

    fn get_dir_reader(&self) -> Option<&dyn DirRead>;

    fn box_clone(&self) -> Box<dyn TransportFactory>;
    fn factory_name(&self) -> &str;
}

impl Clone for Box<dyn TransportFactory> {
    fn clone(&self) -> Box<dyn TransportFactory> {
        self.box_clone()
    }
}

impl fmt::Debug for dyn TransportFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.factory_name())
    }
}
