use std::fmt;
use std::io;
use std::path::Path;

pub trait TransportFactory {
    fn can_open(&self, path: &Path) -> bool;
    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>>;
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
