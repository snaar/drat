use std::fmt;
use std::io;
use std::path::PathBuf;

pub trait TransportFactory {
    fn can_open(&self, path: &PathBuf) -> bool;
    fn open(&self, path: &PathBuf) -> io::Result<Box<io::Read+'static>>;
    fn box_clone(&self) -> Box<TransportFactory>;
    fn factory_name(&self) -> &str;
}

impl Clone for Box<TransportFactory> {
    fn clone(&self) -> Box<TransportFactory> {
        self.box_clone()
    }
}

impl fmt::Debug for TransportFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.factory_name())
    }
}
