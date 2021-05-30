use std::fmt;
use std::path::{Path, PathBuf};

use crate::chopper::error::ChopperResult;

pub trait DirTransport {
    fn can_handle(&self, path: &Path) -> bool;
    fn is_dir(&self, path: &Path) -> bool;
    fn read_dir(&self, path: &Path) -> ChopperResult<Box<dyn Iterator<Item = PathBuf>>>;

    fn box_clone(&self) -> Box<dyn DirTransport>;
    fn name(&self) -> &str;
}

impl Clone for Box<dyn DirTransport> {
    fn clone(&self) -> Box<dyn DirTransport> {
        self.box_clone()
    }
}

impl fmt::Debug for dyn DirTransport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.name())
    }
}
