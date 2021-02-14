use crate::error::CliResult;
use std::path::{Path, PathBuf};

pub trait DirRead {
    fn is_dir(&self, path: &Path) -> bool;
    fn read_dir(&self, path: &Path) -> CliResult<Box<dyn Iterator<Item = PathBuf>>>;
}
