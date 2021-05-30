use std::path::{Path, PathBuf};

use crate::chopper::error::ChopperResult;
use crate::transport::dir::dir_transport::DirTransport;

#[derive(Clone)]
pub struct DirFileTransport;

impl DirTransport for DirFileTransport {
    fn can_handle(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn read_dir(&self, path: &Path) -> ChopperResult<Box<dyn Iterator<Item = PathBuf>>> {
        match path.read_dir() {
            Ok(dir_entry_iter) => {
                let path_iter = dir_entry_iter.filter_map(|entry| Some(entry.ok()?.path()));
                Ok(Box::new(path_iter))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn box_clone(&self) -> Box<dyn DirTransport> {
        Box::new((*self).clone())
    }

    fn name(&self) -> &str {
        "file[dir]"
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use crate::transport::dir::dir_transport::DirTransport;
    use crate::transport::dir::file::DirFileTransport;

    #[test]
    fn test_read_dir() {
        let transport = DirFileTransport {};
        assert!(transport.is_dir(Path::new("./tests/input/multi_file")));

        let contents = transport
            .read_dir(Path::new("./tests/input/multi_file"))
            .unwrap();
        let mut contents: Vec<PathBuf> = contents.collect();
        contents.sort();

        assert_eq!(contents.len(), 4);
        assert_eq!(contents[0].file_name().unwrap(), "1.csv");
        assert_eq!(contents[1].file_name().unwrap(), "2.csv");
        assert_eq!(contents[2].file_name().unwrap(), "3.csv");
        assert_eq!(contents[3].file_name().unwrap(), "inner");
    }
}
