use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::error::CliResult;
use crate::transport::dir_read::DirRead;
use crate::transport::transport_factory::TransportFactory;

#[derive(Clone)]
pub struct FileInput;

impl TransportFactory for FileInput {
    fn can_open(&self, path: &Path) -> bool {
        path.exists()
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>> {
        match fs::File::open(path) {
            Ok(r) => Ok(Box::new(r)),
            Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err)),
        }
    }

    fn get_dir_reader(&self) -> Option<&dyn DirRead> {
        Some(self)
    }

    fn box_clone(&self) -> Box<dyn TransportFactory> {
        Box::new((*self).clone())
    }

    fn factory_name(&self) -> &str {
        "file"
    }
}

impl DirRead for FileInput {
    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn read_dir(&self, path: &Path) -> CliResult<Box<dyn Iterator<Item = PathBuf>>> {
        match path.read_dir() {
            Ok(dir_entry_iter) => {
                let path_iter = dir_entry_iter.filter_map(|entry| Some(entry.ok()?.path()));
                Ok(Box::new(path_iter))
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use crate::transport::file::FileInput;
    use crate::transport::transport_factory::TransportFactory;

    #[test]
    fn test_read_dir() {
        let dr = FileInput {}.get_dir_reader().unwrap();
        assert!(dr.is_dir(Path::new("./tests/input/multifile")));

        let contents = dr.read_dir(Path::new("./tests/input/multifile")).unwrap();
        let mut contents: Vec<PathBuf> = contents.collect();
        contents.sort();

        assert_eq!(contents.len(), 3);
        assert_eq!(contents[0].file_name().unwrap(), "1.csv");
        assert_eq!(contents[1].file_name().unwrap(), "2.csv");
        assert_eq!(contents[2].file_name().unwrap(), "3.csv");
    }
}
