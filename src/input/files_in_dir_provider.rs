use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

use crate::error::CliResult;
use crate::error::Error::Io;
use crate::input::serial_multi_file_provider::SerialMultiFilePathProvider;
use crate::transport::dir::dir_transport::DirTransport;

pub struct FilesInDirInputReaderProviderFactory;

pub struct FilesInDirPathProvider {
    files_in_reverse: Vec<PathBuf>,
}

impl FilesInDirPathProvider {
    pub fn new(
        dir_transport: &Box<dyn DirTransport>,
        path: &Path,
    ) -> CliResult<FilesInDirPathProvider> {
        if !dir_transport.is_dir(path) {
            return Err(Io(Error::new(
                ErrorKind::InvalidData,
                format!("directory expected: {:?}", path),
            )));
        };

        let mut files: Vec<PathBuf> = dir_transport
            .read_dir(path)?
            .filter(|e| !dir_transport.is_dir(e))
            .collect();

        files.sort_by(|a, b| b.cmp(a));

        Ok(FilesInDirPathProvider {
            files_in_reverse: files,
        })
    }
}

impl SerialMultiFilePathProvider for FilesInDirPathProvider {
    fn get_next_path(&mut self) -> Option<PathBuf> {
        self.files_in_reverse.pop()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::input::files_in_dir_provider::FilesInDirPathProvider;
    use crate::input::serial_multi_file_provider::SerialMultiFilePathProvider;
    use crate::transport::dir::dir_transport::DirTransport;
    use crate::transport::dir::file::DirFileTransport;

    #[test]
    fn test() {
        let transport: Box<dyn DirTransport> = Box::new(DirFileTransport {});
        let path = Path::new("./tests/input/multi_file");
        let mut provider = FilesInDirPathProvider::new(&transport, path).unwrap();

        let next = provider.get_next_path();
        assert!(next.is_some());
        assert_eq!(next.unwrap(), path.join("1.csv"));

        let next = provider.get_next_path();
        assert!(next.is_some());
        assert_eq!(next.unwrap(), path.join("2.csv"));

        let next = provider.get_next_path();
        assert!(next.is_some());
        assert_eq!(next.unwrap(), path.join("3.csv"));

        let next = provider.get_next_path();
        assert!(next.is_none());
    }
}
