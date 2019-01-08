use std::fs;
use std::io;
use std::path::PathBuf;

use input::input_factory::InputFactory;

#[derive(Clone)]
pub struct FileInput;

impl InputFactory for FileInput {
    fn can_open(&self, path: &PathBuf) -> bool {
        path.exists()
    }

    fn open(&self, path: &PathBuf) -> io::Result<Box<io::Read+'static>> {
        match fs::File::open(path) {
            Ok(r) => {
                Ok(Box::new(r))
            },
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err))
            }
        }
    }

    fn box_clone(&self) -> Box<InputFactory> {
        Box::new((*self).clone())
    }

    fn factory_name(&self) -> &str {
        "File input"
    }
}
