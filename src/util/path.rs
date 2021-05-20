use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

pub fn get_file_name(path: &Path) -> Option<String> {
    if let Some(file_name) = path.file_name() {
        // that's right, unwrap to_str first to panic on os->str conversion if needed
        Some(file_name.to_str().unwrap().to_owned())
    } else {
        // weird but ok
        None
    }
}

pub fn buf_writer_from_file_path(path: &Option<String>) -> io::Result<Box<dyn io::Write>> {
    match path {
        None => Ok(Box::new(io::stdout())),
        Some(p) => {
            let path = PathBuf::from(p);
            let file = File::create(path)?;
            Ok(Box::new(file))
        }
    }
}
