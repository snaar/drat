use std::path::PathBuf;

pub trait SerialMultiFilePathProvider {
    fn get_next_path(&mut self) -> Option<PathBuf>;
}
