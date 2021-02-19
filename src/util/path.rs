use std::path::Path;

pub fn get_file_name(path: &Path) -> Option<String> {
    if let Some(file_name) = path.file_name() {
        // that's right, unwrap to_str first to panic on os->str conversion if needed
        Some(file_name.to_str().unwrap().to_owned())
    } else {
        // weird but ok
        None
    }
}
