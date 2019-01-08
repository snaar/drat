use std::fmt;
use std::io;
use std::path::PathBuf;

//pub enum InputResult {
//    WrongFactory,
//    CannotOpen,
//    Empty,
//    Ok(Box<io::Read+'static>),
//}

pub trait InputFactory {
    fn can_open(&self, path: &PathBuf) -> bool;
    fn open(&self, path: &PathBuf) -> io::Result<Box<io::Read+'static>>;
    fn box_clone(&self) -> Box<InputFactory>;
    fn factory_name(&self) -> &str;
}

impl Clone for Box<InputFactory> {
    fn clone(&self) -> Box<InputFactory> {
        self.box_clone()
    }
}

impl fmt::Debug for InputFactory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.factory_name())
    }
}
