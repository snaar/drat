use std::io::Read;

pub trait Preview {
    fn get_lines(&self) -> &Option<Vec<String>>;
    fn get_reader(self: Box<Self>) -> Box<dyn Read>;
}
