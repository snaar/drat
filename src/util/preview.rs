use std::io::Read;

pub trait Preview {
    /// 'None' means that there was an error parsing buffer as vector of valid strings
    fn get_lines(&self) -> &Option<Vec<String>>;

    /// buf will be trimmed down to number of bytes that was actually read,
    /// if it's less than the default buf capacity
    fn get_buf(&self) -> &Box<[u8]>;

    fn get_reader(self: Box<Self>) -> Box<dyn Read>;
}
