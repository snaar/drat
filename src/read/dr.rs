use read::types::Row;

pub trait Reader {
    fn header(&self) -> &Vec<String>;
    fn next_row(&mut self) -> Option<Row>;
}
