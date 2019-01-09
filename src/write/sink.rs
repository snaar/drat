use crate::read::types::Row;

//TODO think about preventing calling write_header multiple times via type system
pub trait Sink {
    fn write_row(&mut self, row: &Row);
    fn write_header(&mut self, header: &Vec<String>);
}
