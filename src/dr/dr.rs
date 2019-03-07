use std::fmt;

use crate::dr::types::{FieldType, Row};
use crate::result::CliResult;

pub trait DRDriver {
    fn drive(&mut self);
}

pub trait Source {
    fn header(&self) -> &Header;
    fn next_row(&mut self) -> Option<Row>;
}

//TODO better debug format?
impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "source field names: {:?}", self.header().field_names)
    }
}

pub trait HeaderSink {
    fn write_header(self: Box<Self>, header: &Header) -> Box<dyn DataSink>;
}

pub trait DataSink {
    fn write_row(&mut self, row: Row) -> CliResult<()>;
    fn flush(&mut self) -> CliResult<()>;
    fn boxed(self) -> Box<dyn DataSink>;
}

#[derive(Clone)]
pub struct Header {
    field_names: Vec<String>,
    field_types: Vec<FieldType>,
}

impl Header {
    pub fn new(field_names: Vec<String>, field_types: Vec<FieldType>) -> Self {
        Header { field_names, field_types }
    }

    pub fn get_field_names(&self) -> &Vec<String> {
        &self.field_names
    }

    pub fn get_field_types(&self) -> &Vec<FieldType> {
        &self.field_types
    }

    pub fn get_field_names_mut(&mut self) -> &mut Vec<String> {
        &mut self.field_names
    }

    pub fn get_field_types_mut(&mut self) -> &mut Vec<FieldType> {
        &mut self.field_types
    }
}