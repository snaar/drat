use std::fmt;

use crate::chopper::types::{Header, Row};
use crate::error::CliResult;

pub trait Source {
    fn header(&self) -> &Header;
    fn next_row(&mut self) -> CliResult<Option<Row>>;
}

impl<S: Source + ?Sized> Source for Box<S> {
    #[inline]
    fn header(&self) -> &Header {
        S::header(self)
    }

    #[inline]
    fn next_row(&mut self) -> CliResult<Option<Row>> {
        S::next_row(self)
    }
}

//TODO better debug format?
impl fmt::Debug for dyn Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "source field names: {:?}", self.header().field_names())
    }
}
