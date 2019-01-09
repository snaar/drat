use std::fs::File;
use std::io::{self, Write, BufWriter};
use std::path::PathBuf;

use crate::read::types::{Row, FieldValue};
use crate::write::sink::Sink;

pub struct CSVSink {
    writer: BufWriter<Box<io::Write+'static>>,
}

impl CSVSink {
    pub fn new(path: &Option<&str>) -> Self {
        let writer = BufWriter::new(CSVSink::into_writer(path).unwrap());
        CSVSink{ writer }
    }

    fn into_writer(path: &Option<&str>) -> io::Result<Box<io::Write>> {
        match path {
            None => {
                Ok(Box::new(io::stdout()))
            }
            Some(p) => {
                let path = PathBuf::from(p);
                let file = File::create(path).unwrap();
                Ok(Box::new(file))
            }
        }
    }
}

impl Sink for CSVSink {
    fn write_row (&mut self, row: &Row) {
        write!(self.writer, "{}", row.timestamp).unwrap();
        let field_values = &row.field_values;
        for value in field_values {
            let v = match value {
                FieldValue::Double(x) => x.to_string(),
                FieldValue::Float(x) => x.to_string(), //TODO print floats with at least .0
                FieldValue::Int(x) => x.to_string(),
                FieldValue::Long(x) => x.to_string(),
                FieldValue::Short(x) => x.to_string(),
                FieldValue::String(x) => x.to_string(),
                FieldValue::None => "".to_string(),
            };
            write!(self.writer, ",{}", v).unwrap();
        }
        write!(self.writer, "\n").unwrap();
    }

    fn write_header(&mut self, header: &Vec<String>) {
        let mut first = true;
        for field in header {
            if first { first = false; }
            else { write!(self.writer, ",").unwrap(); } //TODO figure out how to print single char without buffer

            write!(self.writer, "{}", field).unwrap();
        }
        self.writer.write(b"\n").unwrap();
    }
}
