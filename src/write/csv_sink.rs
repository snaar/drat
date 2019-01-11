use std::fs::File;
use std::io::{self, Write, BufWriter};
use std::path::PathBuf;
use std::process;

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
            match value {
                FieldValue::Boolean(x) => {
                    println!("ERROR: boolean field type is not supported for writing CSV file");
                    process::exit(1);
                },
                FieldValue::Byte(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::ByteBuf(x) => {
                    println!("ERROR: ByteBuffer field type is not supported for writing CSV file");
                    process::exit(1);
                },
                FieldValue::Char(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::Double(x) => {
                    write!(self.writer, ",").unwrap();
                    dtoa::write(&mut self.writer, *x).unwrap();
                },
                FieldValue::Float(x) => {
                    write!(self.writer, ",").unwrap();
                    dtoa::write(&mut self.writer, *x).unwrap();
                },
                FieldValue::Int(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::Long(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::Short(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::String(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::None => write!(self.writer, ",", ).unwrap(),
            };
        }
        write!(self.writer, "\n").unwrap();
    }

    fn write_header(&mut self, header: &Vec<String>) {
        let mut first = true;
        for field in header {
            if first {
                write!(self.writer, "{}", field).unwrap();
                first = false;
            } else {
                write!(self.writer, ",{}", field).unwrap();
            }
        }
        self.writer.write(b"\n").unwrap();
    }
}
