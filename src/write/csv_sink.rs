use std::fs::File;
use std::io::{self, Write, BufWriter};
use std::path::PathBuf;
use std::process;

use crate::dr::dr;
use crate::dr::types::{FieldValue, Row};

pub struct CSVSink {
    writer: BufWriter<Box<io::Write+'static>>,
}

impl CSVSink {
    pub fn new(path: &Option<&str>, header: &dr::Header, has_header: bool) -> Self {
        let mut writer = BufWriter::new(CSVSink::into_writer(path).unwrap());

        if has_header {
            CSVSink::write_header(&mut writer, header)
        }
        CSVSink { writer }

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

    //TODO names/types?
    fn write_header(writer: &mut BufWriter<Box<io::Write+'static>>, header: &dr::Header) {
        let field_name = header.get_field_names().clone();
        let mut first = true;
        for name in field_name {
            if first {
                write!(writer, "{}", name).unwrap();
                first = false;
            } else {
                write!(writer, ",{}", name).unwrap();
            }
        }
        write!(writer, "\n").unwrap();
    }
}

impl dr::Sink for CSVSink {
    fn write_row (&mut self, row: &Row) {
        write!(self.writer, "{}", row.timestamp).unwrap();
        let field_values = &row.field_values;
        for value in field_values {
            match value {
                FieldValue::Boolean(_x) => {
                    println!();
                    println!("Error: boolean field type is not supported for writing CSV file");
                    process::exit(1);
                },
                FieldValue::Byte(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::ByteBuf(_x) => {
                    println!();
                    println!("Error: ByteBuffer field type is not supported for writing CSV file");
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

    fn boxed(&self) -> Box<&dr::Sink> {
        Box::new(self)
    }
}
