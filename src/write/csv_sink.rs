use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process;

use crate::dr::dr::{Header, HeaderSink, DataSink};
use crate::dr::types::{FieldType, FieldValue, Row};
use crate::result::{CliError, CliResult};
use crate::util::dc_util;

pub struct CSVSink {
    writer: BufWriter<Box<io::Write+'static>>,
}

impl CSVSink {
    pub fn new(path: &Option<String>) -> Self {
        let writer = BufWriter::new(CSVSink::into_writer(path).unwrap());
        CSVSink { writer }
    }

    fn into_writer(path: &Option<String>) -> io::Result<Box<io::Write>> {
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

    fn write_csv_header(&mut self, mut header: Header) {
        Self::write_field_name(&mut self.writer, &mut header);
        Self::write_field_type(&mut self.writer, &mut header);
    }

    fn write_field_name(writer: &mut BufWriter<Box<io::Write+'static>>, header: &mut Header) {
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

    fn write_field_type(writer: &mut BufWriter<Box<io::Write+'static>>, header: &mut Header) {
        let field_types = header.get_field_types().clone();
        let field_string_map = &dc_util::FIELD_STRING_MAP_TYPE;
        let mut first = true;

        for field_type in field_types {
            let type_string = match field_type {
                FieldType::Boolean => {
                    write_error!("Error: boolean field type is not supported");
                    process::exit(1);
                },
                FieldType::Byte => field_string_map.get(&FieldType::Byte),
                FieldType::ByteBuf => {
                    write_error!("Error: ByteBuffer field type is not supported");
                    process::exit(1);
                },
                FieldType::Char => field_string_map.get(&FieldType::Char),
                FieldType::Double => field_string_map.get(&FieldType::Double),
                FieldType::Float => field_string_map.get(&FieldType::Float),
                FieldType::Int => field_string_map.get(&FieldType::Int),
                FieldType::Long => field_string_map.get(&FieldType::Long),
                FieldType::Short => field_string_map.get(&FieldType::Short),
                FieldType::String => field_string_map.get(&FieldType::String),
            };
            match type_string {
                Some(t) => {
                    if first {
                        write!(writer, "{}", t).unwrap();
                        first = false;
                    } else {
                        write!(writer, ",{}", t).unwrap();
                    }
                },
                None => {
                    write_error!("Error: field type missing");
                    process::exit(1)
                }
            }
        }
        write!(writer, "\n").unwrap();
    }
}

impl HeaderSink for CSVSink {
    fn write_header(mut self: Box<Self>, header: &Header) -> Box<dyn DataSink> {
        let header = header.clone();
        self.write_csv_header(header);
        self.boxed()
    }
}

impl DataSink for CSVSink {
    fn write_row (&mut self, row: Row) -> CliResult<()> {
        write!(self.writer, "{}", row.timestamp).unwrap();
        let field_values = &row.field_values;
        for value in field_values {
            match value {
                FieldValue::Boolean(_x) => {
                    return Err(CliError::Data("Error: boolean field type is not supported for writing CSV file".to_string()));
                },
                FieldValue::Byte(x) => write!(self.writer, ",{}", x).unwrap(),
                FieldValue::ByteBuf(_x) => {
                    return Err(CliError::Data("Error: ByteBuffer field type is not supported for writing CSV file".to_string()));
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
        Ok(())
    }

    fn flush(&mut self) -> CliResult<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn boxed(self) -> Box<DataSink> {
        Box::new(self)
    }
}
