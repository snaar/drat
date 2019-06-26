use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use crate::chopper::chopper::{DataSink, HeaderSink};
use crate::chopper::header_graph::PinId;
use crate::chopper::types::{FieldType, FieldValue, Header, Row};
use crate::error::{CliResult, Error};
use crate::util::dc_util;

pub struct CSVSink {
    writer: BufWriter<Box<io::Write+'static>>,
}

impl CSVSink {
    pub fn new(path: &Option<String>) -> CliResult<Self> {
        let writer = BufWriter::new(CSVSink::into_writer(path)?);
        Ok(CSVSink { writer })
    }

    fn into_writer(path: &Option<String>) -> io::Result<Box<io::Write>> {
        match path {
            None => {
                Ok(Box::new(io::stdout()))
            }
            Some(p) => {
                let path = PathBuf::from(p);
                let file = File::create(path)?;
                Ok(Box::new(file))
            }
        }
    }

    fn write_csv_header(&mut self, header: &mut Header) -> CliResult<()> {
        Self::write_field_name(&mut self.writer, header)?;
        Self::write_field_type(&mut self.writer, header)?;
        Ok(())
    }

    fn write_field_name(writer: &mut BufWriter<Box<io::Write+'static>>, header: &mut Header) -> CliResult<()> {
        let field_name = header.field_names().clone();
        let mut first = true;
        for name in field_name {
            if first {
                write!(writer, "{}", name)?;
                first = false;
            } else {
                write!(writer, ",{}", name)?;
            }
        }
        write!(writer, "\n")?;
        Ok(())
    }

    fn write_field_type(writer: &mut BufWriter<Box<io::Write+'static>>, header: &mut Header) -> CliResult<()> {
        let field_types = header.field_types().clone();
        let field_string_map = &dc_util::FIELD_STRING_MAP_TYPE;
        let mut first = true;

        for field_type in field_types {
            let type_string = match field_type {
                FieldType::Boolean =>
                    return Err(Error::from("CSVSink -- boolean field type is not supported")),
                FieldType::Byte => field_string_map.get(&FieldType::Byte),
                FieldType::ByteBuf =>
                    return Err(Error::from("CSVSink -- ByteBuffer field type is not supported")),
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
                        write!(writer, "{}", t)?;
                        first = false;
                    } else {
                        write!(writer, ",{}", t)?;
                    }
                },
                None =>
                    return Err(Error::from("CSVSink -- field type missing"))
            }
        }
        write!(writer, "\n")?;
        Ok(())
    }
}

impl HeaderSink for CSVSink {
    fn process_header(mut self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>> {
        self.write_csv_header(header)?;
        Ok(self.boxed())
    }
}

impl DataSink for CSVSink {
    fn write_row(&mut self, row: Row) -> CliResult<Option<Row>> {
        write!(self.writer, "{}", row.timestamp)?;
        let field_values = &row.field_values;
        for value in field_values {
            match value {
                FieldValue::Boolean(_x) =>
                    return Err(Error::from("CSVSink -- boolean field type is not supported")),
                FieldValue::Byte(x) => write!(self.writer, ",{}", x)?,
                FieldValue::ByteBuf(_x) =>
                    return Err(Error::from("CSVSink -- ByteBuffer field type is not supported")),
                FieldValue::Char(x) => write!(self.writer, ",{}", x)?,
                FieldValue::Double(x) => {
                    write!(self.writer, ",")?;
                    dtoa::write(&mut self.writer, *x)?;
                },
                FieldValue::Float(x) => {
                    write!(self.writer, ",")?;
                    dtoa::write(&mut self.writer, *x)?;
                },
                FieldValue::Int(x) => write!(self.writer, ",{}", x)?,
                FieldValue::Long(x) => write!(self.writer, ",{}", x)?,
                FieldValue::Short(x) => write!(self.writer, ",{}", x)?,
                FieldValue::String(x) => write!(self.writer, ",{}", x)?,
                FieldValue::None => write!(self.writer, ",", )?,
            };
        }
        write!(self.writer, "\n")?;
        Ok(None)
    }

    fn write_row_to_pin(&mut self, _pin_id: PinId, row: Row) -> CliResult<Option<Row>> {
        self.write_row(row)
    }

    fn flush(&mut self) -> CliResult<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn boxed(self) -> Box<dyn DataSink> {
        Box::new(self)
    }
}
