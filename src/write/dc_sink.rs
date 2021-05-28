use std::io::Write;

use byteorder::{BigEndian, WriteBytesExt};

use crate::chopper::sink::{DataSink, DynHeaderSink};
use crate::chopper::types::{FieldType, FieldValue, Header, Row};
use crate::error::{CliResult, Error};
use crate::util::dc_util;

pub struct DCSink<W: 'static + Write> {
    writer: W,
    bitset_bytes: usize,
}

impl<W: 'static + Write> DCSink<W> {
    pub fn new(writer: W) -> CliResult<Self> {
        Ok(DCSink {
            writer,
            bitset_bytes: 0,
        })
    }

    fn write_header(mut dc_sink: &mut DCSink<W>, header: &mut Header) -> CliResult<()> {
        DCSink::write_magic(&mut dc_sink)?;
        DCSink::write_version(&mut dc_sink)?;
        DCSink::write_empty_user_data(&mut dc_sink)?;
        DCSink::write_field_descriptors(&mut dc_sink, header)?;
        Ok(())
    }

    fn write_magic(dc_sink: &mut DCSink<W>) -> CliResult<()> {
        dc_sink.writer.write_u64::<BigEndian>(dc_util::MAGIC_NUM)?;
        Ok(())
    }

    fn write_version(dc_sink: &mut DCSink<W>) -> CliResult<()> {
        dc_sink.writer.write_u16::<BigEndian>(dc_util::VERSION)?;
        Ok(())
    }

    fn write_empty_user_data(dc_sink: &mut DCSink<W>) -> CliResult<()> {
        dc_sink.writer.write_u32::<BigEndian>(0)?;
        Ok(())
    }

    fn write_field_descriptors(dc_sink: &mut DCSink<W>, header: &mut Header) -> CliResult<()> {
        let field_types = header.field_types();
        let field_names = header.field_names();
        let field_count = field_types.len();

        // write field count
        dc_sink.writer.write_u32::<BigEndian>(field_count as u32)?;

        // write field names and types as SizedStrings
        if field_names.len() != field_types.len() {
            return Err(Error::from(
                "DCSink -- number of field name and number of field types does not match",
            ));
        }
        for i in 0..field_types.len() {
            dc_util::write_sized_string(&mut dc_sink.writer, &field_names[i])?;
            DCSink::write_field_type(dc_sink, &field_types[i])?;
            DCSink::write_display_hint(dc_sink, dc_util::DisplayHint::None)?;
        }
        Ok(())
    }

    fn write_field_type(dc_sink: &mut DCSink<W>, field_type: &FieldType) -> CliResult<()> {
        let field_string_map = &dc_util::FIELD_STRING_MAP_TYPE;
        let type_string = match field_type {
            FieldType::Boolean => {
                return Err(Error::from("DCSink -- boolean field type is not supported"))
            }
            FieldType::Byte => field_string_map.get(&FieldType::Byte),
            FieldType::ByteBuf => {
                return Err(Error::from(
                    "DCSink -- ByteBuffer field type is not supported",
                ))
            }
            FieldType::Char => field_string_map.get(&FieldType::Char),
            FieldType::Double => field_string_map.get(&FieldType::Double),
            FieldType::Float => field_string_map.get(&FieldType::Float),
            FieldType::Int => field_string_map.get(&FieldType::Int),
            FieldType::Long => field_string_map.get(&FieldType::Long),
            FieldType::Short => field_string_map.get(&FieldType::Short),
            FieldType::String => field_string_map.get(&FieldType::String),
        };
        match type_string {
            Some(t) => dc_util::write_sized_string(&mut dc_sink.writer, t)?,
            None => return Err(Error::from("DCSink -- field type missing")),
        }
        Ok(())
    }

    fn write_display_hint(
        dc_sink: &mut DCSink<W>,
        display_hint: dc_util::DisplayHint,
    ) -> CliResult<()> {
        let hint: i32 = match display_hint {
            dc_util::DisplayHint::Timestamp => 0,
            dc_util::DisplayHint::ArrayInt => 1,
            dc_util::DisplayHint::ArrayDouble => 2,
            dc_util::DisplayHint::ArrayLong => 3,
            dc_util::DisplayHint::ArrayString => 4,
            dc_util::DisplayHint::ArrayByte => 5,
            dc_util::DisplayHint::MatrixDouble2D => 6,
            dc_util::DisplayHint::None => -1,
        };
        dc_sink.writer.write_i32::<BigEndian>(hint)?;
        Ok(())
    }
}

impl<W: 'static + Write> DynHeaderSink for DCSink<W> {
    fn process_header(mut self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>> {
        Self::write_header(&mut self, header)?;
        let bitset_bytes = dc_util::get_bitset_bytes(header.field_types().len() - 1);
        self.bitset_bytes = bitset_bytes;
        Ok(Box::new(*self))
    }
}

impl<W: 'static + Write> DataSink for DCSink<W> {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> CliResult<()> {
        let row = io_rows.get(0).unwrap();

        // write timestamp
        self.writer.write_u64::<BigEndian>(row.timestamp)?;

        // write bitset
        let field_values = &row.field_values;
        let mut bitset_bytes: Vec<u8> = Vec::with_capacity(self.bitset_bytes);
        let mut field_count = 0;

        for _i in 0..self.bitset_bytes {
            let mut current_bitset: u8 = 0;
            let mut current_bit = 0;
            for _j in 0..8 {
                if field_count >= field_values.len() {
                    break;
                }
                match &field_values.get(field_count).unwrap() {
                    FieldValue::None => current_bitset += 2_u8.pow(current_bit),
                    _ => current_bitset = current_bitset,
                }
                current_bit += 1;
                field_count += 1;
            }
            bitset_bytes.push(current_bitset);
        }
        self.writer.write_all(&bitset_bytes)?;

        // write row values
        for value in field_values {
            match value {
                FieldValue::Boolean(_x) => {
                    return Err(Error::from("DCSink -- boolean field type is not supported"))
                }
                FieldValue::Byte(x) => self.writer.write_u8(*x)?,
                FieldValue::ByteBuf(_x) => {
                    return Err(Error::from(
                        "DCSink -- ByteBuffer field type is not supported",
                    ))
                }
                FieldValue::Char(x) => self.writer.write_u16::<BigEndian>(*x)?,
                FieldValue::Double(x) => self.writer.write_f64::<BigEndian>(*x)?,
                FieldValue::Float(x) => self.writer.write_f32::<BigEndian>(*x)?,
                FieldValue::Int(x) => self.writer.write_i32::<BigEndian>(*x)?,
                FieldValue::Long(x) => self.writer.write_i64::<BigEndian>(*x)?,
                FieldValue::Short(x) => self.writer.write_i16::<BigEndian>(*x)?,
                FieldValue::String(x) => dc_util::write_string_value(&mut self.writer, &x)?,
                FieldValue::None => continue,
            };
        }
        Ok(())
    }

    fn flush(&mut self) -> CliResult<()> {
        self.writer.flush()?;
        Ok(())
    }
}
