use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use byteorder::{BigEndian, WriteBytesExt};

use crate::chopper::error::{ChopperResult, Error};
use crate::chopper::sink::{DataSink, DynHeaderSink, TypedHeaderSink};
use crate::chopper::types::{FieldType, FieldValue, Header, Row};
use crate::util::dc_util;

pub struct DCSink<W: 'static + Write> {
    writer: W,
    bitset_bytes: usize,
    field_type_map: Rc<HashMap<FieldType, String>>,
}

impl<W: 'static + Write> DCSink<W> {
    pub fn new(writer: W, field_type_map: Rc<HashMap<FieldType, String>>) -> ChopperResult<Self> {
        Ok(DCSink {
            writer,
            bitset_bytes: 0,
            field_type_map,
        })
    }

    fn write_header(&mut self, header: &mut Header) -> ChopperResult<()> {
        self.write_magic()?;
        self.write_version()?;
        self.write_empty_user_data()?;
        self.write_field_descriptors(header)?;
        Ok(())
    }

    fn write_magic(&mut self) -> ChopperResult<()> {
        self.writer.write_u64::<BigEndian>(dc_util::MAGIC_NUM)?;
        Ok(())
    }

    fn write_version(&mut self) -> ChopperResult<()> {
        self.writer.write_u16::<BigEndian>(dc_util::VERSION)?;
        Ok(())
    }

    fn write_empty_user_data(&mut self) -> ChopperResult<()> {
        self.writer.write_u32::<BigEndian>(0)?;
        Ok(())
    }

    fn write_field_descriptors(&mut self, header: &mut Header) -> ChopperResult<()> {
        let field_types = header.field_types();
        let field_names = header.field_names();
        let field_count = field_types.len();

        // write field count
        self.writer.write_u32::<BigEndian>(field_count as u32)?;

        // write field names and types as SizedStrings
        if field_names.len() != field_types.len() {
            return Err(Error::from(
                "DCSink -- number of field name and number of field types does not match",
            ));
        }
        for i in 0..field_types.len() {
            dc_util::write_u32_sized_string(&mut self.writer, &field_names[i])?;
            self.write_field_type(&field_types[i])?;
            self.write_display_hint(dc_util::DisplayHint::None)?;
        }
        Ok(())
    }

    fn write_field_type(&mut self, field_type: &FieldType) -> ChopperResult<()> {
        let type_string = self.field_type_map.get(field_type);
        match type_string {
            Some(t) => dc_util::write_u32_sized_string(&mut self.writer, t)?,
            None => return Err(Error::from("DCSink -- field type missing")),
        }
        Ok(())
    }

    fn write_display_hint(&mut self, display_hint: dc_util::DisplayHint) -> ChopperResult<()> {
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
        self.writer.write_i32::<BigEndian>(hint)?;
        Ok(())
    }
}

impl<W: 'static + Write> TypedHeaderSink<Self> for DCSink<W> {
    fn process_header(mut self, header: &mut Header) -> ChopperResult<Self> {
        self.write_header(header)?;
        let bitset_bytes = dc_util::get_bitset_bytes(header.field_types().len() - 1);
        self.bitset_bytes = bitset_bytes;
        Ok(self)
    }
}

impl<W: 'static + Write> DynHeaderSink for DCSink<W> {
    fn process_header(self: Box<Self>, header: &mut Header) -> ChopperResult<Box<dyn DataSink>> {
        Ok(Box::new(TypedHeaderSink::process_header(*self, header)?))
    }
}

impl<W: 'static + Write> DataSink for DCSink<W> {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> ChopperResult<()> {
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
                FieldValue::Boolean(x) => self.writer.write_u8(if *x { 1 } else { 0 })?,
                FieldValue::Byte(x) => self.writer.write_u8(*x)?,
                FieldValue::ByteBuf(x) => dc_util::write_sized_byte_buf(&mut self.writer, x)?,
                FieldValue::Char(x) => self.writer.write_u16::<BigEndian>(*x)?,
                FieldValue::Double(x) => self.writer.write_f64::<BigEndian>(*x)?,
                FieldValue::Float(x) => self.writer.write_f32::<BigEndian>(*x)?,
                FieldValue::Int(x) => self.writer.write_i32::<BigEndian>(*x)?,
                FieldValue::Long(x) => self.writer.write_i64::<BigEndian>(*x)?,
                FieldValue::Short(x) => self.writer.write_i16::<BigEndian>(*x)?,
                FieldValue::String(x) => {
                    dc_util::write_sized_byte_buf(&mut self.writer, x.as_bytes())?
                }
                FieldValue::MultiDimDoubleArray(x) => {
                    dc_util::write_multi_dim_double_array(&mut self.writer, x)?
                }
                FieldValue::None => continue,
            };
        }
        Ok(())
    }

    fn flush(&mut self) -> ChopperResult<()> {
        self.writer.flush()?;
        Ok(())
    }
}
