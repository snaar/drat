use std::convert::TryFrom;
use std::io::{BufRead, Write};
use std::string::String;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use ndarray::ArrayD;

use crate::chopper::error::{ChopperResult, Error};

pub const MAGIC_NUM: u64 = 0x44434154;
pub const VERSION: u16 = 2;

pub enum DisplayHint {
    Timestamp,
    ArrayInt,
    ArrayDouble,
    ArrayLong,
    ArrayString,
    ArrayByte,
    MatrixDouble2D,
    None,
}

pub fn get_bitset_bytes(field_count: usize) -> usize {
    1 + ((field_count - 1) / 8)
}

pub struct FieldDescriptor {
    name: String,
    type_string: String,
    display_hint: DisplayHint,
}

impl FieldDescriptor {
    pub fn new<R: BufRead>(mut reader: R) -> ChopperResult<Self> {
        let name = Self::get_sized_string(&mut reader)?;
        let type_string = Self::get_sized_string(&mut reader)?;

        let display_hint = match reader.read_i32::<BigEndian>()? {
            -1 => DisplayHint::None,
            0 => DisplayHint::Timestamp,
            1 => DisplayHint::ArrayInt,
            2 => DisplayHint::ArrayDouble,
            3 => DisplayHint::ArrayLong,
            4 => DisplayHint::ArrayString,
            5 => DisplayHint::ArrayByte,
            6 => DisplayHint::MatrixDouble2D,
            n => return Err(Error::from(format!("unexpected display hint value: {}", n))),
        };

        Ok(FieldDescriptor {
            name,
            type_string,
            display_hint,
        })
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type_string(&self) -> &str {
        &self.type_string
    }

    pub fn get_display_hint(&self) -> &DisplayHint {
        &self.display_hint
    }

    fn get_sized_string<R: BufRead>(mut rdr: R) -> ChopperResult<String> {
        let size = rdr.read_u32::<BigEndian>()?;
        let mut string_bytes: Vec<u8> = Vec::with_capacity(size as usize);
        for _i in 0..size as usize {
            string_bytes.push(rdr.read_u8()?);
        }
        Ok(String::from_utf8(string_bytes).unwrap().to_string())
    }
}

pub fn write_u32_sized_string<W: Write>(writer: &mut W, string: &str) -> ChopperResult<()> {
    let bytes = string.as_bytes();
    writer.write_u32::<BigEndian>(bytes.len() as u32)?;
    writer.write_all(bytes)?;
    Ok(())
}

pub fn write_u16u32_size<W: Write>(writer: &mut W, size: usize) -> ChopperResult<()> {
    match size {
        x if x <= i16::MAX as usize => writer.write_u16::<BigEndian>(size as u16)?,
        _ => {
            writer.write_i16::<BigEndian>(-1)?;
            writer.write_u32::<BigEndian>(u32::try_from(size)?)?;
        }
    }
    Ok(())
}

pub fn write_sized_byte_buf<W: Write>(writer: &mut W, buf: &[u8]) -> ChopperResult<()> {
    write_u16u32_size(writer, buf.len())?;
    writer.write_all(buf)?;
    Ok(())
}

pub fn write_multi_dim_double_array<W: Write>(
    writer: &mut W,
    array: &ArrayD<f64>,
) -> ChopperResult<()> {
    let shape = array.shape();
    write_u16u32_size(writer, shape.len())?;
    for d in shape {
        write_u16u32_size(writer, *d)?;
    }

    if !shape.is_empty() {
        for f in array.iter() {
            writer.write_f64::<BigEndian>(*f)?;
        }
    }

    Ok(())
}
