use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io::{self, Write, BufWriter};
use std::string::String;

use crate::chopper::types::{FieldType};
use crate::error::{CliResult, Error};

pub const MAGIC_NUM: u64 = 0x44434154;
pub const VERSION: u16 = 2;

pub enum DisplayHint {
    Timestamp,
    None,
}

pub fn get_bitset_bytes(field_count: usize) -> usize {
    1+((field_count-1)/8)
}

// map for field types
lazy_static! {
    pub static ref FIELD_STRING_MAP_TYPE: HashMap<FieldType, &'static str>
                                        = creat_field_string_map_type();
}

pub fn creat_field_string_map_name() -> HashMap<&'static str, FieldType> {
    let mut map = HashMap::new();
    map.insert("Z", FieldType::Boolean);
    map.insert("B", FieldType::Byte);
    map.insert("Ljava.lang.ByteBuffer;", FieldType::ByteBuf);
    map.insert("C", FieldType::Char);
    map.insert("D", FieldType::Double);
    map.insert("F", FieldType::Float);
    map.insert("I", FieldType::Int);
    map.insert("J", FieldType::Long);
    map.insert("S", FieldType::Short);
    map.insert("Ljava.lang.String;", FieldType::String);
    map
}

pub fn creat_field_string_map_type() -> HashMap<FieldType, &'static str> {
    let mut map = HashMap::new();
    map.insert(FieldType::Boolean, "Z");
    map.insert(FieldType::Byte, "B");
    map.insert(FieldType::ByteBuf, "Ljava.lang.ByteBuffer;");
    map.insert(FieldType::Char, "C");
    map.insert(FieldType::Double, "D");
    map.insert(FieldType::Float, "F");
    map.insert(FieldType::Int, "I");
    map.insert(FieldType::Long, "J");
    map.insert(FieldType::Short, "S");
    map.insert(FieldType::String, "Ljava.lang.String;");
    map
}

pub struct FieldDescriptor {
    name: String,
    type_string: String,
    display_hint: DisplayHint,
}

impl FieldDescriptor {
    pub fn new<R: io::BufRead>(mut reader: R) -> CliResult<Self> {
        let name = Self::get_sized_string(&mut reader)?;
        let type_string = Self::get_sized_string(&mut reader)?;

        let display_hint = match reader.read_i32::<BigEndian>()? {
            -1 => DisplayHint::None,
            0 => DisplayHint::Timestamp,
            _ =>
                return Err(Error::Custom("FieldDescriptor -- missing display hint".to_owned()))
        };

        Ok(FieldDescriptor { name, type_string, display_hint })
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

    fn get_sized_string<R: io::BufRead>(mut rdr: R) -> CliResult<String> {
        let size = rdr.read_u32::<BigEndian>()?;
        let mut string_bytes: Vec<u8> = Vec::with_capacity(size as usize);
        for _i in 0..size as usize {
            string_bytes.push(rdr.read_u8()?);
        }
        Ok(String::from_utf8(string_bytes).unwrap().to_string())
    }
}

pub fn write_sized_string(writer: &mut BufWriter<Box<io::Write+'static>>, string: &str) -> CliResult<()> {
    let bytes = string.as_bytes();
    writer.write_u32::<BigEndian>(bytes.len() as u32)?;
    writer.write_all(bytes)?;
    Ok(())
}

pub fn write_string_value(writer: &mut BufWriter<Box<io::Write+'static>>, value: &str) -> CliResult<()> {
    let bytes = value.as_bytes();
    match bytes.len() {
        x if x <= std::i16::MAX as usize => writer.write_i16::<BigEndian>(bytes.len() as i16)?,
        _ => {
            writer.write_i16::<BigEndian>(-1)?;
            writer.write_u32::<BigEndian>(bytes.len() as u32)?;
        }
    }
    writer.write_all(bytes)?;
    Ok(())
}
