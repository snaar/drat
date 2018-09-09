use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io;
use std::str;

pub type Nanos = u64;

#[derive(PartialEq, Clone)]
pub enum FieldValue {
    Double(f64),
    Float(f32),
    Int(i32),
    Long(i64),
    Short(i16),
    String(String),
    None,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum FieldType {
    Double,
    Float,
    Int,
    Long,
    Short,
    String,
}

pub fn create_field_string_map() -> HashMap<String, FieldType> {
    let mut map = HashMap::new();
    map.insert("D".to_string(), FieldType::Double);
    map.insert("".to_string(), FieldType::Float);
    map.insert("I".to_string(), FieldType::Int);
    map.insert("".to_string(), FieldType::Long);
    map.insert("".to_string(), FieldType::Short);
    map.insert("Ljava.lang.String;".to_string(), FieldType::String);
    map
}

#[derive(Clone)]
pub struct Row {
    pub timestamp: Nanos,
    pub field_values: Vec<FieldValue>,
}

// for DC files
pub struct FieldDescriptor {
    name: String,
    type_string: String,
    display_hint: i32, // -1: no hint, 0: timestamp
}

impl FieldDescriptor {
    pub fn new<R: io::BufRead>(mut reader: R) -> Self {
        let name = Self::get_sized_string(&mut reader);
        let type_string = Self::get_sized_string(&mut reader);
        let display_hint = reader.read_i32::<BigEndian>().unwrap();

        FieldDescriptor { name, type_string, display_hint }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_type_string(&self) -> &str {
        &self.type_string
    }

    pub fn get_display_hint(&self) -> i32 {
        self.display_hint
    }

    fn get_sized_string<R: io::BufRead>(mut rdr: R) -> String {
        let size = rdr.read_u32::<BigEndian>().unwrap();
        let mut string_bytes: Vec<u8> = Vec::with_capacity(size as usize);
        for _i in 0..size as usize {
            string_bytes.push(rdr.read_u8().unwrap());
        }
        str::from_utf8_mut(&mut string_bytes[0..]).unwrap().to_string()
    }
}

//pub struct SizedString<'a> {
//    size: u32,
//    string: &'a str, //u8[size]
//}
//
//impl SizedString {
//    pub fn new<R: io::BufRead>(rdr: &mut R) -> Self {
//        let size = rdr.read_u32::<BigEndian>().unwrap();
//        let mut string_bytes: Vec<u8> = Vec::with_capacity(size as usize);
//        for _i in 0..size as usize {
//            string_bytes.push(rdr.read_u8().unwrap());
//        }
//        let string = str::from_utf8_mut(&mut *string_bytes).unwrap().to_string();
//        SizedString { size, string }
//    }
//
//    fn get_string(&self) -> &str {
//        self.string
//    }
//}
