use std::cmp::Ordering;
use std::fmt;

use crate::error::{CliResult, Error};
use crate::util::timestamp_util;
use crate::util::tz::ChopperTz;

pub type Nanos = u64;

#[derive(Copy, Clone)]
pub struct TimestampRange {
    pub begin: Option<Nanos>,
    pub end: Option<Nanos>,
}

pub static TIMESTAMP_RANGE_DEFAULT: TimestampRange = TimestampRange {
    begin: None,
    end: None,
};

impl TimestampRange {
    pub fn new(begin: Option<&str>, end: Option<&str>, timezone: &ChopperTz) -> CliResult<Self> {
        let begin = match begin {
            Some(t) => Some(timestamp_util::parse_datetime_range_element(t, timezone)?),
            None => None,
        };
        let end = match end {
            Some(t) => Some(timestamp_util::parse_datetime_range_element(t, timezone)?),
            None => None,
        };
        Ok(TimestampRange { begin, end })
    }
}

#[derive(Clone)]
pub struct Header {
    field_names: Vec<String>,
    field_types: Vec<FieldType>,
}

impl PartialEq for Header {
    fn eq(&self, other: &Header) -> bool {
        self.field_names().eq(other.field_names()) && self.field_types().eq(other.field_types())
    }
}

impl Header {
    pub fn new(field_names: Vec<String>, field_types: Vec<FieldType>) -> Self {
        Header {
            field_names,
            field_types,
        }
    }

    pub fn field_names(&self) -> &Vec<String> {
        &self.field_names
    }

    pub fn update_field_names(&mut self, new_names: Vec<String>) {
        self.field_names = new_names;
    }

    pub fn field_types(&self) -> &Vec<FieldType> {
        &self.field_types
    }

    pub fn update_field_types(&mut self, new_types: Vec<FieldType>) {
        self.field_types = new_types;
    }

    pub fn field_names_mut(&mut self) -> &mut Vec<String> {
        &mut self.field_names
    }

    pub fn field_types_mut(&mut self) -> &mut Vec<FieldType> {
        &mut self.field_types
    }

    pub fn get_field_index(&self, name: &str) -> CliResult<usize> {
        match self.field_names.iter().position(|s| s == name) {
            None => Err(Error::Custom(format!(
                "Failed to find column named '{}'.",
                name
            ))),
            Some(i) => Ok(i),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FieldValue {
    Boolean(bool),
    Byte(u8),
    ByteBuf(Vec<u8>),
    Char(u16),
    Double(f64),
    Float(f32),
    Int(i32),
    Long(i64),
    Short(i16),
    String(String),
    None,
}

impl PartialOrd for FieldValue {
    fn partial_cmp(&self, other: &FieldValue) -> Option<Ordering> {
        match (self, other) {
            (FieldValue::Boolean(_x), FieldValue::Boolean(_y)) => {
                Error::from("FieldValue -- boolean field type is not supported").exit()
            }
            (FieldValue::Byte(x), FieldValue::Byte(y)) => Some(x.cmp(y)),
            (FieldValue::ByteBuf(_x), FieldValue::ByteBuf(_y)) => {
                Error::from("FieldValue -- ByteBuffer field type is not supported").exit()
            }
            (FieldValue::Char(x), FieldValue::Char(y)) => Some(x.cmp(y)),
            (FieldValue::Double(x), FieldValue::Double(y)) => x.partial_cmp(y),
            (FieldValue::Float(x), FieldValue::Float(y)) => x.partial_cmp(y),
            (FieldValue::Int(x), FieldValue::Int(y)) => Some(x.cmp(y)),
            (FieldValue::Long(x), FieldValue::Long(y)) => Some(x.cmp(y)),
            (FieldValue::Short(x), FieldValue::Short(y)) => Some(x.cmp(y)),
            (FieldValue::String(x), FieldValue::String(y)) => Some(x.cmp(y)),
            (FieldValue::None, FieldValue::None) => Some(Ordering::Equal),
            _ => Error::from(format!(
                "FieldValue -- cannot compare different field types - {} {}",
                self, other
            ))
            .exit(),
        }
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &FieldValue) -> bool {
        match (self, other) {
            (FieldValue::Boolean(_x), FieldValue::Boolean(_y)) => {
                Error::from("FieldValue -- boolean field type is not supported").exit()
            }
            (FieldValue::Byte(x), FieldValue::Byte(y)) => x == y,
            (FieldValue::ByteBuf(_x), FieldValue::ByteBuf(_y)) => {
                Error::from("FieldValue -- ByteBuffer field type is not supported").exit()
            }
            (FieldValue::Char(x), FieldValue::Char(y)) => x == y,
            (FieldValue::Double(x), FieldValue::Double(y)) => x == y,
            (FieldValue::Float(x), FieldValue::Float(y)) => x == y,
            (FieldValue::Int(x), FieldValue::Int(y)) => x == y,
            (FieldValue::Long(x), FieldValue::Long(y)) => x == y,
            (FieldValue::Short(x), FieldValue::Short(y)) => x == y,
            (FieldValue::String(x), FieldValue::String(y)) => x.eq(y),
            (FieldValue::None, FieldValue::None) => true,
            _ => false,
        }
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            FieldValue::Boolean(_x) => {
                Error::from("FieldValue -- boolean field type is not supported").exit()
            }
            FieldValue::Byte(x) => f.write_str(format!("byte[{}]", x).as_str()),
            FieldValue::ByteBuf(_x) => {
                Error::from("FieldValue -- ByteBuffer field type is not supported").exit()
            }
            FieldValue::Char(x) => f.write_str(format!("char[{}]", x).as_str()),
            FieldValue::Double(x) => f.write_str(format!("double[{}]", x).as_str()),
            FieldValue::Float(x) => f.write_str(format!("float[{}]", x).as_str()),
            FieldValue::Int(x) => f.write_str(format!("int[{}]", x).as_str()),
            FieldValue::Long(x) => f.write_str(format!("long[{}]", x).as_str()),
            FieldValue::Short(x) => f.write_str(format!("short[{}]", x).as_str()),
            FieldValue::String(x) => f.write_str(format!("string[{}]", x.as_str()).as_str()),
            FieldValue::None => f.write_str(""),
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub enum FieldType {
    Boolean,
    Byte,
    ByteBuf,
    Char,
    Double,
    Float,
    Int,
    Long,
    Short,
    String,
}

#[derive(Clone, Debug)]
pub struct Row {
    pub timestamp: Nanos,
    pub field_values: Vec<FieldValue>,
}
