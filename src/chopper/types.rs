use std::cmp::Ordering;
use std::fmt;

use ndarray::ArrayD;

use crate::chopper::error::{ChopperResult, Error};
use crate::util::timestamp_util;
use crate::util::tz::ChopperTz;

pub type ChainId = usize;
pub type NodeId = usize;

pub type Nanos = u64;

#[derive(Copy, Clone)]
pub struct TimestampRange {
    pub begin: Option<Nanos>,
    pub end: Option<Nanos>,
}

pub static TIMESTAMP_RANGE_ALL: TimestampRange = TimestampRange {
    begin: None,
    end: None,
};

impl TimestampRange {
    pub fn new(
        begin: Option<&str>,
        end: Option<&str>,
        timezone: &ChopperTz,
    ) -> ChopperResult<Self> {
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

#[derive(Clone, Debug)]
pub struct Header {
    field_names: Vec<String>,
    field_types: Vec<FieldType>,
}

impl Header {
    pub fn generate_default_field_names(field_count: usize) -> Vec<String> {
        // if field name is not given, assign default name - "col_x"
        let mut field_names: Vec<String> = Vec::new();
        for i in 0..field_count {
            field_names.push(format!("col_{}", i));
        }
        field_names
    }
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

    pub fn get_field_index(&self, name: &str) -> ChopperResult<usize> {
        match self.field_names.iter().position(|s| s == name) {
            None => Err(Error::ColumnMissing(name.to_string())),
            Some(i) => Ok(i),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
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
    MultiDimDoubleArray(ArrayD<f64>),
    None,
}

impl PartialOrd for FieldValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            FieldValue::Boolean(v) => {
                if let FieldValue::Boolean(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::Byte(v) => {
                if let FieldValue::Byte(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::ByteBuf(v) => {
                if let FieldValue::ByteBuf(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::Char(v) => {
                if let FieldValue::Char(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::Double(v) => {
                if let FieldValue::Double(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::Float(v) => {
                if let FieldValue::Float(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::Int(v) => {
                if let FieldValue::Int(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::Long(v) => {
                if let FieldValue::Long(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::Short(v) => {
                if let FieldValue::Short(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::String(v) => {
                if let FieldValue::String(o) = other {
                    v.partial_cmp(o)
                } else {
                    None
                }
            }
            FieldValue::MultiDimDoubleArray(_) => None,
            FieldValue::None => {
                if &FieldValue::None == other {
                    Some(Ordering::Equal)
                } else {
                    None
                }
            }
        }
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            FieldValue::Boolean(x) => f.write_str(format!("bool[{}]", x).as_str()),
            FieldValue::Byte(x) => f.write_str(format!("byte[{}]", x).as_str()),
            FieldValue::ByteBuf(x) => f.write_str(format!("ByteBuf[len={}]", x.len()).as_str()),
            FieldValue::Char(x) => f.write_str(format!("char[{}]", x).as_str()),
            FieldValue::Double(x) => f.write_str(format!("double[{}]", x).as_str()),
            FieldValue::Float(x) => f.write_str(format!("float[{}]", x).as_str()),
            FieldValue::Int(x) => f.write_str(format!("int[{}]", x).as_str()),
            FieldValue::Long(x) => f.write_str(format!("long[{}]", x).as_str()),
            FieldValue::Short(x) => f.write_str(format!("short[{}]", x).as_str()),
            FieldValue::String(x) => f.write_str(format!("string[{}]", x.as_str()).as_str()),
            FieldValue::MultiDimDoubleArray(x) => {
                f.write_str("MultiDimDoubleArray[")?;
                f.write_str(
                    &x.shape()
                        .iter()
                        .map(|d| d.to_string())
                        .collect::<Vec<String>>()
                        .join("x"),
                )?;
                f.write_str("]")?;
                Ok(())
            }
            FieldValue::None => f.write_str("none[]"),
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
    MultiDimDoubleArray,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub timestamp: Nanos,
    pub field_values: Vec<FieldValue>,
}

impl Row {
    pub fn empty() -> Row {
        Row {
            timestamp: 0,
            field_values: vec![],
        }
    }
}
