use std::cmp::Ordering;
use std::process;
pub type Nanos = u64;

#[derive(Clone)]
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
                write_error!("Error: boolean field type is not supported");
                process::exit(1)
            }
            (FieldValue::Byte(x), FieldValue::Byte(y)) => Some(x.cmp(y)),
            (FieldValue::ByteBuf(_x), FieldValue::ByteBuf(_y)) => {
                write_error!("Error: ByteBuffer field type is not supported");
                process::exit(1)
            }
            (FieldValue::Char(x), FieldValue::Char(y)) => Some(x.cmp(y)),
            (FieldValue::Double(x), FieldValue::Double(y)) => x.partial_cmp(y),
            (FieldValue::Float(x), FieldValue::Float(y)) => x.partial_cmp(y),
            (FieldValue::Int(x), FieldValue::Int(y)) => Some(x.cmp(y)),
            (FieldValue::Long(x), FieldValue::Long(y)) => Some(x.cmp(y)),
            (FieldValue::Short(x), FieldValue::Short(y)) => Some(x.cmp(y)),
            (FieldValue::String(x), FieldValue::String(y)) => Some(x.cmp(y)),
            (FieldValue::None, FieldValue::None) => {
                write_error!("Error: cannot compare different field types");
                process::exit(1)
            },
            _ => {
                write_error!("Error: cannot compare different field types");
                process::exit(1)
            },
        }
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &FieldValue) -> bool {
        match (self, other) {
            (FieldValue::Boolean(_x), FieldValue::Boolean(_y)) => {
                write_error!("Error: boolean field type is not supported");
                process::exit(1)
            }
            (FieldValue::Byte(x), FieldValue::Byte(y)) => x == y,
            (FieldValue::ByteBuf(_x), FieldValue::ByteBuf(_y)) => {
                write_error!("Error: ByteBuffer field type is not supported");
                process::exit(1)
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

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
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

#[derive(Clone)]
pub struct Row {
    pub timestamp: Nanos,
    pub field_values: Vec<FieldValue>,
}
