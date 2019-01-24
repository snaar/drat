pub type Nanos = u64;

#[derive(PartialEq, Clone)]
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
