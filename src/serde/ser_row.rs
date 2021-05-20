use serde::ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct};
use serde::{Serialize, Serializer};

use crate::chopper::types::Row;
use crate::serde::ser_error::SerError;
use crate::serde::ser_field_value::to_field_value;
use crate::serde::ser_u64_timestamp::U64TimestampSerializer;
use crate::serde::ser_util::TimestampFieldLocator;

pub fn to_row<T>(value: &T, timestamp_field_locator: TimestampFieldLocator) -> Result<Row, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(RowSerializer::new(timestamp_field_locator))
}

pub struct RowSerializer {
    timestamp_field_locator: TimestampFieldLocator,
}

impl RowSerializer {
    pub fn new(timestamp_field_locator: TimestampFieldLocator) -> RowSerializer {
        RowSerializer {
            timestamp_field_locator,
        }
    }
}

impl Serializer for RowSerializer {
    type Ok = Row;
    type Error = SerError;
    type SerializeSeq = RowSerializerWithTimestampFieldIndex;
    type SerializeTuple = RowSerializerWithTimestampFieldIndex;
    type SerializeTupleStruct = RowSerializerWithTimestampFieldIndex;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = RowSerializerFromStruct;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("bool"))
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("i8"))
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("i16"))
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("i32"))
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("i64"))
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("u8"))
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("u16"))
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("u32"))
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("u64"))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("f32"))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("f64"))
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("char"))
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("&str"))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("&[u8]"))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("none"))
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("unit"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(SerError::type_not_supported("unit variant"))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(SerError::type_not_supported("newtype variant"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        RowSerializerWithTimestampFieldIndex::new(self.timestamp_field_locator)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        RowSerializerWithTimestampFieldIndex::new(self.timestamp_field_locator)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        RowSerializerWithTimestampFieldIndex::new(self.timestamp_field_locator)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(SerError::type_not_supported("tuple variant"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(SerError::type_not_supported("map"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        RowSerializerFromStruct::new(self.timestamp_field_locator)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(SerError::type_not_supported("struct variant"))
    }
}

pub struct RowSerializerWithTimestampFieldIndex {
    row: Row,
    timestamp_index: usize,
    fields_processed_count: usize,
}

impl RowSerializerWithTimestampFieldIndex {
    fn new(
        timestamp_field_locator: TimestampFieldLocator,
    ) -> Result<RowSerializerWithTimestampFieldIndex, SerError> {
        let timestamp_index = match timestamp_field_locator {
            TimestampFieldLocator::ByName(_) => {
                return Err(SerError::InvalidTimestampFieldLocator);
            }
            TimestampFieldLocator::ByIndex(index) => index,
        };

        Ok(RowSerializerWithTimestampFieldIndex {
            row: Row {
                timestamp: 0,
                field_values: vec![],
            },
            timestamp_index,
            fields_processed_count: 0,
        })
    }
}

impl SerializeSeq for RowSerializerWithTimestampFieldIndex {
    type Ok = Row;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.timestamp_index == self.fields_processed_count {
            self.row.timestamp = value.serialize(U64TimestampSerializer {})?;
        } else {
            self.row.field_values.push(to_field_value(value)?);
        }
        self.fields_processed_count += 1;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.row)
    }
}

impl SerializeTuple for RowSerializerWithTimestampFieldIndex {
    type Ok = Row;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.timestamp_index == self.fields_processed_count {
            self.row.timestamp = value.serialize(U64TimestampSerializer {})?;
        } else {
            self.row.field_values.push(to_field_value(value)?);
        }
        self.fields_processed_count += 1;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.row)
    }
}

impl SerializeTupleStruct for RowSerializerWithTimestampFieldIndex {
    type Ok = Row;
    type Error = SerError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.timestamp_index == self.fields_processed_count {
            self.row.timestamp = value.serialize(U64TimestampSerializer {})?;
        } else {
            self.row.field_values.push(to_field_value(value)?);
        }
        self.fields_processed_count += 1;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.row)
    }
}

pub struct RowSerializerFromStruct {
    row: Row,
    timestamp_name: String,
}

impl RowSerializerFromStruct {
    fn new(
        timestamp_field_locator: TimestampFieldLocator,
    ) -> Result<RowSerializerFromStruct, SerError> {
        let timestamp_name = match timestamp_field_locator {
            TimestampFieldLocator::ByName(name) => name,
            TimestampFieldLocator::ByIndex(_) => {
                return Err(SerError::InvalidTimestampFieldLocator);
            }
        };

        Ok(RowSerializerFromStruct {
            row: Row {
                timestamp: 0,
                field_values: vec![],
            },
            timestamp_name,
        })
    }
}

impl SerializeStruct for RowSerializerFromStruct {
    type Ok = Row;
    type Error = SerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if key == self.timestamp_name {
            self.row.timestamp = value.serialize(U64TimestampSerializer {})?;
        } else {
            self.row.field_values.push(to_field_value(value)?);
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.row)
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use crate::chopper::types::FieldValue;
    use crate::serde::ser_row::to_row;
    use crate::serde::ser_util::TimestampFieldLocator;

    #[test]
    fn test_timestamp_by_name() {
        #[derive(Serialize)]
        struct Row {
            a_bool: bool,
            a_byte: u8,
            a_byte_buf: Vec<u8>,
            a_char: char,
            a_double: f64,
            f_float: f32,
            an_int: i32,
            timestamp: u64,
            a_long: i64,
            a_short: i16,
            a_string: String,
        }

        let row = Row {
            a_bool: false,
            a_byte: 5u8,
            a_byte_buf: vec![b'a'],
            a_char: 'a',
            a_double: 6.6f64,
            f_float: 7.7f32,
            an_int: 8i32,
            timestamp: 123u64,
            a_long: 9i64,
            a_short: 10i16,
            a_string: "a".to_string(),
        };

        let row = to_row(&row, TimestampFieldLocator::ByName("timestamp".to_string())).unwrap();
        assert_eq!(row.timestamp, 123u64);
        assert_eq!(row.field_values.len(), 10);
        assert_eq!(
            row.field_values,
            vec![
                FieldValue::Boolean(false),
                FieldValue::Byte(5u8),
                FieldValue::ByteBuf(vec![b'a']),
                FieldValue::Char('a' as u16),
                FieldValue::Double(6.6f64),
                FieldValue::Float(7.7f32),
                FieldValue::Int(8i32),
                FieldValue::Long(9i64),
                FieldValue::Short(10i16),
                FieldValue::String("a".to_string()),
            ]
        );
    }

    #[test]
    fn test_timestamp_by_index() {
        let row = (
            false,
            5u8,
            vec![b'a'],
            'a',
            6.6f64,
            7.7f32,
            8i32,
            123u64,
            9i64,
            10i16,
            "a".to_string(),
        );

        let row = to_row(&row, TimestampFieldLocator::ByIndex(7)).unwrap();
        assert_eq!(row.timestamp, 123u64);
        assert_eq!(row.field_values.len(), 10);
        assert_eq!(
            row.field_values,
            vec![
                FieldValue::Boolean(false),
                FieldValue::Byte(5u8),
                FieldValue::ByteBuf(vec![b'a']),
                FieldValue::Char('a' as u16),
                FieldValue::Double(6.6f64),
                FieldValue::Float(7.7f32),
                FieldValue::Int(8i32),
                FieldValue::Long(9i64),
                FieldValue::Short(10i16),
                FieldValue::String("a".to_string()),
            ]
        );
    }
}
