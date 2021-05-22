use serde::ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct};
use serde::{Serialize, Serializer};

use crate::chopper::types::Row;
use crate::serde::ser::error::SerError;
use crate::serde::ser::field_value::to_field_value;
use crate::serde::ser::u64_timestamp::U64TimestampSerializer;

pub fn to_row<T>(value: &T, timestamp_field_index: usize) -> Result<Row, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(RowSerializer::new(timestamp_field_index))
}

pub struct RowSerializer {
    timestamp_field_index: usize,
    fields_processed_count: usize,
    row: Row,
}

impl RowSerializer {
    pub fn new(timestamp_field_index: usize) -> RowSerializer {
        RowSerializer {
            timestamp_field_index,
            fields_processed_count: 0,
            row: Row {
                timestamp: 0,
                field_values: vec![],
            },
        }
    }
}

impl Serializer for RowSerializer {
    type Ok = Row;
    type Error = SerError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Self;
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
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
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
        Ok(self)
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

impl SerializeSeq for RowSerializer {
    type Ok = Row;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.timestamp_field_index == self.fields_processed_count {
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

impl SerializeTuple for RowSerializer {
    type Ok = Row;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.timestamp_field_index == self.fields_processed_count {
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

impl SerializeTupleStruct for RowSerializer {
    type Ok = Row;
    type Error = SerError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.timestamp_field_index == self.fields_processed_count {
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

impl SerializeStruct for RowSerializer {
    type Ok = Row;
    type Error = SerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if self.timestamp_field_index == self.fields_processed_count {
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

#[cfg(test)]
mod tests {
    use crate::chopper::types::FieldValue;
    use crate::serde::ser::from_seq::row::to_row;

    #[test]
    fn test() {
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

        let row = to_row(&row, 7).unwrap();
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
