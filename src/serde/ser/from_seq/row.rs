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

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
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

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    return_error! { <type_not_supported>
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes
        none unit unit_struct unit_variant newtype_variant
        tuple_variant map struct_variant
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
