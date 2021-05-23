use serde::ser::{Impossible, SerializeStruct};
use serde::{Serialize, Serializer};

use crate::chopper::types::Row;
use crate::serde::ser::error::SerError;
use crate::serde::ser::field_value::to_field_value;
use crate::serde::ser::u64_timestamp::U64TimestampSerializer;

pub fn to_row<T, N>(value: &T, timestamp_field_name: N) -> Result<Row, SerError>
where
    T: Serialize + ?Sized,
    N: AsRef<str>,
{
    value.serialize(RowSerializer::new(timestamp_field_name))
}

pub struct RowSerializer<N: AsRef<str>> {
    timestamp_field_name: N,
    row: Row,
}

impl<N: AsRef<str>> RowSerializer<N> {
    pub fn new(timestamp_field_name: N) -> RowSerializer<N> {
        RowSerializer {
            timestamp_field_name,
            row: Row {
                timestamp: 0,
                field_values: vec![],
            },
        }
    }
}

impl<N: AsRef<str>> Serializer for RowSerializer<N> {
    type Ok = Row;
    type Error = SerError;
    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
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

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    err_type_not_supported! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes
        none unit unit_struct unit_variant newtype_variant
        seq tuple tuple_struct tuple_variant map struct_variant
    }
}

impl<N: AsRef<str>> SerializeStruct for RowSerializer<N> {
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
        if key == self.timestamp_field_name.as_ref() {
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
    use crate::serde::ser::from_struct::row::to_row;

    #[test]
    fn test() {
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

        let row = to_row(&row, "timestamp").unwrap();
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
