use serde::ser::{Impossible, SerializeStruct};
use serde::{Serialize, Serializer};

use crate::chopper::types::{FieldType, Header};
use crate::serde::ser_error::SerError;
use crate::serde::ser_field_type::to_field_type;

pub fn to_header<T, N>(value: &T, timestamp_field_name: N) -> Result<Header, SerError>
where
    T: Serialize + ?Sized,
    N: AsRef<str>,
{
    value.serialize(HeaderSerializer::new(timestamp_field_name))
}

pub struct HeaderSerializer<N: AsRef<str>> {
    timestamp_field_name: N,
    field_names: Vec<String>,
    field_types: Vec<FieldType>,
}

impl<N: AsRef<str>> HeaderSerializer<N> {
    pub fn new(timestamp_field_name: N) -> HeaderSerializer<N> {
        HeaderSerializer {
            timestamp_field_name,
            field_names: Vec::new(),
            field_types: Vec::new(),
        }
    }

    fn into_header(self) -> Result<Header, SerError> {
        if self.field_types.is_empty() {
            return Err(SerError::NoTimestampField);
        }

        let mut field_names = self.field_names;
        let mut field_types = self.field_types;
        let timestamp_field_name = self.timestamp_field_name.as_ref();

        let idx = match field_names
            .iter()
            .position(|field_name| field_name == timestamp_field_name)
        {
            None => {
                return Err(SerError::timestamp_field_not_found(
                    self.timestamp_field_name.as_ref(),
                ));
            }
            Some(i) => i,
        };

        if field_types[idx] != FieldType::Long {
            return Err(SerError::InvalidTimestampFieldType);
        }

        field_names.remove(idx);
        field_types.remove(idx);

        Ok(Header::new(field_names, field_types))
    }
}

impl<N: AsRef<str>> Serializer for HeaderSerializer<N> {
    type Ok = Header;
    type Error = SerError;
    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
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
        Err(SerError::type_not_supported("seq"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerError::type_not_supported("tuple"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(SerError::type_not_supported("tuple struct"))
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

impl<N: AsRef<str>> SerializeStruct for HeaderSerializer<N> {
    type Ok = Header;
    type Error = SerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.field_names.push(key.to_string());
        self.field_types.push(to_field_type(value)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.into_header()
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use crate::chopper::types::FieldType;
    use crate::serde::ser::from_struct::header::to_header;

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

        let header = to_header(&row, "timestamp").unwrap();
        assert_eq!(header.field_names().len(), 10);
        assert_eq!(header.field_types().len(), 10);
        assert_eq!(
            header.field_names(),
            &vec![
                "a_bool".to_string(),
                "a_byte".to_string(),
                "a_byte_buf".to_string(),
                "a_char".to_string(),
                "a_double".to_string(),
                "f_float".to_string(),
                "an_int".to_string(),
                "a_long".to_string(),
                "a_short".to_string(),
                "a_string".to_string()
            ]
        );
        assert_eq!(
            header.field_types(),
            &vec![
                FieldType::Boolean,
                FieldType::Byte,
                FieldType::ByteBuf,
                FieldType::Char,
                FieldType::Double,
                FieldType::Float,
                FieldType::Int,
                FieldType::Long,
                FieldType::Short,
                FieldType::String,
            ]
        );
    }
}
