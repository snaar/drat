use serde::ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct};
use serde::{Serialize, Serializer};

use crate::chopper::types::{FieldType, Header};
use crate::serde::ser_error::SerError;
use crate::serde::ser_field_type::to_field_type;
use crate::serde::ser_util::TimestampFieldLocator;

pub fn to_header<T>(
    value: &T,
    timestamp_field_locator: TimestampFieldLocator,
) -> Result<Header, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(HeaderSerializer::new(timestamp_field_locator))
}

pub struct HeaderSerializer {
    timestamp_field_locator: TimestampFieldLocator,
    field_names: Vec<String>,
    field_types: Vec<FieldType>,
}

impl HeaderSerializer {
    pub fn new(timestamp_field_locator: TimestampFieldLocator) -> HeaderSerializer {
        HeaderSerializer {
            timestamp_field_locator,
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

        let idx = match self.timestamp_field_locator {
            TimestampFieldLocator::ByName(name) => {
                if field_names.is_empty() {
                    return Err(SerError::InvalidTimestampFieldLocator);
                }

                let idx = match field_names.iter().position(|e| e == &name) {
                    None => {
                        return Err(SerError::timestamp_field_not_found(name));
                    }
                    Some(i) => i,
                };
                field_names.remove(idx);
                idx
            }
            TimestampFieldLocator::ByIndex(idx) => {
                if !field_names.is_empty() {
                    return Err(SerError::InvalidTimestampFieldLocator);
                }
                field_names = Header::generate_default_field_names(field_types.len() - 1);
                idx
            }
        };

        if field_types[idx] != FieldType::Long {
            return Err(SerError::InvalidTimestampFieldType);
        }
        field_types.remove(idx);

        Ok(Header::new(field_names, field_types))
    }
}

impl Serializer for HeaderSerializer {
    type Ok = Header;
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

impl SerializeSeq for HeaderSerializer {
    type Ok = Header;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.field_types.push(to_field_type(value)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.into_header()
    }
}

impl SerializeTuple for HeaderSerializer {
    type Ok = Header;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.field_types.push(to_field_type(value)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.into_header()
    }
}

impl SerializeTupleStruct for HeaderSerializer {
    type Ok = Header;
    type Error = SerError;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.field_types.push(to_field_type(value)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.into_header()
    }
}

impl SerializeStruct for HeaderSerializer {
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

    use crate::chopper::types::{FieldType, Header};
    use crate::serde::ser_header::{to_header, TimestampFieldLocator};

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

        let header =
            to_header(&row, TimestampFieldLocator::ByName("timestamp".to_string())).unwrap();
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

        let header = to_header(&row, TimestampFieldLocator::ByIndex(7)).unwrap();
        assert_eq!(header.field_names().len(), 10);
        assert_eq!(header.field_types().len(), 10);
        assert_eq!(
            header.field_names(),
            &Header::generate_default_field_names(10)
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
