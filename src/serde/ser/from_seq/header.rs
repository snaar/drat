use serde::ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct};
use serde::{Serialize, Serializer};

use crate::chopper::types::{FieldType, Header};
use crate::serde::ser::error::SerError;
use crate::serde::ser::field_type::to_field_type;

pub fn to_header<T>(value: &T, timestamp_field_index: usize) -> Result<Header, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(HeaderSerializer::new(timestamp_field_index))
}

pub struct HeaderSerializer {
    timestamp_field_index: usize,
    field_types: Vec<FieldType>,
}

impl HeaderSerializer {
    pub fn new(timestamp_field_index: usize) -> HeaderSerializer {
        HeaderSerializer {
            timestamp_field_index,
            field_types: Vec::new(),
        }
    }

    fn into_header(self) -> Result<Header, SerError> {
        if self.field_types.is_empty() {
            return Err(SerError::NoTimestampField);
        }

        let mut field_types = self.field_types;
        let field_names = Header::generate_default_field_names(field_types.len() - 1);

        let idx = self.timestamp_field_index;
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

    err_type_not_supported! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes
        none unit unit_struct unit_variant newtype_variant
        tuple_variant map struct_variant
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
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
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

#[cfg(test)]
mod tests {
    use crate::chopper::types::{FieldType, Header};
    use crate::serde::ser::from_seq::header::to_header;

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

        let header = to_header(&row, 7).unwrap();
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
