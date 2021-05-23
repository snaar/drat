use serde::ser::{Impossible, SerializeSeq, SerializeTuple};
use serde::{Serialize, Serializer};

use crate::chopper::types::FieldType;
use crate::serde::ser::byte_buf_element::ByteBufElementSerializer;
use crate::serde::ser::error::SerError;

pub fn to_field_type<T>(value: &T) -> Result<FieldType, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(FieldTypeSerializer {})
}

pub struct FieldTypeSerializer {}

impl Serializer for FieldTypeSerializer {
    type Ok = FieldType;
    type Error = SerError;
    type SerializeSeq = FieldTypeByteBufSerializer;
    type SerializeTuple = FieldTypeByteBufSerializer;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Boolean)
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Byte)
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Short)
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Int)
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Long)
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Byte)
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Short)
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Int)
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Long)
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Float)
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Double)
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::Char)
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::String)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::ByteBuf)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerError::NoneInHeader)
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::String)
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
        Ok(FieldTypeByteBufSerializer {})
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(FieldTypeByteBufSerializer {})
    }

    err_type_not_supported! {
        unit unit_struct newtype_variant tuple_struct tuple_variant
        map struct struct_variant
    }
}

// note that if there are no elements, then we will not get a chance to discover element type,
// so we will just have to assume that we are serializing an actual byte buffer,
// which is ok because serialization then will just fail later during row serialization
pub struct FieldTypeByteBufSerializer {}

impl SerializeSeq for FieldTypeByteBufSerializer {
    type Ok = FieldType;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        value.serialize(ByteBufElementSerializer {})?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::ByteBuf)
    }
}

impl SerializeTuple for FieldTypeByteBufSerializer {
    type Ok = FieldType;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        value.serialize(ByteBufElementSerializer {})?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(FieldType::ByteBuf)
    }
}

#[cfg(test)]
mod tests {
    use serde::Serializer;

    use crate::chopper::types::FieldType;
    use crate::serde::ser::field_type::{to_field_type, FieldTypeSerializer};

    #[test]
    fn test() {
        assert_eq!(to_field_type(&true).unwrap(), FieldType::Boolean);
        assert_eq!(to_field_type(&(-1 as i8)).unwrap(), FieldType::Byte);
        assert_eq!(to_field_type(&7).unwrap(), FieldType::Int);
        assert_eq!(to_field_type(&u64::MAX).unwrap(), FieldType::Long);
        assert_eq!(to_field_type(&0.25f32).unwrap(), FieldType::Float);
        assert_eq!(to_field_type(&0.25).unwrap(), FieldType::Double);
        assert_eq!(to_field_type(&b'a').unwrap(), FieldType::Byte);
        assert_eq!(to_field_type(&'a').unwrap(), FieldType::Char);
        assert_eq!(to_field_type("str").unwrap(), FieldType::String);
        // this uses serialize_seq
        assert_eq!(
            to_field_type("bytes".as_bytes()).unwrap(),
            FieldType::ByteBuf
        );
        // this uses serialize_tuple
        assert_eq!(to_field_type(&[4u8, 5u8, 6u8]).unwrap(), FieldType::ByteBuf);
        assert_eq!(to_field_type(&Some(10u16)).unwrap(), FieldType::Short);
    }

    #[test]
    fn test_bytes() {
        // test serialize_bytes explicitly because https://serde.rs/impl-serialize.html says:
        //
        // "Currently Serde does not use serialize_bytes in the Serialize impl for &[u8] or
        // Vec<u8> but once specialization lands in stable Rust we will begin using it."
        //
        let ser = FieldTypeSerializer {};
        assert_eq!(
            ser.serialize_bytes("bytes".as_bytes()).unwrap(),
            FieldType::ByteBuf
        );
    }
}
