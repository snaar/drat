use encode_unicode::CharExt;
use serde::ser::{Impossible, SerializeSeq, SerializeTuple};
use serde::{Serialize, Serializer};

use crate::chopper::types::FieldValue;
use crate::serde::ser_byte_buf_element::ByteBufElementSerializer;
use crate::serde::ser_error::SerError;

pub fn to_field_value<T>(value: &T) -> Result<FieldValue, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(FieldValueSerializer {})
}

pub struct FieldValueSerializer {}

impl Serializer for FieldValueSerializer {
    type Ok = FieldValue;
    type Error = SerError;
    type SerializeSeq = FieldValueByteBufSerializer;
    type SerializeTuple = FieldValueByteBufSerializer;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Boolean(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Byte(v as u8))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Short(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Int(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Long(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Byte(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Short(v as i16))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Int(v as i32))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Long(v as i64))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Float(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::Double(v))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let (c16_1, c16_2) = v.to_utf16_tuple();
        if c16_2.is_none() {
            Ok(FieldValue::Char(c16_1))
        } else {
            Err(SerError::type_not_supported(
                "char that doesn't fit in 2-byte utf-16",
            ))
        }
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::String(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::ByteBuf(Vec::from(v)))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::None)
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
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        variant.serialize(self)
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
        Ok(FieldValueByteBufSerializer::new())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(FieldValueByteBufSerializer::new())
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
        Err(SerError::type_not_supported("struct"))
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

pub struct FieldValueByteBufSerializer {
    buf: Vec<u8>,
}

impl FieldValueByteBufSerializer {
    fn new() -> FieldValueByteBufSerializer {
        FieldValueByteBufSerializer { buf: Vec::new() }
    }
}

impl SerializeSeq for FieldValueByteBufSerializer {
    type Ok = FieldValue;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.buf.push(value.serialize(ByteBufElementSerializer {})?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::ByteBuf(self.buf))
    }
}

impl SerializeTuple for FieldValueByteBufSerializer {
    type Ok = FieldValue;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        self.buf.push(value.serialize(ByteBufElementSerializer {})?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(FieldValue::ByteBuf(self.buf))
    }
}

#[cfg(test)]
mod tests {
    use serde::Serializer;

    use crate::chopper::types::FieldValue;
    use crate::serde::ser_field_value::{to_field_value, FieldValueSerializer};

    #[test]
    fn test() {
        assert_eq!(to_field_value(&true).unwrap(), FieldValue::Boolean(true));
        assert_eq!(to_field_value(&(-1 as i8)).unwrap(), FieldValue::Byte(255));
        assert_eq!(to_field_value(&7).unwrap(), FieldValue::Int(7));
        assert_eq!(to_field_value(&u64::MAX).unwrap(), FieldValue::Long(-1));
        assert_eq!(to_field_value(&0.25f32).unwrap(), FieldValue::Float(0.25));
        assert_eq!(to_field_value(&0.25).unwrap(), FieldValue::Double(0.25));
        assert_eq!(to_field_value(&b'a').unwrap(), FieldValue::Byte(b'a'));
        assert_eq!(to_field_value(&'a').unwrap(), FieldValue::Char('a' as u16));
        assert_eq!(
            to_field_value("str").unwrap(),
            FieldValue::String("str".to_string())
        );
        // this uses serialize_seq
        assert_eq!(
            to_field_value("bytes".as_bytes()).unwrap(),
            FieldValue::ByteBuf(Vec::from("bytes"))
        );
        // this uses serialize_tuple
        assert_eq!(
            to_field_value(&[4u8, 5u8, 6u8]).unwrap(),
            FieldValue::ByteBuf(Vec::from([4u8, 5u8, 6u8]))
        );
        assert_eq!(
            to_field_value::<Option<()>>(&None).unwrap(),
            FieldValue::None
        );
        assert_eq!(to_field_value(&Some(10u16)).unwrap(), FieldValue::Short(10));
    }

    #[test]
    fn test_bytes() {
        // test serialize_bytes explicitly because https://serde.rs/impl-serialize.html says:
        //
        // "Currently Serde does not use serialize_bytes in the Serialize impl for &[u8] or
        // Vec<u8> but once specialization lands in stable Rust we will begin using it."
        //
        let ser = FieldValueSerializer {};
        assert_eq!(
            ser.serialize_bytes("bytes".as_bytes()).unwrap(),
            FieldValue::ByteBuf(Vec::from("bytes"))
        );
    }
}
