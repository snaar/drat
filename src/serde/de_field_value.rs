use encode_unicode::CharExt;
use serde::de::{Error, IntoDeserializer, Unexpected, Visitor};
use serde::forward_to_deserialize_any;
use serde::{Deserialize, Deserializer};

use crate::chopper::types::FieldValue;
use crate::serde::de_error::DeError;
use crate::visit_unit;

pub fn from_field_value<'de: 'a, 'a, T>(field_value: &'a FieldValue) -> Result<T, DeError>
where
    T: Deserialize<'de>,
{
    T::deserialize(FieldValueDeserializer::new(field_value))
}

pub struct FieldValueDeserializer<'a> {
    field_value: &'a FieldValue,
}

impl<'a> FieldValueDeserializer<'a> {
    pub fn new(field_value: &'a FieldValue) -> Self {
        FieldValueDeserializer { field_value }
    }
}

impl<'de: 'a, 'a> Deserializer<'de> for FieldValueDeserializer<'a> {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.field_value {
            FieldValue::Boolean(v) => visitor.visit_bool(*v),
            FieldValue::Byte(v) => visitor.visit_u8(*v),
            FieldValue::ByteBuf(v) => visitor.visit_bytes(v), //TODO visit_borrowed_bytes
            FieldValue::Char(v) => {
                let c = char::from_utf16_tuple((*v, None)).map_err(|_| {
                    DeError::invalid_value(Unexpected::Other("invalid 2-byte utf-16 char"), &"char")
                })?;
                visitor.visit_char(c)
            }
            FieldValue::Double(v) => visitor.visit_f64(*v),
            FieldValue::Float(v) => visitor.visit_f32(*v),
            FieldValue::Int(v) => visitor.visit_i32(*v),
            FieldValue::Long(v) => visitor.visit_i64(*v),
            FieldValue::Short(v) => visitor.visit_i16(*v),
            FieldValue::String(v) => visitor.visit_str(v), //TODO visit_borrowed_str
            FieldValue::None => visitor.visit_none(),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 str string
        bytes byte_buf newtype_struct seq tuple
        tuple_struct map struct identifier
    }

    visit_unit! {
        unit unit_struct ignored_any
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.field_value {
            FieldValue::Byte(b) => visitor.visit_char(*b as char),
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.field_value == &FieldValue::None {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.field_value {
            FieldValue::String(v) => {
                //TODO BorrowedStrDeserializer::new(v)
                let v: &str = v.as_ref();
                v.into_deserializer()
                    .deserialize_enum(name, variants, visitor)
            }
            _ => self.deserialize_any(visitor),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use crate::chopper::types::FieldValue;
    use crate::serde::de_field_value::from_field_value;

    #[derive(Debug, Deserialize, PartialEq)]
    enum TestEnum {
        One,
        Two,
        Three,
    }

    #[test]
    fn test() {
        let v: bool = from_field_value(&FieldValue::Boolean(true)).unwrap();
        assert_eq!(v, true);
        let v: String = from_field_value(&FieldValue::String("chop".to_string())).unwrap();
        assert_eq!(v, "chop".to_string());
        let v: TestEnum = from_field_value(&FieldValue::String("Two".to_string())).unwrap();
        assert_eq!(v, TestEnum::Two);
        let v: char = from_field_value(&FieldValue::Char(b'c' as u16)).unwrap();
        assert_eq!(v, 'c');
        let v: char = from_field_value(&FieldValue::String("c".to_string())).unwrap();
        assert_eq!(v, 'c');
        let v: char = from_field_value(&FieldValue::Byte(b'c')).unwrap();
        assert_eq!(v, 'c');
        let v: i32 = from_field_value(&FieldValue::Int(7)).unwrap();
        assert_eq!(v, 7);
        let v: Option<i32> = from_field_value(&FieldValue::Int(7)).unwrap();
        assert_eq!(v, Some(7));
        let v: Option<i32> = from_field_value(&FieldValue::None).unwrap();
        assert_eq!(v, None);
    }
}
