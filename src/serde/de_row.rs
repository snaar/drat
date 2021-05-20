use serde::de::{DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::forward_to_deserialize_any;
use serde::{Deserialize, Deserializer};

use crate::chopper::types::{Header, Row};
use crate::serde::de_error::DeError;
use crate::serde::de_field_value::FieldValueDeserializer;
use crate::visit_unit;

pub fn from_row_with_header<'de: 'a + 'b + 'c, 'a, 'b, 'c, T>(
    row: &'a Row,
    header: &'b Header,
    field_name_for_row_timestamp: &'c str,
) -> Result<T, DeError>
where
    T: Deserialize<'de>,
{
    Ok(T::deserialize(RowDeserializer::new_with_header(
        row,
        header,
        field_name_for_row_timestamp,
    ))?)
}

pub fn from_row_without_header<'de: 'a, 'a, T>(row: &'a Row) -> Result<T, DeError>
where
    T: Deserialize<'de>,
{
    Ok(T::deserialize(RowDeserializer::new_without_header(row))?)
}

enum RowCursor {
    Initial,
    Timestamp,
    Field(usize),
}

impl RowCursor {
    pub fn next(&self) -> RowCursor {
        match self {
            RowCursor::Initial => RowCursor::Timestamp,
            RowCursor::Timestamp => RowCursor::Field(0),
            RowCursor::Field(i) => RowCursor::Field(i + 1),
        }
    }
}

pub struct RowDeserializer<'a, 'b, 'c> {
    row: &'a Row,
    header: Option<&'b Header>,
    field_name_for_row_timestamp: Option<&'c str>,
    cursor: RowCursor,
}

impl<'a, 'b, 'c> RowDeserializer<'a, 'b, 'c> {
    pub fn new_with_header(
        row: &'a Row,
        header: &'b Header,
        field_name_for_row_timestamp: &'c str,
    ) -> Self {
        RowDeserializer {
            row,
            header: Some(header),
            field_name_for_row_timestamp: Some(field_name_for_row_timestamp),
            cursor: RowCursor::Initial,
        }
    }

    pub fn new_without_header(row: &'a Row) -> Self {
        RowDeserializer {
            row,
            header: None,
            field_name_for_row_timestamp: None,
            cursor: RowCursor::Initial,
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(match self.cursor {
            RowCursor::Initial | RowCursor::Timestamp => self.row.field_values.len() + 1,
            RowCursor::Field(i) => self.row.field_values.len() - i,
        })
    }
}

impl<'de: 'a + 'b + 'c, 'a, 'b, 'c> Deserializer<'de> for RowDeserializer<'a, 'b, 'c> {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.header.is_some() {
            visitor.visit_map(self)
        } else {
            visitor.visit_seq(self)
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit newtype_struct
        map struct enum identifier
    }

    visit_unit! {
        unit_struct ignored_any
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }
}

impl<'de: 'a + 'b + 'c, 'a, 'b, 'c> SeqAccess<'de> for RowDeserializer<'a, 'b, 'c> {
    type Error = DeError;

    fn next_element_seed<T>(
        &mut self,
        seed: T,
    ) -> Result<Option<<T as DeserializeSeed<'de>>::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.cursor = self.cursor.next();

        Ok(Some(match self.cursor {
            RowCursor::Initial => {
                panic!("unexpected cursor position")
            }
            RowCursor::Timestamp => seed.deserialize(self.row.timestamp.into_deserializer())?,
            RowCursor::Field(i) => {
                if i < self.row.field_values.len() {
                    seed.deserialize(FieldValueDeserializer::new(&self.row.field_values[i]))?
                } else {
                    return Ok(None);
                }
            }
        }))
    }

    fn size_hint(&self) -> Option<usize> {
        RowDeserializer::size_hint(self)
    }
}

impl<'de: 'a + 'b + 'c, 'a, 'b, 'c> MapAccess<'de> for RowDeserializer<'a, 'b, 'c> {
    type Error = DeError;

    fn next_key_seed<K>(
        &mut self,
        seed: K,
    ) -> Result<Option<<K as DeserializeSeed<'de>>::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        self.cursor = self.cursor.next();

        let str = match self.cursor {
            RowCursor::Initial => {
                panic!("unexpected cursor position")
            }
            RowCursor::Timestamp => self.field_name_for_row_timestamp.unwrap(),
            RowCursor::Field(i) => {
                if i < self.row.field_values.len() {
                    &self.header.unwrap().field_names()[i]
                } else {
                    return Ok(None);
                }
            }
        };
        //TODO BorrowedStrDeserializer::new(str)
        Ok(Some(seed.deserialize(str.into_deserializer())?))
    }

    fn next_value_seed<V>(
        &mut self,
        seed: V,
    ) -> Result<<V as DeserializeSeed<'de>>::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        Ok(match self.cursor {
            RowCursor::Initial => {
                panic!("unexpected cursor position")
            }
            RowCursor::Timestamp => seed.deserialize(self.row.timestamp.into_deserializer())?,
            RowCursor::Field(i) => {
                if i < self.row.field_values.len() {
                    seed.deserialize(FieldValueDeserializer::new(&self.row.field_values[i]))?
                } else {
                    panic!("reading cursor after last field");
                }
            }
        })
    }

    fn size_hint(&self) -> Option<usize> {
        RowDeserializer::size_hint(self)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use crate::chopper::types::{FieldType, FieldValue, Header, Row};
    use crate::serde::de_row::{from_row_with_header, from_row_without_header};

    #[test]
    fn test_with_header_tuple() {
        let header = Header::new(
            vec!["a_bool".to_string(), "an_int".to_string()],
            vec![FieldType::Boolean, FieldType::Int],
        );
        let row = Row {
            timestamp: 12345,
            field_values: vec![FieldValue::Boolean(true), FieldValue::Int(7)],
        };

        let v: (u32, bool, i32) = from_row_with_header(&row, &header, "timestamp").unwrap();
        assert_eq!(v.0, 12345);
        assert_eq!(v.1, true);
        assert_eq!(v.2, 7);
    }

    #[test]
    fn test_with_header_struct() {
        let header = Header::new(
            vec!["a_bool".to_string(), "an_int".to_string()],
            vec![FieldType::Boolean, FieldType::Int],
        );
        let row = Row {
            timestamp: 12345,
            field_values: vec![FieldValue::Boolean(true), FieldValue::Int(7)],
        };

        #[derive(Deserialize)]
        struct DeRow {
            timestamp: u64,
            an_int: i32,
            a_bool: bool,
        }

        let v: DeRow = from_row_with_header(&row, &header, "timestamp").unwrap();
        assert_eq!(v.timestamp, 12345);
        assert_eq!(v.a_bool, true);
        assert_eq!(v.an_int, 7);
    }

    #[test]
    fn test_without_header_tuple() {
        let row = Row {
            timestamp: 12345,
            field_values: vec![FieldValue::Boolean(true), FieldValue::Int(7)],
        };

        let v: (u32, bool, i32) = from_row_without_header(&row).unwrap();
        assert_eq!(v.0, 12345);
        assert_eq!(v.1, true);
        assert_eq!(v.2, 7);
    }

    #[test]
    fn test_without_header_struct() {
        let row = Row {
            timestamp: 12345,
            field_values: vec![FieldValue::Boolean(true), FieldValue::Int(7)],
        };

        #[derive(Deserialize)]
        struct DeRow {
            timestamp: u64,
            a_bool: bool,
            an_int: i32,
        }

        let v: DeRow = from_row_without_header(&row).unwrap();
        assert_eq!(v.timestamp, 12345);
        assert_eq!(v.a_bool, true);
        assert_eq!(v.an_int, 7);
    }
}
