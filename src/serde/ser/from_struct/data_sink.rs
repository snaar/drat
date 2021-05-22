use serde::ser::{Impossible, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::chopper::chopper::DataSink;
use crate::chopper::types::Row;
use crate::serde::ser::error::SerError;
use crate::serde::ser::from_struct::row::RowSerializer;

pub fn to_data_sink<T, D, N>(
    value: &T,
    timestamp_field_name: N,
    data_sink: Box<D>,
) -> Result<Box<D>, SerError>
where
    T: Serialize + ?Sized,
    D: DataSink + ?Sized,
    N: AsRef<str>,
{
    value.serialize(DataSinkSerializer::new(timestamp_field_name, data_sink))
}

pub struct DataSinkSerializer<D: DataSink + ?Sized, N: AsRef<str>> {
    timestamp_field_name: N,
    data_sink: Box<D>,
    row_vec: Vec<Row>,
}

impl<D: DataSink + ?Sized, N: AsRef<str>> DataSinkSerializer<D, N> {
    pub fn new(timestamp_field_name: N, data_sink: Box<D>) -> DataSinkSerializer<D, N> {
        DataSinkSerializer {
            timestamp_field_name,
            data_sink,
            row_vec: Vec::new(),
        }
    }
}

impl<D: DataSink + ?Sized, N: AsRef<str>> Serializer for DataSinkSerializer<D, N> {
    type Ok = Box<D>;
    type Error = SerError;
    type SerializeSeq = Self;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
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

impl<D: DataSink + ?Sized, N: AsRef<str>> SerializeSeq for DataSinkSerializer<D, N> {
    type Ok = Box<D>;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let serializer = RowSerializer::new(self.timestamp_field_name.as_ref());
        let row = value.serialize(serializer)?;
        self.row_vec.push(row);
        self.data_sink.write_row(&mut self.row_vec)?;
        self.row_vec.clear();
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.data_sink)
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use crate::serde::ser::from_struct::data_sink::to_data_sink;
    use crate::serde::ser::from_struct::row::to_row;
    use crate::write::vec_sink::VecSink;

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

        let rows: Vec<Row> = vec![
            Row {
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
            },
            Row {
                a_bool: true,
                a_byte: 15u8,
                a_byte_buf: vec![b'b'],
                a_char: 'b',
                a_double: 16.6f64,
                f_float: 17.7f32,
                an_int: 18i32,
                timestamp: 1123u64,
                a_long: 19i64,
                a_short: 110i16,
                a_string: "b".to_string(),
            },
            Row {
                a_bool: false,
                a_byte: 25u8,
                a_byte_buf: vec![b'c'],
                a_char: 'c',
                a_double: 26.6f64,
                f_float: 27.7f32,
                an_int: 28i32,
                timestamp: 2123u64,
                a_long: 29i64,
                a_short: 210i16,
                a_string: "c".to_string(),
            },
        ];

        let tfn = "timestamp";
        let sink = to_data_sink(&rows, tfn, Box::new(VecSink::new())).unwrap();
        assert_eq!(sink.rows.len(), 3);
        assert_eq!(sink.rows[0], to_row(&rows[0], tfn).unwrap());
        assert_eq!(sink.rows[1], to_row(&rows[1], tfn).unwrap());
        assert_eq!(sink.rows[2], to_row(&rows[2], tfn).unwrap());
    }
}
