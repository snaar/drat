use serde::ser::{Impossible, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::chopper::sink::DataSink;
use crate::chopper::types::Row;
use crate::serde::ser::error::SerError;
use crate::serde::ser::from_struct::row::RowSerializer;

pub fn to_data_sink<T, D, N>(
    value: &T,
    timestamp_field_name: N,
    data_sink: D,
) -> Result<D, SerError>
where
    T: Serialize + ?Sized,
    D: DataSink,
    N: AsRef<str>,
{
    value.serialize(DataSinkSerializer::new(timestamp_field_name, data_sink))
}

pub struct DataSinkSerializer<D: DataSink, N: AsRef<str>> {
    timestamp_field_name: N,
    data_sink: D,
    row_vec: Vec<Row>,
}

impl<D: DataSink, N: AsRef<str>> DataSinkSerializer<D, N> {
    pub fn new(timestamp_field_name: N, data_sink: D) -> DataSinkSerializer<D, N> {
        DataSinkSerializer {
            timestamp_field_name,
            data_sink,
            row_vec: Vec::new(),
        }
    }
}

impl<D: DataSink, N: AsRef<str>> Serializer for DataSinkSerializer<D, N> {
    type Ok = D;
    type Error = SerError;
    type SerializeSeq = Self;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
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

    return_error! { <type_not_supported>
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes
        none unit unit_struct unit_variant newtype_variant
        tuple tuple_struct tuple_variant map struct struct_variant
    }
}

impl<D: DataSink, N: AsRef<str>> SerializeSeq for DataSinkSerializer<D, N> {
    type Ok = D;
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
        let sink = to_data_sink(&rows, tfn, VecSink::new()).unwrap();
        assert_eq!(sink.rows.len(), 3);
        assert_eq!(sink.rows[0], to_row(&rows[0], tfn).unwrap());
        assert_eq!(sink.rows[1], to_row(&rows[1], tfn).unwrap());
        assert_eq!(sink.rows[2], to_row(&rows[2], tfn).unwrap());
    }
}
