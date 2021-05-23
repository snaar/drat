use serde::ser::{Impossible, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::chopper::chopper::{DataSink, HeaderSink};
use crate::chopper::types::{Header, Row};
use crate::serde::ser::error::SerError;
use crate::serde::ser::from_struct::header::to_header;
use crate::serde::ser::from_struct::row::to_row;

pub fn to_header_sink<T, N>(
    value: &T,
    timestamp_field_name: N,
    header_sink: Box<dyn HeaderSink>,
) -> Result<Box<dyn DataSink>, SerError>
where
    T: Serialize + ?Sized,
    N: AsRef<str>,
{
    value.serialize(HeaderSinkSerializer::new(timestamp_field_name, header_sink))
}

enum SinkStage {
    Header(Option<Box<dyn HeaderSink>>),
    Data(Box<dyn DataSink>),
}

pub struct HeaderSinkSerializer<N: AsRef<str>> {
    timestamp_field_name: N,
    sink: SinkStage,
    row_buf: Vec<Row>,
}

impl<N: AsRef<str>> HeaderSinkSerializer<N> {
    pub fn new(
        timestamp_field_name: N,
        header_sink: Box<dyn HeaderSink>,
    ) -> HeaderSinkSerializer<N> {
        HeaderSinkSerializer {
            timestamp_field_name,
            sink: SinkStage::Header(Some(header_sink)),
            row_buf: Vec::new(),
        }
    }
}

impl<N: AsRef<str>> Serializer for HeaderSinkSerializer<N> {
    type Ok = Box<dyn DataSink>;
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

    err_type_not_supported! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes
        none unit unit_struct unit_variant newtype_variant
        tuple tuple_struct tuple_variant map struct struct_variant
    }
}

impl<N: AsRef<str>> SerializeSeq for HeaderSinkSerializer<N> {
    type Ok = Box<dyn DataSink>;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if let SinkStage::Header(header_sink) = &mut self.sink {
            let header_sink = std::mem::take(header_sink).unwrap();
            let mut header = to_header(value, &self.timestamp_field_name)?;
            let data_sink = header_sink.process_header(&mut header)?;
            self.sink = SinkStage::Data(data_sink);
        }

        match &mut self.sink {
            SinkStage::Data(data_sink) => {
                self.row_buf
                    .push(to_row(value, &self.timestamp_field_name)?);
                data_sink.write_row(&mut self.row_buf)?;
                self.row_buf.clear();
            }
            SinkStage::Header(_) => {
                panic!("header should have been processed already")
            }
        };
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(match self.sink {
            SinkStage::Header(header_sink) => {
                let mut header = Header::new(Vec::new(), Vec::new());
                header_sink.unwrap().process_header(&mut header)?
            }
            SinkStage::Data(data_sink) => data_sink,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use crate::chopper::types::{FieldType, Header, Row};
    use crate::serde::ser::from_struct::header_sink::to_header_sink;
    use crate::serde::ser::from_struct::row::to_row;
    use crate::write::asserting_sink::AssertingSink;

    #[test]
    fn test() {
        #[derive(Serialize)]
        struct InputRow {
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

        let input_rows: Vec<InputRow> = vec![
            InputRow {
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
            InputRow {
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
            InputRow {
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

        let expected_header = Header::new(
            vec![
                "a_bool".to_string(),
                "a_byte".to_string(),
                "a_byte_buf".to_string(),
                "a_char".to_string(),
                "a_double".to_string(),
                "f_float".to_string(),
                "an_int".to_string(),
                "a_long".to_string(),
                "a_short".to_string(),
                "a_string".to_string(),
            ],
            vec![
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
            ],
        );
        let tfn = "timestamp";
        let expected_rows: Vec<Row> = vec![
            to_row(&input_rows[0], tfn).unwrap(),
            to_row(&input_rows[1], tfn).unwrap(),
            to_row(&input_rows[2], tfn).unwrap(),
        ];
        let header_sink = AssertingSink::new(expected_header, expected_rows);
        let mut data_sink = to_header_sink(&input_rows, tfn, Box::new(header_sink)).unwrap();
        data_sink.flush().unwrap();
    }
}
