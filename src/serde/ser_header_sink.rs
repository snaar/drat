use serde::ser::{Impossible, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::chopper::chopper::{DataSink, HeaderSink};
use crate::chopper::types::{Header, Row};
use crate::serde::ser_error::SerError;
use crate::serde::ser_header::to_header;
use crate::serde::ser_row::to_row;
use crate::serde::ser_util::TimestampFieldLocator;

pub fn to_header_sink<T>(
    value: &T,
    timestamp_field_locator: TimestampFieldLocator,
    header_sink: Box<dyn HeaderSink>,
) -> Result<Box<dyn DataSink>, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(HeaderSinkSerializer::new(
        timestamp_field_locator,
        header_sink,
    ))
}

enum SinkStage {
    Header(Option<Box<dyn HeaderSink>>),
    Data(Box<dyn DataSink>),
}

pub struct HeaderSinkSerializer {
    timestamp_field_locator: TimestampFieldLocator,
    sink: SinkStage,
    row_buf: Vec<Row>,
}

impl HeaderSinkSerializer {
    pub fn new(
        timestamp_field_locator: TimestampFieldLocator,
        header_sink: Box<dyn HeaderSink>,
    ) -> HeaderSinkSerializer {
        HeaderSinkSerializer {
            timestamp_field_locator,
            sink: SinkStage::Header(Some(header_sink)),
            row_buf: Vec::new(),
        }
    }
}

impl Serializer for HeaderSinkSerializer {
    type Ok = Box<dyn DataSink>;
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

impl SerializeSeq for HeaderSinkSerializer {
    type Ok = Box<dyn DataSink>;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if let SinkStage::Header(header_sink) = &mut self.sink {
            let header_sink = std::mem::take(header_sink).unwrap();
            let mut header = to_header(value, self.timestamp_field_locator.clone())?;
            let data_sink = header_sink.process_header(&mut header)?;
            self.sink = SinkStage::Data(data_sink);
        }

        match &mut self.sink {
            SinkStage::Data(data_sink) => {
                //TODO remove .clone()
                self.row_buf
                    .push(to_row(value, self.timestamp_field_locator.clone())?);
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

    use crate::chopper::chopper::{DataSink, HeaderSink};
    use crate::chopper::types::{FieldType, Header, Row};
    use crate::error::CliResult;
    use crate::serde::ser_header_sink::to_header_sink;
    use crate::serde::ser_row::to_row;
    use crate::serde::ser_util::TimestampFieldLocator;

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

        let tfl = TimestampFieldLocator::ByName("timestamp".to_string());

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
        let expected_rows: Vec<Row> = vec![
            to_row(&input_rows[0], tfl.clone()).unwrap(),
            to_row(&input_rows[1], tfl.clone()).unwrap(),
            to_row(&input_rows[2], tfl.clone()).unwrap(),
        ];
        let header_sink = AssertingSink::new(expected_header, expected_rows);
        let mut data_sink =
            to_header_sink(&input_rows, tfl.clone(), Box::new(header_sink)).unwrap();
        data_sink.flush().unwrap();
    }

    struct AssertingSink {
        header: Header,
        rows: Vec<Row>,
        current_row: usize,
    }

    impl AssertingSink {
        pub fn new(header: Header, rows: Vec<Row>) -> AssertingSink {
            AssertingSink {
                header,
                rows,
                current_row: 0,
            }
        }
    }

    impl HeaderSink for AssertingSink {
        fn process_header(self: Box<Self>, header: &mut Header) -> CliResult<Box<dyn DataSink>> {
            assert_eq!(header, &self.header);
            Ok(self.boxed())
        }
    }

    impl DataSink for AssertingSink {
        fn write_row(&mut self, io_rows: &mut Vec<Row>) -> CliResult<()> {
            assert_ne!(self.rows.len(), self.current_row);
            assert_eq!(io_rows.len(), 1);
            assert_eq!(io_rows[0], self.rows[self.current_row]);
            self.current_row += 1;
            Ok(())
        }

        fn flush(&mut self) -> CliResult<()> {
            assert_eq!(self.rows.len(), self.current_row);
            Ok(())
        }

        fn boxed(self) -> Box<dyn DataSink> {
            Box::new(self)
        }
    }
}
