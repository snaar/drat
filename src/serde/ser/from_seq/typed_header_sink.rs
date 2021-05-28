use std::marker::PhantomData;

use serde::ser::{Impossible, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::chopper::sink::{TypedDataSink, TypedHeaderSink};
use crate::chopper::types::{Header, Row};
use crate::serde::ser::error::SerError;
use crate::serde::ser::from_seq::header::to_header;
use crate::serde::ser::from_seq::row::to_row;

pub fn to_typed_header_sink<T, W, D: TypedDataSink<W>, H: TypedHeaderSink<W, D>>(
    value: &T,
    timestamp_field_index: usize,
    header_sink: H,
) -> Result<D, SerError>
where
    T: Serialize + ?Sized,
{
    value.serialize(TypedHeaderSinkSerializer::new(
        timestamp_field_index,
        header_sink,
    ))
}

enum SinkStage<D, H> {
    Header(Option<H>),
    Data(D),
}

pub struct TypedHeaderSinkSerializer<W, D: TypedDataSink<W>, H: TypedHeaderSink<W, D>> {
    timestamp_field_index: usize,
    sink: SinkStage<D, H>,
    row_buf: Vec<Row>,
    phantom_w: PhantomData<W>,
}

impl<W, D: TypedDataSink<W>, H: TypedHeaderSink<W, D>> TypedHeaderSinkSerializer<W, D, H> {
    pub fn new(timestamp_field_index: usize, header_sink: H) -> TypedHeaderSinkSerializer<W, D, H> {
        TypedHeaderSinkSerializer {
            timestamp_field_index,
            sink: SinkStage::Header(Some(header_sink)),
            row_buf: Vec::new(),
            phantom_w: PhantomData::default(),
        }
    }
}

impl<W, D: TypedDataSink<W>, H: TypedHeaderSink<W, D>> Serializer
    for TypedHeaderSinkSerializer<W, D, H>
{
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

impl<W, D: TypedDataSink<W>, H: TypedHeaderSink<W, D>> SerializeSeq
    for TypedHeaderSinkSerializer<W, D, H>
{
    type Ok = D;
    type Error = SerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        if let SinkStage::Header(header_sink) = &mut self.sink {
            let header_sink = std::mem::take(header_sink).unwrap();
            let mut header = to_header(value, self.timestamp_field_index)?;
            let data_sink = header_sink.process_header(&mut header)?;
            self.sink = SinkStage::Data(data_sink);
        }

        match &mut self.sink {
            SinkStage::Data(data_sink) => {
                self.row_buf
                    .push(to_row(value, self.timestamp_field_index)?);
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
    use serde_with::{serde_as, DisplayFromStr};

    use crate::chopper::sink::TypedDataSink;
    use crate::serde::ser::from_seq::typed_header_sink::to_typed_header_sink;
    use crate::source::csv_configs::{CSVOutputConfig, TimestampStyle};
    use crate::source::csv_timestamp::TimestampUnits;
    use crate::util::tz::ChopperTz;
    use crate::write::csv_sink::CSVSink;

    #[test]
    fn test() {
        #[serde_as]
        #[derive(Serialize)]
        struct InputRow {
            #[serde_as(as = "DisplayFromStr")]
            a_bool: bool,
            #[serde_as(as = "DisplayFromStr")]
            a_byte: u8,
            #[serde_as(as = "DisplayFromStr")]
            a_char: char,
            #[serde_as(as = "DisplayFromStr")]
            a_double: f64,
            #[serde_as(as = "DisplayFromStr")]
            f_float: f32,
            #[serde_as(as = "DisplayFromStr")]
            an_int: i32,
            timestamp: u64,
            #[serde_as(as = "DisplayFromStr")]
            a_long: i64,
            #[serde_as(as = "DisplayFromStr")]
            a_short: i16,
            a_string: String,
        }

        let input_rows: Vec<InputRow> = vec![
            InputRow {
                a_bool: false,
                a_byte: 5u8,
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

        let expected_output = "\
            timestamp,col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8\n\
            123,false,5,a,6.6,7.7,8,9,10,a\n\
            1123,true,15,b,16.6,17.7,18,19,110,b\n\
            2123,false,25,c,26.6,27.7,28,29,210,c\n";

        let tfi = 6;
        let buf: Vec<u8> = Vec::new();
        let csv_output_config = CSVOutputConfig::new(
            ",",
            true,
            Some("timestamp".to_string()),
            TimestampStyle::Epoch,
            TimestampUnits::Nanos,
            ChopperTz::new_always_fails(),
        );
        let header_sink = CSVSink::new(buf, csv_output_config).unwrap();
        let data_sink = to_typed_header_sink(&input_rows, tfi, header_sink).unwrap();
        let output = String::from_utf8(data_sink.inner()).unwrap();

        assert_eq!(output, expected_output);
    }
}
