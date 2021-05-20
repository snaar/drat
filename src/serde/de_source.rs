use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde::forward_to_deserialize_any;
use serde::{Deserialize, Deserializer};

use crate::chopper::chopper::Source;
use crate::serde::de_error::DeError;
use crate::serde::de_row::RowDeserializer;

pub fn from_source<'de, T>(
    source: Box<dyn Source>,
    field_name_for_row_timestamp: String,
) -> Result<T, DeError>
where
    T: Deserialize<'de>,
{
    Ok(T::deserialize(SourceDeserializer::new(
        source,
        field_name_for_row_timestamp,
    ))?)
}

pub struct SourceDeserializer {
    source: Box<dyn Source>,
    field_name_for_row_timestamp: String,
}

impl SourceDeserializer {
    pub fn new(
        source: Box<dyn Source>,
        field_name_for_row_timestamp: String,
    ) -> SourceDeserializer {
        SourceDeserializer {
            source,
            field_name_for_row_timestamp,
        }
    }
}

impl<'de> Deserializer<'de> for SourceDeserializer {
    type Error = DeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

impl<'de> SeqAccess<'de> for SourceDeserializer {
    type Error = DeError;

    fn next_element_seed<T>(
        &mut self,
        seed: T,
    ) -> Result<Option<<T as DeserializeSeed<'de>>::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        //TODO unwrap
        let row = match self.source.next_row().unwrap() {
            None => return Ok(None),
            Some(row) => row,
        };

        let deserializer = RowDeserializer::new_with_header(
            &row,
            self.source.header(),
            &self.field_name_for_row_timestamp,
        );

        Ok(Some(seed.deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use serde::Deserialize;
    use serde_with::{serde_as, DisplayFromStr};

    use crate::cli::util::YesNoAuto;
    use crate::serde::de_source::from_source;
    use crate::source::csv_configs::{
        CSVInputConfig, TimestampColConfig, TimestampConfig, TimestampFmtConfig,
    };
    use crate::source::csv_source::CSVSource;
    use crate::source::csv_timestamp::TimestampUnits;
    use crate::util::reader::ChopperBufPreviewer;
    use crate::util::tz::ChopperTz;

    #[test]
    fn test() {
        let reader: Box<dyn Read> =
            Box::new("timestamp,a_bool,an_int\n101,true,7\n102,false,8\n105,true,-10\n".as_bytes());
        let previewer = ChopperBufPreviewer::new(reader).unwrap();
        let timestamp_config = TimestampConfig::new(
            TimestampColConfig::Index(0),
            TimestampFmtConfig::Epoch(TimestampUnits::Nanos),
            ChopperTz::new_always_fails(),
        );
        let csv_input_config =
            CSVInputConfig::new(Some(","), YesNoAuto::Yes, timestamp_config).unwrap();
        let source = CSVSource::new(previewer, &csv_input_config).unwrap();

        #[serde_as]
        #[derive(Debug, Deserialize, PartialEq)]
        struct Row {
            ts_nanos: u64,
            timestamp: String,
            #[serde_as(as = "DisplayFromStr")]
            a_bool: bool,
            #[serde_as(as = "DisplayFromStr")]
            an_int: i32,
        }

        let v: Vec<Row> = from_source(Box::new(source), "ts_nanos".to_string()).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(
            v[0],
            Row {
                ts_nanos: 101,
                timestamp: "101".to_string(),
                a_bool: true,
                an_int: 7
            }
        );
        assert_eq!(
            v[1],
            Row {
                ts_nanos: 102,
                timestamp: "102".to_string(),
                a_bool: false,
                an_int: 8
            }
        );
        assert_eq!(
            v[2],
            Row {
                ts_nanos: 105,
                timestamp: "105".to_string(),
                a_bool: true,
                an_int: -10
            }
        );
    }
}
