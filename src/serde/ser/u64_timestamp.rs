use serde::ser::Impossible;
use serde::{Serialize, Serializer};

use crate::serde::ser::error::SerError;

pub struct U64TimestampSerializer {}

impl Serializer for U64TimestampSerializer {
    type Ok = u64;
    type Error = SerError;
    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v)
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

    err_not_a_u64_timestamp! {
        bool i8 i16 i32 i64 u8 u16 u32 f32 f64 char str bytes
        none some unit unit_struct unit_variant newtype_variant
        seq tuple tuple_struct tuple_variant map struct struct_variant
    }
}
