use std::collections::HashMap;
use std::io::{Read, Write};
use std::rc::Rc;

use crate::chopper::error::ChopperResult;
use crate::chopper::types::FieldType;
use crate::source::dc_source::DCSource;
use crate::write::dc_sink::DCSink;

#[derive(Clone)]
pub struct DCFactory {
    field_type_to_name_map: Rc<HashMap<FieldType, String>>,
    field_name_to_type_map: Rc<HashMap<String, FieldType>>,
}

impl DCFactory {
    pub fn default() -> DCFactory {
        Self::new(Self::create_default_field_name_to_type_map())
    }

    pub fn new(field_name_to_type_map: HashMap<String, FieldType>) -> DCFactory {
        let mut field_type_to_name_map: HashMap<FieldType, String> = HashMap::new();
        for (field_name, field_type) in field_name_to_type_map.iter() {
            field_type_to_name_map.insert(field_type.clone(), field_name.clone());
        }

        DCFactory {
            field_type_to_name_map: Rc::new(field_type_to_name_map),
            field_name_to_type_map: Rc::new(field_name_to_type_map),
        }
    }

    pub fn new_source<R: Read>(&self, reader: R) -> ChopperResult<DCSource<R>> {
        DCSource::new(reader, self.field_name_to_type_map.clone())
    }

    pub fn new_sink<W: 'static + Write>(&self, writer: W) -> ChopperResult<DCSink<W>> {
        DCSink::new(writer, self.field_type_to_name_map.clone())
    }

    pub fn create_default_field_name_to_type_map() -> HashMap<String, FieldType> {
        let mut map = HashMap::new();
        map.insert("Z".to_string(), FieldType::Boolean);
        map.insert("B".to_string(), FieldType::Byte);
        map.insert("Ljava.lang.ByteBuffer;".to_string(), FieldType::ByteBuf);
        map.insert("C".to_string(), FieldType::Char);
        map.insert("D".to_string(), FieldType::Double);
        map.insert("F".to_string(), FieldType::Float);
        map.insert("I".to_string(), FieldType::Int);
        map.insert("J".to_string(), FieldType::Long);
        map.insert("S".to_string(), FieldType::Short);
        map.insert("Ljava.lang.String;".to_string(), FieldType::String);
        map.insert(
            "MultiDimDoubleArray".to_string(),
            FieldType::MultiDimDoubleArray,
        );
        map
    }
}
