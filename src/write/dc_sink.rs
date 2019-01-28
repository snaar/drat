use byteorder::{BigEndian, WriteBytesExt};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write, BufWriter};
use std::path::PathBuf;
use std::process;

use crate::util::dc_util;
use crate::dr::dr;
use crate::dr::types::{Row, FieldValue, FieldType};
use crate::result::CliResult;

// map for field types
lazy_static! {
    static ref FIELD_STRING_MAP_TYPE: HashMap<FieldType, &'static str> = dc_util::creat_field_string_map_type();
}

pub struct DCSink {
    writer: BufWriter<Box<io::Write+'static>>,
    bitset_bytes: usize,
}

impl DCSink {
    pub fn new(path: &Option<String>, header: &dr::Header) -> Self {
        let writer = BufWriter::new(DCSink::into_writer(path).unwrap());
        let bitset_bytes = dc_util::get_bitset_bytes(header.get_field_types().len()-1);
        let mut dc_sink = DCSink { writer, bitset_bytes };
        Self::write_header(&mut dc_sink, header);

        dc_sink
    }

    fn into_writer(path: &Option<String>) -> io::Result<Box<io::Write>> {
        match path {
            None => {
                Ok(Box::new(io::stdout()))
            }
            Some(p) => {
                let path = PathBuf::from(p);
                let file = File::create(path).unwrap();
                Ok(Box::new(file))
            }
        }
    }

    fn write_header(mut dc_sink: &mut DCSink, header: &dr::Header) {
        DCSink::write_magic(&mut dc_sink);
        DCSink::write_version(&mut dc_sink);
        DCSink::write_empty_user_data(&mut dc_sink);
        DCSink::write_field_descriptors(&mut dc_sink, header);
    }

    fn write_magic(dc_sink: &mut DCSink) {
        dc_sink.writer.write_u64::<BigEndian>(dc_util::MAGIC_NUM).unwrap();
    }

    fn write_version(dc_sink: &mut DCSink) {
        dc_sink.writer.write_u16::<BigEndian>(dc_util::VERSION).unwrap();
    }

    fn write_empty_user_data(dc_sink: &mut DCSink) {
        dc_sink.writer.write_u32::<BigEndian>(0).unwrap();
    }

    fn write_field_descriptors(dc_sink: &mut DCSink, header: &dr::Header) {
        let field_types = header.get_field_types();
        let field_names = header.get_field_names();
        let field_count = field_types.len();

        // write field count
        dc_sink.writer.write_u32::<BigEndian>(field_count as u32).unwrap();

        // write field names (if available) and types as SizedStrings
        let has_field_name = field_names.len() >= 1;
        if has_field_name && field_names.len() != field_types.len() {
            eprintln!("Error: number of field name and number of field types does not match");
            process::exit(1);
        }
        for i in 0..field_types.len() {
            let name = match has_field_name {
                true => &field_names[i],
                false => "",
            };
            dc_util::write_sized_string(&mut dc_sink.writer, name);
            DCSink::write_field_type(dc_sink, &field_types[i]);
            DCSink::write_display_hint(dc_sink, dc_util::DisplayHint::None);
        }
    }

    fn write_field_type(dc_sink: &mut DCSink, field_types: &FieldType) {
        let field_string_map = &FIELD_STRING_MAP_TYPE;
        let type_string = match field_types {
            FieldType::Boolean => {
                eprintln!("Error: boolean field type is not supported");
                process::exit(1);
            },
            FieldType::Byte => field_string_map.get(&FieldType::Byte),
            FieldType::ByteBuf => {
                eprintln!("Error: ByteBuffer field type is not supported");
                process::exit(1);
            },
            FieldType::Char => field_string_map.get(&FieldType::Char),
            FieldType::Double => field_string_map.get(&FieldType::Double),
            FieldType::Float => field_string_map.get(&FieldType::Float),
            FieldType::Int => field_string_map.get(&FieldType::Int),
            FieldType::Long => field_string_map.get(&FieldType::Long),
            FieldType::Short => field_string_map.get(&FieldType::Short),
            FieldType::String => field_string_map.get(&FieldType::String),
        };
        match type_string {
            Some(t) => dc_util::write_sized_string(&mut dc_sink.writer, t),
            None => {
                eprintln!("Error: field type missing");
                process::exit(1)
            }
        }
    }

    fn write_display_hint(dc_sink: &mut DCSink, display_hint: dc_util::DisplayHint) {
        let hint: i32 = match display_hint {
            dc_util::DisplayHint::Timestamp => 0,
            dc_util::DisplayHint::None => -1,
        };
        dc_sink.writer.write_i32::<BigEndian>(hint).unwrap();
    }
}

impl dr::Sink for DCSink {
    fn write_row (&mut self, row: &Row) {
        // write timestamp
        self.writer.write_u64::<BigEndian>(row.timestamp).unwrap();

        // write bitset
        let field_values = &row.field_values;
        let mut bitset_bytes: Vec<u8> = Vec::with_capacity(self.bitset_bytes);
        let mut field_count = 0;

        for _i in 0..self.bitset_bytes {
            let mut current_bitset: u8 = 0;
            let mut current_bit = 0;
            for _j in 0..8 {
                if field_count >= field_values.len() {
                    break;
                }
                match &field_values.get(field_count).unwrap() {
                    FieldValue::None => current_bitset += 2_u8.pow(current_bit),
                    _ => current_bitset = current_bitset,
                }
                current_bit += 1;
                field_count += 1;
            }
            bitset_bytes.push(current_bitset);
        }
        self.writer.write_all(&bitset_bytes).unwrap();

        // write row values
        for value in field_values {
            match value {
                FieldValue::Boolean(_x) => {
                    println!("Error: boolean field type is not supported for writing DC file");
                    process::exit(1);
                },
                FieldValue::Byte(x) => self.writer.write_u8(*x).unwrap(),
                FieldValue::ByteBuf(_x) => {
                    eprintln!("Error: ByteBuffer field type is not supported for writing DC file");
                    process::exit(1);
                },
                FieldValue::Char(x) => self.writer.write_u16::<BigEndian>(*x).unwrap(),
                FieldValue::Double(x) => self.writer.write_f64::<BigEndian>(*x).unwrap(),
                FieldValue::Float(x) => self.writer.write_f32::<BigEndian>(*x).unwrap(),
                FieldValue::Int(x) => self.writer.write_i32::<BigEndian>(*x).unwrap(),
                FieldValue::Long(x) => self.writer.write_i64::<BigEndian>(*x).unwrap(),
                FieldValue::Short(x) => self.writer.write_i16::<BigEndian>(*x).unwrap(),
                FieldValue::String(x) => dc_util::write_string_value(&mut self.writer, x),
                FieldValue::None => continue,
            };
        }
    }

    fn flush(&mut self) -> CliResult<()> {
        self.writer.flush();
        Ok(())
    }

    fn boxed(&self) -> Box<&dr::Sink> {
        Box::new(self)
    }
}
