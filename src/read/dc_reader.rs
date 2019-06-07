use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::process;
use std::str;

use crate::util::dc_util;
use crate::dr::dr::Source;
use crate::dr::types::{FieldType, FieldValue, Header, Row};

// map for field types
lazy_static! {
    static ref FIELD_STRING_MAP_NAME: HashMap<&'static str, FieldType> = dc_util::creat_field_string_map_name();
}

pub struct DCReader<R> {
    reader: io::BufReader<R>,
    header: Header,
    field_count: usize,
    bitset_byte_count: usize,
    current_row: Row,
}

impl <R: io::Read> DCReader<R> {
    pub fn new(reader: R) -> Self {
        let mut reader = io::BufReader::new(reader);
        let magic_num = reader.read_u64::<BigEndian>().unwrap();
        if &magic_num != &dc_util::MAGIC_NUM {
            write_error!("Error: wrong magic number -- {}", magic_num);
        }
        let version = reader.read_u16::<BigEndian>().unwrap();
        if &version != &dc_util::VERSION {
            write_error!("Error: wrong version -- {}", version);
        }

        // skip user given data
        let user_header_size = reader.read_u32::<BigEndian>().unwrap();
        for _i in 0..user_header_size as usize {
            reader.read_u8().unwrap();
        }

        let map_field_string = &FIELD_STRING_MAP_NAME;
        let field_count = reader.read_u32::<BigEndian>().unwrap() as usize;
        let bitset_byte_count = dc_util::get_bitset_bytes(field_count);

        // field descriptor (header)
        let mut field_names: Vec<String> = Vec::with_capacity(field_count);
        let mut field_types: Vec<FieldType> = Vec::with_capacity(field_count);
        let mut field_values: Vec<FieldValue> = Vec::with_capacity(field_count);
        for i in 0..field_count {
            let field_descriptor = dc_util::FieldDescriptor::new(&mut reader);
            let mut name = field_descriptor.get_name().to_string();
            // if field name is not given, assign default name - "col_x"
            if name.is_empty() {
                name= format!("col_{}", i);
            }
            field_names.push(name);
            field_types.push(map_field_string.get(field_descriptor.get_type_string()).unwrap().clone());
            field_values.push(FieldValue::None);
        }

        // Header
        let header: Header = Header::new(field_names, field_types);

        // Row
        let timestamp = 0 as u64;
        let current_row = Row { timestamp, field_values };

        DCReader { reader, header, field_count, bitset_byte_count, current_row }
    }

    fn next_row(&mut self) -> Option<Row> {
        match self.reader.read_u64::<BigEndian>() {
            Ok(i) => self.current_row.timestamp = i,
            Err(_e) => return None,
        };

        // bitset of null values
        let mut bitset_bytes: Vec<u8> = vec![0 as u8; self.bitset_byte_count];
        let bitset_bytes = bitset_bytes.as_mut_slice();
        self.reader.read_exact(bitset_bytes).unwrap();

        // get non-null fields, if null put string "null"
        let mut field_index = 0 as usize;
        for i in 0..bitset_bytes.len() {
            let mut current_bitset = bitset_bytes[i];
            for _j in 0..8 {
                self.current_row.field_values[field_index] = {
                    if current_bitset & 1 == 0 { // not null
                        match self.header.field_types()[field_index] {
                            FieldType::Boolean => write_error!("Error: boolean field type is not supported"),
                            FieldType::Byte => FieldValue::Byte(self.reader.read_u8().unwrap()),
                            FieldType::ByteBuf => write_error!("Error: ByteBuffer field type is not supported"),
                            FieldType::Char => FieldValue::Char(self.reader.read_u16::<BigEndian>().unwrap()),
                            FieldType::Double => FieldValue::Double(self.reader.read_f64::<BigEndian>().unwrap()),
                            FieldType::Float => FieldValue::Float(self.reader.read_f32::<BigEndian>().unwrap()),
                            FieldType::Int => FieldValue::Int(self.reader.read_i32::<BigEndian>().unwrap()),
                            FieldType::Long => FieldValue::Long(self.reader.read_i64::<BigEndian>().unwrap()),
                            FieldType::Short => FieldValue::Short(self.reader.read_i16::<BigEndian>().unwrap()),
                            FieldType::String => FieldValue::String(self.read_string().to_owned()),
                        }
                    } else {
                        FieldValue::None
                    }
                };
                field_index += 1;
                if field_index >= self.field_count {
                    break;
                }
                current_bitset = current_bitset >> 1;
            }
        }
        Some(self.current_row.clone())
    }

    fn read_string(&mut self) -> String {
        let data_size_short = self.reader.read_i16::<BigEndian>().unwrap();
        let data_size = match data_size_short {
            -1 => self.reader.read_u32::<BigEndian>().unwrap(),
            _ => data_size_short as u32,
        };
        let mut string: Vec<u8> = vec![0; data_size as usize];
        let string = string.as_mut_slice();
        self.reader.read_exact(string).unwrap();

        str::from_utf8_mut(string).unwrap().to_string()
    }
}

impl <R: io::Read> io::Read for DCReader<R> {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        self.reader.read(into)
    }
}

impl <R: io::Read> Source for DCReader<R> {
    fn header(&self) -> &Header {
        &self.header
    }

    fn next_row(&mut self) -> Option<Row> {
        self.next_row()
    }
}
