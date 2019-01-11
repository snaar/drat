use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::process;
use std::str;

use crate::read::dr;
use crate::read::types::{FieldType, FieldValue, FieldDescriptor, Row};
use crate::read::types;

const MAGIC_NUM: u64 = 0x44434154;
const VERSION: u16 = 2;

// map for field types
lazy_static! {
    static ref FIELD_STRING_MAP: HashMap<&'static str, FieldType> = types::creat_field_string_map();
}

pub struct DCReader<'a, R> {
    reader: io::BufReader<R>,
    header: Vec<String>,
    field_types: Vec<FieldType>,
    field_count: usize,
    bitset_byte_count: usize,
    current_row: Row,
    map_field_string: &'a HashMap<&'static str, FieldType>,
}

impl <'a, R: io::Read> DCReader<'a, R> {
    pub fn new(reader: R) -> Self {
        let mut reader = io::BufReader::new(reader);
        let magic_num = reader.read_u64::<BigEndian>().unwrap();
        if magic_num != MAGIC_NUM {
            println!("ERROR: wrong magic number -- {}", magic_num);
        }
        let version = reader.read_u16::<BigEndian>().unwrap();
        if version != VERSION {
            println!("ERROR: wrong version -- {}", version);
        }

        // skip user given data
        let user_header_size = reader.read_u32::<BigEndian>().unwrap();
        for _i in 0..user_header_size as usize {
            reader.read_u8().unwrap();
        }

        let map_field_string = &FIELD_STRING_MAP;

        // field descriptor
        let field_count = reader.read_u32::<BigEndian>().unwrap() as usize;
        let bitset_byte_count = 1 + ((field_count - 1) / 8);
        let mut header: Vec<String> = Vec::with_capacity(field_count);
        let mut field_types: Vec<FieldType> = Vec::with_capacity(field_count);
        let mut field_values: Vec<FieldValue> = Vec::with_capacity(field_count);
        for _i in 0..field_count {
            let field_descriptor = FieldDescriptor::new(&mut reader);
            let field_name = field_descriptor.get_name();
            header.push(field_name.to_string());
            field_types.push(map_field_string.get(field_descriptor.get_type_string()).unwrap().clone());
            field_values.push(FieldValue::None);
        }

        // Row
        let timestamp = 0 as u64;
        let current_row = Row { timestamp, field_values };

        DCReader { reader, header, field_types, field_count, bitset_byte_count, current_row, map_field_string }
    }

    fn next_row(&mut self) -> Option<Row> {
        match self.reader.read_u64::<BigEndian>() {
            Ok(i) => self.current_row.timestamp = i,
            Err(_e) => {
                return None
            },
        };

        // bitset of null values
        let mut bitset_bytes: Vec<u8> = vec![0 as u8; self.bitset_byte_count];
        let bitset_bytes = bitset_bytes.as_mut_slice();
        self.reader.read_exact(bitset_bytes).unwrap();

        // get non-null fields, if null put string "null"
        let mut field_index = 0 as usize;
        for i in 0..bitset_bytes.len() {
            let mut current_bitset = bitset_bytes[i];

            for _i in 0..8 {
                self.current_row.field_values[field_index] = {
                    if current_bitset & 1 == 0 { // not null
                        match self.field_types[field_index] {
                            FieldType::Boolean => {
                                println!("ERROR: boolean field type is not supported");
                                process::exit(1);
                            },
                            FieldType::Byte => FieldValue::Byte(self.reader.read_u8().unwrap()),
                            FieldType::ByteBuf => {
                                println!("ERROR: ByteBuffer field type is not supported");
                                process::exit(1);
                            },
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
        let data_size = {
            let data_size_short = self.reader.read_i16::<BigEndian>().unwrap();
            match data_size_short {
                -1 => self.reader.read_u32::<BigEndian>().unwrap(),
                _ => data_size_short as u32
            }
        };

        let mut string: Vec<u8> = vec![0 as u8; data_size as usize];
        let string = string.as_mut_slice();
        self.reader.read_exact(string).unwrap();

        str::from_utf8_mut(string).unwrap().to_string()
    }
}

impl <'a, R: io::Read> io::Read for DCReader<'a, R> {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        self.reader.read(into)
    }
}

impl <'a, R: io::Read> dr::Reader for DCReader<'a, R> {
    fn header(&self) -> &Vec<String> {
        &self.header
    }

    fn next_row(&mut self) -> Option<Row> {
        self.next_row()
    }
}
