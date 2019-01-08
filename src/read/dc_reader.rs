extern crate byteorder;

use byteorder::{BigEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io;
use std::str;

use read::dr;
use read::types::{FieldType, FieldValue, FieldDescriptor, Row};
use read::types;

pub struct DCReader<R> {
    reader: io::BufReader<R>,
    header: Vec<String>,
    field_types: Vec<FieldType>,
    field_count: usize,
    current_row: Row,
    map_field_string: HashMap<String, FieldType>,
}

impl <R: io::Read> DCReader<R> {
    pub fn new(reader: R) -> Self {
        let mut reader = io::BufReader::new(reader);
        let _magic_num = reader.read_u64::<BigEndian>().unwrap();
        let _version = reader.read_u16::<BigEndian>().unwrap();
        //TODO check magic and version

        // skip user given data
        let user_header_size = reader.read_u32::<BigEndian>().unwrap();
        for _i in 0..user_header_size as usize {
            reader.read_u8().unwrap();
        }

        // map for field types
        //TODO don't create new map for every file, create it once per program
        let map_field_string = types::create_field_string_map();

        // field descriptor
        let field_count = reader.read_u32::<BigEndian>().unwrap() as usize;
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

        DCReader { reader, header, field_types, field_count, current_row, map_field_string }
    }

    fn next_row(&mut self) -> Option<Row> {
        match self.reader.read_u64::<BigEndian>() {
            Ok(i) => self.current_row.timestamp = i,
            Err(_e) => {
                return None
            },
        };

        // bitset of null values
        let bitset_byte_count = 1 + ((self.field_count - 1) / 8);
        let mut bitset_bytes: Vec<u8> = Vec::with_capacity(bitset_byte_count as usize);
        for _i in 0..bitset_byte_count {
            bitset_bytes.push(self.reader.read_u8().unwrap()); //TODO read all bytes at once
        }

        // get non-null fields, if null put string "null"
        let mut field_index = 0 as usize;
        for one_byte in bitset_bytes {
            let mut current_bitset = one_byte;

            for _i in 0..8 {
                self.current_row.field_values[field_index] = {
                    if current_bitset & 1 == 0 { // not null
                        match self.field_types[field_index] {
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

        let mut string_vec: Vec<u8> = Vec::new();
        for _i in 0..data_size {
            string_vec.push(self.reader.read_u8().unwrap()); //TODO read all bytes at once
        }

        str::from_utf8_mut(&mut *string_vec).unwrap().to_string()
    }
}

impl <R: io::Read> io::Read for DCReader<R> {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        self.reader.read(into)
    }
}

impl <R: io::Read> dr::Reader for DCReader<R> {
    fn header(&self) -> &Vec<String> {
        &self.header
    }

    fn next_row(&mut self) -> Option<Row> {
        self.next_row()
    }
}
