use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{self, BufReader, Read};
use std::str;

use byteorder::{BigEndian, ReadBytesExt};
use lazy_static::lazy_static;
use ndarray::{prelude::*, StrideShape};

use crate::chopper::error::{ChopperResult, Error};
use crate::chopper::types::{FieldType, FieldValue, Header, Row};
use crate::source::source::Source;
use crate::util::dc_util;

// map for field types
lazy_static! {
    static ref FIELD_STRING_MAP_NAME: HashMap<&'static str, FieldType> =
        dc_util::create_field_string_map_name();
}

pub struct DCSource<R: Read> {
    reader: BufReader<R>,
    header: Header,
    field_count: usize,
    bitset_bytes: Vec<u8>,
    current_row: Row,
}

impl<R: Read> DCSource<R> {
    pub fn new(reader: R) -> ChopperResult<Self> {
        let mut reader = BufReader::new(reader);

        let magic_num = reader.read_u64::<BigEndian>()?;
        if &magic_num != &dc_util::MAGIC_NUM {
            return Err(Error::from(format!(
                "DCReader -- wrong magic number - {}",
                magic_num
            )));
        }

        let version = reader.read_u16::<BigEndian>()?;
        if &version != &dc_util::VERSION {
            return Err(Error::from(format!(
                "DCReader -- wrong version - {}",
                version
            )));
        }

        // skip user given data
        let user_header_size = reader.read_u32::<BigEndian>()?;
        for _i in 0..user_header_size as usize {
            reader.read_u8()?;
        }

        let map_field_string = &FIELD_STRING_MAP_NAME;
        let field_count = reader.read_u32::<BigEndian>()? as usize;
        let bitset_byte_count = dc_util::get_bitset_bytes(field_count);
        let bitset_bytes: Vec<u8> = vec![0 as u8; bitset_byte_count];

        // field descriptor (header)
        let mut field_names: Vec<String> = Vec::with_capacity(field_count);
        let mut field_types: Vec<FieldType> = Vec::with_capacity(field_count);
        let mut field_values: Vec<FieldValue> = Vec::with_capacity(field_count);
        for i in 0..field_count {
            let field_descriptor = dc_util::FieldDescriptor::new(&mut reader)?;
            let mut name = field_descriptor.get_name().to_string();
            // if field name is not given, assign default name - "col_x"
            if name.is_empty() {
                name = format!("col_{}", i);
            }
            field_names.push(name);
            field_types.push(
                map_field_string
                    .get(field_descriptor.get_type_string())
                    .unwrap()
                    .clone(),
            );
            field_values.push(FieldValue::None);
        }

        // Header
        let header: Header = Header::new(field_names, field_types);

        // Row
        let timestamp = 0 as u64;
        let current_row = Row {
            timestamp,
            field_values,
        };

        Ok(DCSource {
            reader,
            header,
            field_count,
            bitset_bytes,
            current_row,
        })
    }

    fn next_row(&mut self) -> ChopperResult<Option<Row>> {
        match self.reader.read_u64::<BigEndian>() {
            Ok(i) => self.current_row.timestamp = i,
            Err(_e) => return Ok(None),
        };

        // bitset of null values
        let bitset_bytes = &mut self.bitset_bytes;
        self.reader.read_exact(bitset_bytes)?;
        let bitset_bytes = &self.bitset_bytes;

        // get non-null fields, if null put string "null"
        let mut field_index = 0 as usize;
        for i in 0..bitset_bytes.len() {
            let mut current_bitset = bitset_bytes[i];
            for _j in 0..8 {
                self.current_row.field_values[field_index] = {
                    if current_bitset & 1 == 0 {
                        // not null
                        match self.header.field_types()[field_index] {
                            FieldType::Boolean => FieldValue::Boolean(self.reader.read_u8()? != 0),
                            FieldType::Byte => FieldValue::Byte(self.reader.read_u8()?),
                            FieldType::ByteBuf => {
                                FieldValue::ByteBuf(Self::read_byte_buf(&mut self.reader)?)
                            }
                            FieldType::Char => {
                                FieldValue::Char(self.reader.read_u16::<BigEndian>()?)
                            }
                            FieldType::Double => {
                                FieldValue::Double(self.reader.read_f64::<BigEndian>()?)
                            }
                            FieldType::Float => {
                                FieldValue::Float(self.reader.read_f32::<BigEndian>()?)
                            }
                            FieldType::Int => FieldValue::Int(self.reader.read_i32::<BigEndian>()?),
                            FieldType::Long => {
                                FieldValue::Long(self.reader.read_i64::<BigEndian>()?)
                            }
                            FieldType::Short => {
                                FieldValue::Short(self.reader.read_i16::<BigEndian>()?)
                            }
                            FieldType::String => {
                                FieldValue::String(Self::read_string(&mut self.reader)?)
                            }
                            FieldType::MultiDimDoubleArray => FieldValue::MultiDimDoubleArray(
                                Self::read_multi_dim_double_array(&mut self.reader)?,
                            ),
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
        Ok(Some(self.current_row.clone()))
    }

    fn read_encoded_size(reader: &mut BufReader<R>) -> ChopperResult<u32> {
        let data_size_short = reader.read_i16::<BigEndian>()?;
        Ok(match data_size_short {
            -1 => u32::try_from(reader.read_i32::<BigEndian>()?)?,
            _ => data_size_short as u16 as u32, // that's right, we want to convert to unsigned without sign extension
        })
    }

    fn read_byte_buf(reader: &mut BufReader<R>) -> ChopperResult<Vec<u8>> {
        let data_size = Self::read_encoded_size(reader)?;
        let mut buf: Vec<u8> = vec![0; data_size as usize];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_string(reader: &mut BufReader<R>) -> ChopperResult<String> {
        let buf = Self::read_byte_buf(reader)?;
        Ok(String::from_utf8(buf)?)
    }

    fn read_multi_dim_double_array(reader: &mut BufReader<R>) -> ChopperResult<ArrayD<f64>> {
        // how many dimensions
        let dim_count = Self::read_encoded_size(reader)?;

        // now size of every dimension
        let mut shape: Vec<usize> = Vec::new();
        let mut has_a_zero_dim = false;
        for _ in 0..dim_count {
            let dim = Self::read_encoded_size(reader)? as usize;
            has_a_zero_dim |= dim == 0;
            shape.push(dim);
        }

        Ok(if has_a_zero_dim {
            // there are either zero dimensions or at least one is zero;
            // ndarray treats this shape as scalar so put a NaN in there
            ArrayD::from_elem(shape, f64::NAN)
        } else {
            // we have at least one dimension, and all dimensions are non-zero
            let shape: StrideShape<IxDyn> = shape.into();
            let mut values: Vec<f64> = Vec::new();
            for _ in 0..shape.size() {
                values.push(reader.read_f64::<BigEndian>()?);
            }
            ArrayD::from_shape_vec(shape, values)?
        })
    }
}

impl<R: Read> Read for DCSource<R> {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        self.reader.read(into)
    }
}

impl<R: Read> Source for DCSource<R> {
    fn header(&self) -> &Header {
        &self.header
    }

    fn next_row(&mut self) -> ChopperResult<Option<Row>> {
        self.next_row()
    }
}
