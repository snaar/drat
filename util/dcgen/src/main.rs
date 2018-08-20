extern crate byteorder;
extern crate clap;

use byteorder::{BigEndian, WriteBytesExt};
use clap::{App, Arg};
use std::fs::File;
use std::io::Write;

fn main() {
    let matches = App::new("dcgen")
        .arg(Arg::with_name("file")
            .required(true)
            .help("output file name"))
        .get_matches();

    let file_name = matches.value_of("file").unwrap();
    let file = File::create(file_name).unwrap();

    write_dc(file);
}

enum FieldType {
    Double,
    Int,
    String,
}

enum DisplayHint {
    Timestamp,
    None,
}

fn write_dc(out: File) {
    write_header(&out);
}

fn write_header(out: &File) {
    write_magic(&out);
    write_version(&out);
    write_user_header(&out);
    write_field_descriptors(&out);
    write_rows(&out);
}

fn write_magic(mut out: &File) { out.write_u32::<BigEndian>(0x44434154).unwrap(); }
fn write_version(mut out: &File) { out.write_u16::<BigEndian>(2).unwrap(); }

fn write_user_header(mut out: &File) {
    let user_header = b"example";
    out.write_u32::<BigEndian>(user_header.len() as u32).unwrap();
    out.write(user_header).unwrap();
}

fn write_field_descriptors(mut out: &File) {
    let field_count: u32 = 4;
    out.write_u32::<BigEndian>(field_count).unwrap();
    write_field_descriptor(out, "a_double", FieldType::Double, DisplayHint::None);
    write_field_descriptor(out, "an_int", FieldType::Int, DisplayHint::None);
    write_field_descriptor(out, "an_int_timestamp", FieldType::Int, DisplayHint::Timestamp);
    write_field_descriptor(out, "a_string", FieldType::String, DisplayHint::None);
}

fn write_field_descriptor(out: &File, field_name: &str, field_type: FieldType, display_hint: DisplayHint) {
    write_sized_string(out, field_name);
    write_field_type(out, field_type);
    write_field_display_hint(out, display_hint);
}

fn write_field_type(out: &File, field_type: FieldType) {
    let type_string = match field_type {
        FieldType::Double => "D",
        FieldType::Int => "I",
        FieldType::String => "Ljava.lang.String;",
    };
    write_sized_string(out, type_string);
}

fn write_sized_string(mut out: &File, string: &str) {
    let bytes = string.as_bytes();
    out.write_u32::<BigEndian>(bytes.len() as u32).unwrap();
    out.write(bytes).unwrap();
}

fn write_field_display_hint(mut out: &File, display_hint: DisplayHint) {
    let hint: i32 = match display_hint {
        DisplayHint::Timestamp => 0,
        DisplayHint::None => -1,
    };
    out.write_i32::<BigEndian>(hint).unwrap();
}

fn write_rows(out: &File) {
    let row_count = 100;
    for i in 0..row_count {
        write_row(out, i);
    }
}

fn write_row(mut out: &File, timestamp: u64) {
    let field_count: u32 = 4;

    out.write_u64::<BigEndian>(timestamp).unwrap();
    write_row_bitfield_with_no_nulls(out, field_count);

    out.write_f64::<BigEndian>(timestamp as f64).unwrap();
    out.write_u32::<BigEndian>(timestamp as u32).unwrap();
    out.write_u32::<BigEndian>(timestamp as u32).unwrap();
    write_string_value(out, &timestamp.to_string());
}

fn write_row_bitfield_with_no_nulls(mut out: &File, field_count: u32) {
    let bytes_needed: u32 = 1+((field_count-1)/8);
    let bitfield: Vec<u8> = vec![0; bytes_needed as usize];
    out.write(bitfield.as_ref()).unwrap();
}

fn write_string_value(mut out: &File, value: &str) {
    let bytes = value.as_bytes();
    if bytes.len() <= std::i16::MAX as usize {
        out.write_i16::<BigEndian>(bytes.len() as i16).unwrap();
    } else {
        out.write_i16::<BigEndian>(-1).unwrap();
        out.write_u32::<BigEndian>(bytes.len() as u32).unwrap();
    }
    out.write(bytes).unwrap();
}
