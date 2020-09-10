use std::io;

use flate2::read::GzDecoder;
use lzf;

use crate::error::CliResult;
use crate::util::preview::Preview;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::BufReader;

static GZ: &str = ".gz";
static LZF: &str = ".lzf";

pub enum DecompressionFormat {
    GZ,
    LZF,
}

pub fn is_compressed_gz(format: &str) -> bool {
    format.ends_with(GZ)
}

pub fn is_compressed_lzf(format: &str) -> bool {
    format.ends_with(LZF)
}

pub fn is_compressed_using_format(format: &str) -> Option<(DecompressionFormat, String)> {
    if is_compressed_gz(format) {
        return Some((
            DecompressionFormat::GZ,
            format[..(format.len() - GZ.len())].to_owned(),
        ));
    }
    if is_compressed_lzf(format) {
        return Some((
            DecompressionFormat::LZF,
            format[..(format.len() - LZF.len())].to_owned(),
        ));
    }
    None
}

pub fn is_compressed_using_previewer(previewer: &dyn Preview) -> Option<DecompressionFormat> {
    let buf = previewer.get_buf();
    let mut reader = BufReader::new(buf.as_ref());

    if let Ok(gz_header) = reader.read_u32::<BigEndian>() {
        if gz_header & 0xFFFFFF00 == 0x1F8B0800 {
            return Some(DecompressionFormat::GZ);
        }
    }
    None
}

pub fn decompress_gz(reader: Box<dyn io::Read>) -> CliResult<Box<dyn io::Read>> {
    let decoder = GzDecoder::new(reader);
    Ok(Box::new(decoder))
}

pub fn decompress_lzf(reader: Box<dyn io::Read>) -> CliResult<Box<dyn io::Read>> {
    //TODO: make this streaming #24
    let mut file = reader;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let decompressed = lzf::decompress(&buf[..], buf.len() * 100).unwrap();
    let cursor = io::Cursor::new(decompressed);

    Ok(Box::new(cursor))
}
