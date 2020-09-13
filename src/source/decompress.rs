use std::io;

use flate2::read::GzDecoder;

use crate::decompress::lzf::LzfReader;
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

    let first_four_bytes_big_endian = reader.read_u32::<BigEndian>();
    if let Ok(header) = first_four_bytes_big_endian {
        let header24be = header & 0xFFFFFF00;
        if header24be == 0x1F8B0800 {
            return Some(DecompressionFormat::GZ);
        }
        if header24be == 0x5A560100 || header24be == 0x5A560000 {
            return Some(DecompressionFormat::LZF);
        }
    }

    None
}

pub fn decompress_gz(reader: Box<dyn io::Read>) -> CliResult<Box<dyn io::Read>> {
    let decoder = GzDecoder::new(reader);
    Ok(Box::new(decoder))
}

pub fn decompress_lzf(reader: Box<dyn io::Read>) -> CliResult<Box<dyn io::Read>> {
    let decoder = LzfReader::new(reader);
    Ok(Box::new(decoder))
}
