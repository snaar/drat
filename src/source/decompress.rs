use crate::decompress::lzf::LzfReader;
use crate::decompress::zst::ZstReader;
use crate::error::{CliResult, Error};
use crate::util::preview::Preview;
use byteorder::{BigEndian, ReadBytesExt};
use flate2::read::GzDecoder;
use lz_fear::LZ4FrameReader;
use std::io;
use std::io::{BufReader, Read};

static GZ: &str = ".gz";
static LZ4: &str = ".lz4";
static LZF: &str = ".lzf";
static ZST: &str = ".zst";

pub enum DecompressionFormat {
    GZ,
    LZ4,
    LZF,
    ZST,
}

pub fn is_compressed_using_format(format: &str) -> Option<(DecompressionFormat, String)> {
    if format.ends_with(GZ) {
        return Some((
            DecompressionFormat::GZ,
            format[..(format.len() - GZ.len())].to_owned(),
        ));
    }
    if format.ends_with(LZ4) {
        return Some((
            DecompressionFormat::LZ4,
            format[..(format.len() - LZ4.len())].to_owned(),
        ));
    }
    if format.ends_with(LZF) {
        return Some((
            DecompressionFormat::LZF,
            format[..(format.len() - LZF.len())].to_owned(),
        ));
    }
    if format.ends_with(ZST) {
        return Some((
            DecompressionFormat::ZST,
            format[..(format.len() - ZST.len())].to_owned(),
        ));
    }
    None
}

pub fn is_compressed_using_previewer(previewer: &dyn Preview) -> Option<DecompressionFormat> {
    let buf = previewer.get_buf();
    let mut reader = BufReader::new(buf.as_ref());

    let first_four_bytes_big_endian = reader.read_u32::<BigEndian>();
    if let Ok(header32be) = first_four_bytes_big_endian {
        let header24be = header32be & 0xFFFFFF00;
        if header24be == 0x1F8B0800 {
            return Some(DecompressionFormat::GZ);
        }
        if header32be == 0x04224D18 {
            return Some(DecompressionFormat::LZ4);
        }
        if header24be == 0x5A560100 || header24be == 0x5A560000 {
            return Some(DecompressionFormat::LZF);
        }
        if header32be == 0x28B52FFD {
            return Some(DecompressionFormat::ZST);
        }
    }

    None
}

pub fn decompress(
    decompression_format: DecompressionFormat,
    reader: Box<dyn Read>,
) -> CliResult<Box<dyn Read>> {
    match decompression_format {
        DecompressionFormat::GZ => decompress_gz(reader),
        DecompressionFormat::LZ4 => decompress_lz4(reader),
        DecompressionFormat::LZF => decompress_lzf(reader),
        DecompressionFormat::ZST => decompress_zst(reader),
    }
}

pub fn decompress_gz(reader: Box<dyn Read>) -> CliResult<Box<dyn Read>> {
    let decoder = GzDecoder::new(reader);
    Ok(Box::new(decoder))
}

pub fn decompress_lz4(reader: Box<dyn Read>) -> CliResult<Box<dyn Read>> {
    let frame_reader = match LZ4FrameReader::new(reader) {
        Ok(frame_reader) => frame_reader,
        Err(e) => {
            return CliResult::Err(Error::Io(io::Error::new(io::ErrorKind::Other, e)));
        }
    };
    Ok(Box::new(frame_reader.into_read()))
}

pub fn decompress_lzf(reader: Box<dyn Read>) -> CliResult<Box<dyn Read>> {
    let decoder = LzfReader::new(reader);
    Ok(Box::new(decoder))
}

pub fn decompress_zst(reader: Box<dyn Read>) -> CliResult<Box<dyn Read>> {
    let decoder = ZstReader::new(reader)?;
    Ok(Box::new(decoder))
}
