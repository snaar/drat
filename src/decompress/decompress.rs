use std::io;
use std::io::{BufReader, Read};

use byteorder::{BigEndian, ReadBytesExt};
use flate2::read::GzDecoder;
use lz_fear::LZ4FrameReader;
use paku::lz4_jblock::Lz4JBlockReader;
use paku::lzf::LzfReader;

use crate::decompress::zst::ZstReader;
use crate::error::{CliResult, Error};
use crate::util::reader::{ChopperBufPreviewer, ChopperBufReader};

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

pub fn is_compressed_using_previewer(
    previewer: &ChopperBufPreviewer<Box<dyn Read>>,
) -> Option<DecompressionFormat> {
    let buf = previewer.get_buf();
    let mut reader = BufReader::new(buf.as_ref());

    let first_four_bytes_big_endian = reader.read_u32::<BigEndian>();
    if let Ok(header32be) = first_four_bytes_big_endian {
        let header24be = header32be & 0xFFFFFF00;
        if header24be == 0x1F8B0800 {
            return Some(DecompressionFormat::GZ);
        }
        if header32be == 0x04224D18 {
            // 0x04224D18 is lz4 frame format and is most common
            return Some(DecompressionFormat::LZ4);
        }
        if header32be == 0x4C5A3442 {
            // 0x4C5A3442 is lz4 block format, which seems to be deprecated, but still in use;
            // each block starts with "LZ4Block" text, which is "4C 5A 34 42 6C 6F 63 6B"
            let next_four_bytes_big_endian = reader.read_u32::<BigEndian>();
            if let Ok(next32be) = next_four_bytes_big_endian {
                if next32be == 0x6C6F636B {
                    return Some(DecompressionFormat::LZ4);
                }
            }
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
    previewer: ChopperBufPreviewer<Box<dyn Read>>,
) -> CliResult<Box<dyn Read>> {
    match decompression_format {
        DecompressionFormat::GZ => decompress_gz(previewer.get_reader()),
        DecompressionFormat::LZ4 => decompress_lz4(previewer),
        DecompressionFormat::LZF => decompress_lzf(previewer.get_reader()),
        DecompressionFormat::ZST => decompress_zst(previewer.get_reader()),
    }
}

fn decompress_gz(reader: ChopperBufReader<Box<dyn Read>>) -> CliResult<Box<dyn Read>> {
    let decoder = GzDecoder::new(reader);
    Ok(Box::new(decoder))
}

fn decompress_lz4(previewer: ChopperBufPreviewer<Box<dyn Read>>) -> CliResult<Box<dyn Read>> {
    let buf = previewer.get_buf();
    if buf.len() >= 8 && &buf[..8] == b"LZ4Block" {
        decompress_lz4_jblock(previewer.get_reader())
    } else {
        decompress_lz4_frame(previewer.get_reader())
    }
}

fn decompress_lz4_frame(reader: ChopperBufReader<Box<dyn Read>>) -> CliResult<Box<dyn Read>> {
    let frame_reader = match LZ4FrameReader::new(reader) {
        Ok(frame_reader) => frame_reader,
        Err(e) => {
            return CliResult::Err(Error::Io(io::Error::new(io::ErrorKind::Other, e)));
        }
    };
    Ok(Box::new(frame_reader.into_read()))
}

fn decompress_lz4_jblock(reader: ChopperBufReader<Box<dyn Read>>) -> CliResult<Box<dyn Read>> {
    let decoder = Lz4JBlockReader::new(reader, true, true);
    Ok(Box::new(decoder))
}

fn decompress_lzf(reader: ChopperBufReader<Box<dyn Read>>) -> CliResult<Box<dyn Read>> {
    let decoder = LzfReader::new(reader);
    Ok(Box::new(decoder))
}

fn decompress_zst(reader: ChopperBufReader<Box<dyn Read>>) -> CliResult<Box<dyn Read>> {
    let decoder = ZstReader::new(reader)?;
    Ok(Box::new(decoder))
}
