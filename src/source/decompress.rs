use std::io;

use flate2::read::GzDecoder;
use lzf;

use crate::error::{CliResult, Error};

static GZ: &str = ".gz";
static LZF: &str = ".lzf";

pub fn is_compressed(format: &str) -> bool {
    format.ends_with(GZ) || format.ends_with(LZF)
}

pub fn decompress(
    format: &str,
    reader: Box<dyn io::Read>,
) -> CliResult<(Box<dyn io::Read>, String)> {
    if format.ends_with(GZ) {
        let decoder = GzDecoder::new(reader);

        let leftover_format = format[..(format.len() - GZ.len())].to_owned();

        Ok((Box::new(decoder), leftover_format))
    } else if format.ends_with(LZF) {
        let mut file = reader;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let decompressed = lzf::decompress(&buf[..], buf.len() * 100).unwrap();
        let cursor = io::Cursor::new(decompressed);

        let leftover_format = format[..(format.len() - LZF.len())].to_owned();

        Ok((Box::new(cursor), leftover_format))
    } else {
        Err(Error::from(format!(
            "Cannot decompress; unknown file format: {:?}",
            format
        )))
    }
}
