use std::io;
use std::path::PathBuf;

use flate2::read::GzDecoder;
use lzf;

use crate::error::{CliResult, Error};

pub fn is_compressed(path: &PathBuf) -> bool {
    let extension = path.extension().unwrap().to_str().unwrap();
    extension.eq("gz") || extension.eq("lzf")
}

pub fn decompress(path: &PathBuf, reader: Box<io::Read>) -> CliResult<Box<io::Read+'static>> {
    match path.extension().unwrap().to_str().unwrap() {
        "gz" => {
            let decoder = GzDecoder::new(reader);
            Ok(Box::new(decoder))
        },
        "lzf" => {
            let mut file = reader;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let decompressed = lzf::decompress(&buf[..], buf.len() * 100).unwrap();
            let cursor = io::Cursor::new(decompressed);
            Ok(Box::new(cursor))
        },
        _ => Err(Error::from(format!("Cannot decompress file - {:?}", path)))
    }
}
