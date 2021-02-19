use std::hash::Hasher;
use std::io::{self, Read};

pub struct Crc32Reader<R: Read> {
    reader: R,
    crc32: u32,
    hasher: crc32fast::Hasher,
}

impl<R: Read> Crc32Reader<R> {
    pub fn new(reader: R, expected_crc32: u32) -> Crc32Reader<R> {
        Crc32Reader {
            reader,
            crc32: expected_crc32,
            hasher: crc32fast::Hasher::new(),
        }
    }
}

impl<R: Read> Read for Crc32Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = match self.reader.read(buf) {
            Ok(bytes_read) => {
                if bytes_read == 0 && !buf.is_empty() {
                    if self.hasher.finish() != (self.crc32 as u64) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "crc32 check failed",
                        ));
                    }
                }
                bytes_read
            }
            Err(e) => return Err(e),
        };
        self.hasher.update(&buf[0..bytes_read]);
        Ok(bytes_read)
    }
}
