use ruzstd::{BlockDecodingStrategy, FrameDecoder};
use std::io::{self, Read};

pub struct ZstReader {
    inner: Box<dyn Read>,
    decoder: FrameDecoder,
}

impl ZstReader {
    pub fn new(reader: Box<dyn Read>) -> io::Result<ZstReader> {
        let mut zst_reader = ZstReader {
            inner: reader,
            decoder: FrameDecoder::new(),
        };
        zst_reader.init()?;
        Ok(zst_reader)
    }

    fn init(&mut self) -> io::Result<()> {
        match self.decoder.init(self.inner.as_mut()) {
            Ok(o) => Ok(o),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}

impl Read for ZstReader {
    /// this impl works best with buffered reader on top of it, since it
    /// tries to fill the buf as much as possible by potentially decoding
    /// on every invocation
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while !self.decoder.is_finished() {
            let bytes_available = self.decoder.can_collect();
            if bytes_available >= buf.len() {
                break;
            }

            let additional_bytes_needed = buf.len() - bytes_available;
            if let Err(e) = self.decoder.decode_blocks(
                self.inner.as_mut(),
                BlockDecodingStrategy::UptoBytes(additional_bytes_needed),
            ) {
                return Err(io::Error::new(io::ErrorKind::Other, e));
            }
        }

        self.decoder.read(buf)
    }
}
