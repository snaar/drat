use crate::decompress::lzf_decompress::lzf_decompress;
use byteorder::{BigEndian, ReadBytesExt};
use core::cmp;
use std::io;
use std::io::{BufRead, Error, ErrorKind, Read};

// LZF format specs says only 2 bytes to specify either of buffer sizes
const LZF_BUF_SIZE: usize = 64 * 1024;

pub struct LzfReader<R> {
    inner: R,
    buf_compressed: Box<[u8]>,
    buf_decompressed: Box<[u8]>,
    pos: usize,
    cap: usize,
}

impl<R: Read> LzfReader<R> {
    pub fn new(reader: R) -> LzfReader<R> {
        LzfReader {
            inner: reader,
            buf_compressed: vec![0; LZF_BUF_SIZE].into_boxed_slice(),
            buf_decompressed: vec![0; LZF_BUF_SIZE].into_boxed_slice(),
            pos: 0,
            cap: 0,
        }
    }

    fn fill_buf_decompressed(&mut self) -> io::Result<()> {
        assert_eq!(self.pos, self.cap);

        // looking for 'Z', if there is EOF, then we are done
        let magic_z = match self.inner.read_u8() {
            Ok(b) => b,
            Err(err) => {
                return if err.kind() == ErrorKind::UnexpectedEof {
                    self.pos = 0;
                    self.cap = 0;
                    Ok(())
                } else {
                    Err(err)
                }
            }
        };
        if magic_z != b'Z' {
            return Err(Error::new(ErrorKind::InvalidData, "wrong lzf magic"));
        }

        let magic_v = self.inner.read_u8()?;
        if magic_v != b'V' {
            return Err(Error::new(ErrorKind::InvalidData, "wrong lzf magic"));
        }

        let chunk_type = self.inner.read_u8()?;
        match chunk_type {
            0 => {
                // uncompressed chunk
                let chunk_length = self.inner.read_u16::<BigEndian>()? as usize;
                let buf_decompressed_capped = &mut self.buf_decompressed[..chunk_length];
                self.inner.read_exact(buf_decompressed_capped)?;

                self.pos = 0;
                self.cap = chunk_length;
            }
            1 => {
                // compressed chunk
                let chunk_length = self.inner.read_u16::<BigEndian>()? as usize;
                let original_length = self.inner.read_u16::<BigEndian>()? as usize;

                let buf_compressed_capped = &mut self.buf_compressed[..chunk_length];
                self.inner.read_exact(buf_compressed_capped)?;

                let decompressed_length =
                    lzf_decompress(buf_compressed_capped, self.buf_decompressed.as_mut())?;
                assert_eq!(original_length, decompressed_length);

                self.pos = 0;
                self.cap = decompressed_length;
            }
            _ => {
                return Err(Error::new(ErrorKind::InvalidData, "unknown lzf chunk type"));
            }
        };

        Ok(())
    }
}

impl<R: Read> Read for LzfReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = {
            let mut rem = self.fill_buf()?;
            rem.read(buf)?
        };
        self.consume(bytes_read);
        Ok(bytes_read)
    }
}

impl<R: Read> BufRead for LzfReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // If we've reached the end of our internal buffer then we need to fetch
        // some more data from the underlying reader.
        // Branch using `>=` instead of the more correct `==`
        // to tell the compiler that the pos..cap slice is always valid.
        if self.pos >= self.cap {
            self.fill_buf_decompressed()?;
        }
        Ok(&self.buf_decompressed[self.pos..self.cap])
    }

    fn consume(&mut self, amt: usize) {
        self.pos = cmp::min(self.pos + amt, self.cap);
    }
}
