use crate::decompress::lz4_jblock_decompress::lz4_jblock_decompress;
use crate::decompress::xxhash32::XXHash32;
use byteorder::{LittleEndian, ReadBytesExt};
use core::cmp;
use std::io;
use std::io::{BufRead, Error, ErrorKind, Read};

/// This reader is for files that can be read by:
/// https://github.com/lz4/lz4-java/blob/master/src/java/net/jpountz/lz4/LZ4BlockInputStream.java
/// It's unclear if anything else can actually read/write in this format, but unfortunately
/// we have to be able to read files written in this format.

// minimal one is just 64 bytes, but we will allocate 64kb - the default size Java writer uses,
// since that seems to be the most common source of the LZ4Block lz4 files
const LZ4_BLOCK_STARTING_BUF_SIZE: usize = 64 * 1024;

const DEFAULT_SEED: u32 = 0x9747b28c;

pub struct Lz4JBlockReader<R> {
    inner: R,
    buf_compressed: Vec<u8>,
    buf_decompressed: Vec<u8>,
    pos: usize,
    cap: usize,
    stop_after_empty_block: bool,
    saw_empty_block: bool,
    check_checksum: bool,
}

impl<R: Read> Lz4JBlockReader<R> {
    pub fn new(
        reader: R,
        stop_after_empty_block: bool,
        check_checksum: bool,
    ) -> Lz4JBlockReader<R> {
        Lz4JBlockReader {
            inner: reader,
            buf_compressed: vec![0; LZ4_BLOCK_STARTING_BUF_SIZE],
            buf_decompressed: vec![0; LZ4_BLOCK_STARTING_BUF_SIZE],
            pos: 0,
            cap: 0,
            stop_after_empty_block,
            saw_empty_block: false,
            check_checksum,
        }
    }

    fn fill_buf_decompressed(&mut self) -> io::Result<()> {
        assert_eq!(self.pos, self.cap);

        if self.stop_after_empty_block && self.saw_empty_block {
            self.pos = 0;
            self.cap = 0;
            return Ok(());
        }

        // looking for 'L', if there is EOF, then we are done
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
        if magic_z != b'L' {
            return Err(Error::new(ErrorKind::InvalidData, "wrong lz4 magic"));
        }

        let mut magic_leftovers = [0 as u8; 7];
        self.inner.read_exact(&mut magic_leftovers)?;
        if &magic_leftovers != b"Z4Block" {
            return Err(Error::new(ErrorKind::InvalidData, "wrong lz4 magic"));
        }

        let token = self.inner.read_u8()?;
        let compression_method = token & 0xF0;
        let compression_level = 10 + (token & 0x0F);
        let max_decompressed_buf_len = (1 as usize) << compression_level;
        let chunk_length = self.inner.read_u32::<LittleEndian>()? as usize;
        let original_length = self.inner.read_u32::<LittleEndian>()? as usize;
        let original_checksum = self.inner.read_u32::<LittleEndian>()?;

        if chunk_length == 0 {
            if original_length != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "lz4 compressed chunk is empty, but decompressed one is not",
                ));
            }
            if original_checksum != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "lz4 block is empty, but checksum is not 0",
                ));
            }

            self.saw_empty_block = true;

            return if self.stop_after_empty_block {
                self.pos = 0;
                self.cap = 0;
                Ok(())
            } else {
                self.fill_buf_decompressed()
            };
        }

        if original_length > max_decompressed_buf_len {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "lz4 decompressed buf length mismatch",
            ));
        }

        if self.buf_decompressed.capacity() < original_length {
            self.buf_decompressed.resize(max_decompressed_buf_len, 0);
        }

        match compression_method {
            0x10 => {
                // uncompressed chunk
                if original_length != chunk_length {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "lz4 uncompressed chunk length not equal original length",
                    ));
                }

                let buf_decompressed_capped = &mut self.buf_decompressed[..chunk_length];
                self.inner.read_exact(buf_decompressed_capped)?;

                self.pos = 0;
                self.cap = chunk_length;
            }
            0x20 => {
                // compressed chunk
                if self.buf_compressed.capacity() < chunk_length {
                    self.buf_compressed
                        .resize(max_decompressed_buf_len.max(chunk_length), 0);
                }

                let buf_compressed_capped = &mut self.buf_compressed[..chunk_length];
                self.inner.read_exact(buf_compressed_capped)?;

                let buf_decompressed_capped = &mut self.buf_decompressed[..original_length];
                let buf_compressed_capped = buf_compressed_capped.as_ref();

                let decompressed_length =
                    lz4_jblock_decompress(buf_compressed_capped, buf_decompressed_capped)?;
                assert_eq!(original_length, decompressed_length);

                self.pos = 0;
                self.cap = decompressed_length;
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "unknown lz4 compression method",
                ));
            }
        };

        if self.check_checksum {
            let mut xxhash32 = XXHash32::new(DEFAULT_SEED);
            xxhash32.update(&self.buf_decompressed[..self.cap]);
            let computed_checksum = xxhash32.digest() & 0x0FFFFFFFu32;

            if original_checksum != computed_checksum {
                return Err(Error::new(ErrorKind::InvalidData, "lz4 checksum mismatch"));
            }
        }

        Ok(())
    }
}

impl<R: Read> Read for Lz4JBlockReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = {
            let mut rem = self.fill_buf()?;
            rem.read(buf)?
        };
        self.consume(bytes_read);
        Ok(bytes_read)
    }
}

impl<R: Read> BufRead for Lz4JBlockReader<R> {
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
