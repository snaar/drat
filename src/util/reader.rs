use core::{cmp, fmt};
use std::io;
use std::io::{BufRead, Error, ErrorKind, Read};

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub struct ChopperBufReader<R> {
    inner: R,
    buf: Box<[u8]>,
    pos: usize,
    cap: usize,
}

pub struct ChopperHeaderPreview<R> {
    reader: ChopperBufReader<R>,
    pub header: String,
}

impl<R: Read> ChopperHeaderPreview<R> {
    pub fn new(inner: R) -> io::Result<ChopperHeaderPreview<R>> {
        let mut reader = ChopperBufReader::with_capacity(DEFAULT_BUF_SIZE, inner);
        let header_end = ChopperHeaderPreview::fill_buffer_until_newline(&mut reader)?;
        //TODO fix unwrap and error handling in this impl in general
        let header = std::str::from_utf8(&reader.buf[0..header_end])
            .unwrap()
            .to_string();

        Ok(ChopperHeaderPreview { reader, header })
    }

    pub fn rewind_and_get_reader(self) -> ChopperBufReader<R> {
        self.reader
    }

    fn fill_buffer_until_newline(reader: &mut ChopperBufReader<R>) -> io::Result<usize> {
        debug_assert!(reader.pos == 0);
        debug_assert!(reader.cap == 0);

        let header_end = loop {
            if reader.cap == reader.buf.len() {
                //TODO add new specific error for this case?
                return Err(Error::new(
                    ErrorKind::Other,
                    "internal buffer full while waiting for first line to finish reading; \
                    try disabling cvs input format auto-detection \
                    by specifying delimiter and header presence explicitly",
                ));
            }

            let rc = reader.inner.read(&mut reader.buf[reader.cap..])?;
            if rc == 0 {
                break reader.cap;
            }

            let old_cap = reader.cap;
            reader.cap += rc;
            debug_assert!(reader.cap <= reader.buf.len());

            match reader.buf[old_cap..reader.cap]
                .iter()
                .position(|&c| c == b'\n')
            {
                None => {}
                Some(p) => {
                    break old_cap + p;
                }
            }
        };

        Ok(header_end)
    }
}

impl<R: Read> ChopperBufReader<R> {
    pub fn new(inner: R) -> ChopperBufReader<R> {
        ChopperBufReader::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(capacity: usize, inner: R) -> ChopperBufReader<R> {
        let buf: Vec<u8> = vec![0; capacity];

        ChopperBufReader {
            inner,
            buf: buf.into_boxed_slice(),
            pos: 0,
            cap: 0,
        }
    }
}

impl<R> ChopperBufReader<R> {
    #[inline]
    fn discard_buffer(&mut self) {
        self.pos = 0;
        self.cap = 0;
    }
}

impl<R: Read> Read for ChopperBufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.pos == self.cap && buf.len() >= self.buf.len() {
            self.discard_buffer();
            return self.inner.read(buf);
        }
        let nread = {
            let mut rem = self.fill_buf()?;
            rem.read(buf)?
        };
        self.consume(nread);
        Ok(nread)
    }
}

impl<R: Read> BufRead for ChopperBufReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        // Branch using `>=` instead of the more correct `==`
        // to tell the compiler that the pos..cap slice is always valid.
        if self.pos >= self.cap {
            debug_assert!(self.pos == self.cap);
            self.cap = self.inner.read(&mut self.buf)?;
            self.pos = 0;
        }
        Ok(&self.buf[self.pos..self.cap])
    }

    fn consume(&mut self, amt: usize) {
        self.pos = cmp::min(self.pos + amt, self.cap);
    }
}

impl<R: fmt::Debug> fmt::Debug for ChopperHeaderPreview<R> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ChopperHeaderPreview")
            .field("reader", &self.reader)
            .field("header", &self.header)
            .finish()
    }
}

impl<R: fmt::Debug> fmt::Debug for ChopperBufReader<R> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ChopperBufReader")
            .field("reader", &self.inner)
            .field(
                "buffer",
                &format_args!("{}/{}", self.cap - self.pos, self.buf.len()),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read};

    use crate::util::reader::{ChopperBufReader, ChopperHeaderPreview};

    const TEST_BYTES: &[u8] = "aaaaa\nbbbbb\nccccc".as_bytes();

    #[test]
    fn test_capacity_too_small() {
        let inner = BufReader::new(TEST_BYTES);
        let mut reader = ChopperBufReader::with_capacity(5, inner);
        let result = ChopperHeaderPreview::fill_buffer_until_newline(&mut reader);

        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_capacities() {
        for capacity in 6..20 {
            test_normal_with_capacity(capacity);
        }
    }

    fn test_normal_with_capacity(capacity: usize) {
        let inner = BufReader::new(TEST_BYTES);
        let mut reader = ChopperBufReader::with_capacity(capacity, inner);
        let result = ChopperHeaderPreview::fill_buffer_until_newline(&mut reader);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);

        check_line(&mut reader, 6, "aaaaa\n");
        check_line(&mut reader, 6, "bbbbb\n");
        check_line(&mut reader, 5, "ccccc");
        check_line(&mut reader, 0, "");
    }

    fn check_line<R: Read>(
        reader: &mut ChopperBufReader<R>,
        expect_result: usize,
        expect_line: &str,
    ) {
        let mut line = String::new();
        let result = reader.read_line(&mut line);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expect_result);
        assert_eq!(line, expect_line);
    }
}
