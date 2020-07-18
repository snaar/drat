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

pub struct ChopperBufPreviewer<R> {
    reader: ChopperBufReader<R>,
    lines_populated: bool,
    pub line1: Option<String>,
    pub line2: Option<String>,
}

impl<R: Read> ChopperBufPreviewer<R> {
    pub fn new(inner: R) -> io::Result<ChopperBufPreviewer<R>> {
        let reader = ChopperBufReader::with_capacity(DEFAULT_BUF_SIZE, inner);

        Ok(ChopperBufPreviewer {
            reader,
            lines_populated: false,
            line1: None,
            line2: None,
        })
    }

    pub fn populate_lines_idempotent(&mut self) -> io::Result<()> {
        if self.lines_populated {
            return Ok(());
        }
        self.lines_populated = true;

        Self::fill_buffer_until_n_newlines(&mut self.reader, 2)?;
        let (line1, line2) = Self::get_first_two_lines(&self.reader);
        self.line1 = line1;
        self.line2 = line2;

        Ok(())
    }

    pub fn get_reader(self) -> ChopperBufReader<R> {
        self.reader
    }

    fn get_first_two_lines(reader: &ChopperBufReader<R>) -> (Option<String>, Option<String>) {
        let (line1, line2_start) = Self::get_line(&reader.buf, 0, reader.cap);
        if line2_start == reader.cap {
            return (line1, None);
        }

        let (line2, _) = Self::get_line(&reader.buf, line2_start, reader.cap);

        (line1, line2)
    }

    /// returns line if found and start of next line
    fn get_line(buf: &[u8], start: usize, cap: usize) -> (Option<String>, usize) {
        if start == cap {
            return (None, cap);
        };

        let (end, next_start) = match buf[start..cap].iter().position(|&c| c == b'\n') {
            None => (cap, cap),
            Some(p) => (start + p, start + p + 1),
        };

        (Some(Self::make_string_no_cr(&buf[start..end])), next_start)
    }

    /// get rid of terminal '\r' if present
    fn make_string_no_cr(slice: &[u8]) -> String {
        if slice.len() == 0 {
            return String::new();
        }

        let end = if slice[slice.len() - 1] == b'\r' {
            slice.len() - 1
        } else {
            slice.len()
        };

        std::str::from_utf8(&slice[..end]).unwrap().to_string()
    }

    /// will try to read as much as needed until request number of lines is seen;
    /// it's possible to return successfully without seeing enough lines, if file is too short;
    /// the caller needs to check how many lines are actually in the buffer and also consider
    /// the case of last line in file not having a newline character
    fn fill_buffer_until_n_newlines(
        reader: &mut ChopperBufReader<R>,
        required_newline_count: usize,
    ) -> io::Result<()> {
        debug_assert!(reader.pos == 0);
        debug_assert!(reader.cap == 0);

        let mut total_newline_count = 0;
        loop {
            let (newline_count, bytes_read) = Self::fill_buffer_and_count_newlines(reader)?;

            total_newline_count += newline_count;

            if bytes_read == 0 || total_newline_count >= required_newline_count {
                break;
            }
        }

        Ok(())
    }

    /// when using this function, keep in mind files that do not end in a newline,
    /// i.e. even if you get no newlines, it's possible there is one more line available;
    /// "Ok" return of 0 bytes read means we ran out of data from underlying reader,
    /// but did not run out of our buffer - in this case the caller needs to check if there
    /// is any data after the last newline and treat it as additional line
    fn fill_buffer_and_count_newlines(
        reader: &mut ChopperBufReader<R>,
    ) -> io::Result<(usize, usize)> {
        if reader.cap == reader.buf.len() {
            return Err(Error::new(
                ErrorKind::Other,
                "internal buffer full while pre-reading beginning of input file \
                required for csv input format auto-detection to work; try disabling the \
                auto-detection by specifying delimiter and header presence explicitly",
            ));
        }

        let bytes_read = reader.inner.read(&mut reader.buf[reader.cap..])?;
        if bytes_read == 0 {
            return Ok((0, 0));
        }

        let old_cap = reader.cap;
        reader.cap += bytes_read;
        debug_assert!(reader.cap <= reader.buf.len());

        let newlines_count = reader.buf[old_cap..reader.cap]
            .iter()
            .filter(|&c| *c == b'\n')
            .count();

        Ok((newlines_count, bytes_read))
    }
}

impl<R: Read> ChopperBufReader<R> {
    pub fn new(inner: R) -> ChopperBufReader<R> {
        Self::with_capacity(DEFAULT_BUF_SIZE, inner)
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

impl<R: fmt::Debug> fmt::Debug for ChopperBufPreviewer<R> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ChopperHeaderPreview")
            .field("reader", &self.reader)
            .field("header", &self.line1)
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

    use crate::util::reader::{ChopperBufPreviewer, ChopperBufReader};

    const TEST_BYTES: &[u8] = "aaaaa\nbbbbb\nccccc".as_bytes();

    #[test]
    fn test_capacity_too_small() {
        let inner = BufReader::new(TEST_BYTES);
        let mut reader = ChopperBufReader::with_capacity(5, inner);
        let result = ChopperBufPreviewer::fill_buffer_until_n_newlines(&mut reader, 1);

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
        let result = ChopperBufPreviewer::fill_buffer_until_n_newlines(&mut reader, 1);

        assert!(result.is_ok());

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

    #[test]
    fn test_preview_empty() {
        let inner = BufReader::new("".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_none());
        assert!(preview.line2.is_none());
    }

    #[test]
    fn test_preview_1_1() {
        let inner = BufReader::new("\n".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_none());
        assert_eq!(preview.line1.unwrap(), "");
    }

    #[test]
    fn test_preview_1_2() {
        let inner = BufReader::new("\r\n".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_none());
        assert_eq!(preview.line1.unwrap(), "");
    }

    #[test]
    fn test_preview_1_3() {
        let inner = BufReader::new("zzz\n".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_none());
        assert_eq!(preview.line1.unwrap(), "zzz");
    }

    #[test]
    fn test_preview_1_4() {
        let inner = BufReader::new("zzz\r\n".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_none());
        assert_eq!(preview.line1.unwrap(), "zzz");
    }

    #[test]
    fn test_preview_1_5() {
        let inner = BufReader::new("z".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_none());
        assert_eq!(preview.line1.unwrap(), "z");
    }

    #[test]
    fn test_preview_2_1() {
        let inner = BufReader::new("zzz\n\r\n".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_some());
        assert_eq!(preview.line1.unwrap(), "zzz");
        assert_eq!(preview.line2.unwrap(), "");
    }

    #[test]
    fn test_preview_2_2() {
        let inner = BufReader::new("zzz\r\nxxx".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_some());
        assert_eq!(preview.line1.unwrap(), "zzz");
        assert_eq!(preview.line2.unwrap(), "xxx");
    }

    #[test]
    fn test_preview_2_3() {
        let inner = BufReader::new("zzz\nxxx\rx\n".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_some());
        assert_eq!(preview.line1.unwrap(), "zzz");
        assert_eq!(preview.line2.unwrap(), "xxx\rx");
    }

    #[test]
    fn test_preview_2_4() {
        let inner = BufReader::new("zzz\r\nxxx\nxxx".as_bytes());
        let mut preview = ChopperBufPreviewer::new(inner).unwrap();
        preview.populate_lines_idempotent().unwrap();
        assert!(preview.line1.is_some());
        assert!(preview.line2.is_some());
        assert_eq!(preview.line1.unwrap(), "zzz");
        assert_eq!(preview.line2.unwrap(), "xxx");
    }
}
