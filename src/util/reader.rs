use crate::util::preview::Preview;
use core::{cmp, fmt};
use std::io;
use std::io::{BufRead, Error, ErrorKind, Read};
use std::str::Utf8Error;

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

//TODO: figure out how to split this into more files; tricky part is private
// ChopperBufReader.buf field being used by ChopperBufPreviewer impl

pub struct ChopperBufReader<R> {
    inner: R,
    buf: Box<[u8]>,
    pos: usize,
    cap: usize,
}

pub struct ChopperBufPreviewer<R> {
    reader: ChopperBufReader<R>,
    /// if present, then file had well-formed utf8 strings
    /// it will only have complete strings, if string was cut off then it will not be present
    lines: Option<Vec<String>>,
}

impl<R: 'static + Read> Preview for ChopperBufPreviewer<R> {
    fn get_lines(&self) -> &Option<Vec<String>> {
        &self.lines
    }

    fn get_reader(self: Box<Self>) -> Box<dyn Read> {
        Box::new(self.reader)
    }
}

impl<R: Read> ChopperBufPreviewer<R> {
    pub fn new(inner: R) -> io::Result<ChopperBufPreviewer<R>> {
        let reader = ChopperBufReader::with_capacity(DEFAULT_BUF_SIZE, inner);
        let mut previewer = ChopperBufPreviewer {
            reader,
            lines: None,
        };

        previewer.prepare_preview()?;

        Ok(previewer)
    }

    pub fn get_reader(self) -> ChopperBufReader<R> {
        self.reader
    }

    fn prepare_preview(&mut self) -> io::Result<()> {
        Self::fill_buffer(&mut self.reader)?;

        // we want to make all the lines we find in the filled buffer available for easy
        // preview individually
        //
        // last line is treated specially, as covered later
        //
        // for all the other lines, we make sure that parsing any of them produce no Utf8Error;
        // if Utf8Error happens then we conclude that file is not a text file and so should not
        // be parsed into lines; we put None into lines field
        //
        // for the last line, if cached data is smaller that full buffer size (i.e. file is
        // smaller than the buffer) then line is processed same as above;
        // if last line does not end before the buffer ends, then it's discarded

        self.lines = match Self::get_lines(&self.reader) {
            Ok(lines) => Some(lines),
            Err(_) => None,
        };

        Ok(())
    }

    fn get_lines(reader: &ChopperBufReader<R>) -> Result<Vec<String>, Utf8Error> {
        let mut lines: Vec<String> = Vec::new();
        let mut next_line_start = 0;
        loop {
            if next_line_start >= reader.cap {
                break;
            }
            next_line_start =
                match Self::add_next_line_to_lines(reader, next_line_start, &mut lines) {
                    Ok(Some(next_line_start)) => next_line_start,
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                };
        }
        Ok(lines)
    }

    fn add_next_line_to_lines(
        reader: &ChopperBufReader<R>,
        next_line_start: usize,
        lines: &mut Vec<String>,
    ) -> Result<Option<usize>, Utf8Error> {
        let next_line = Self::get_line(&reader.buf, next_line_start, reader.cap);
        match next_line {
            None => Ok(None),
            Some(result) => match result {
                Ok((line, next_line_start)) => {
                    lines.push(line);
                    Ok(Some(next_line_start))
                }
                Err(e) => Err(e),
            },
        }
    }

    /// returns None if ran out of buffer trying to find line end;
    /// return result of converting part of buffer to line if found and start of next line,
    /// which can be a Utf8Error
    fn get_line(
        buf: &[u8],
        start: usize,
        cap: usize,
    ) -> Option<Result<(String, usize), Utf8Error>> {
        let end = match buf[start..cap].iter().position(|&c| c == b'\n') {
            None => {
                // we didn't find any newline characters
                if cap >= buf.len() {
                    // we also had buffer fully loaded and we got to the end of it,
                    // which means it's inconclusive if line ended or not, so have to report None
                    return None;
                } else {
                    // got to the end of filled space in buffer, without running out of buffer,
                    // so we can assume it's a line without explicit line end character
                    cap
                }
            }
            Some(p) => start + p,
        };

        // no overflow, since we know that end has to be less than buf.len()
        let next_start = end + 1;

        let result = match Self::make_string_no_cr(&buf[start..end]) {
            Ok(s) => Ok((s, next_start)),
            Err(e) => Err(e),
        };
        Some(result)
    }

    /// get rid of terminal '\r' if present
    fn make_string_no_cr(slice: &[u8]) -> Result<String, Utf8Error> {
        if slice.len() == 0 {
            return Ok(String::new());
        }

        let end = if slice[slice.len() - 1] == b'\r' {
            slice.len() - 1
        } else {
            slice.len()
        };

        match std::str::from_utf8(&slice[..end]) {
            Ok(s) => Ok(s.to_string()),
            Err(e) => Err(e),
        }
    }

    fn fill_buffer(reader: &mut ChopperBufReader<R>) -> io::Result<()> {
        debug_assert!(reader.pos == 0);
        debug_assert!(reader.cap == 0);

        loop {
            let bytes_read = Self::fill_buffer_one_shot(reader)?;
            if bytes_read == 0 || reader.cap == reader.buf.len() {
                break;
            }
        }

        Ok(())
    }

    fn fill_buffer_one_shot(reader: &mut ChopperBufReader<R>) -> io::Result<usize> {
        if reader.cap == reader.buf.len() {
            return Err(Error::new(
                ErrorKind::Other,
                "asked to fill buffer when it was already at capacity",
            ));
        }

        let bytes_read = reader.inner.read(&mut reader.buf[reader.cap..])?;
        reader.cap += bytes_read;
        debug_assert!(reader.cap <= reader.buf.len());
        Ok(bytes_read)
    }
}

impl<R: Read> ChopperBufReader<R> {
    pub fn new(inner: R) -> ChopperBufReader<R> {
        Self::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(capacity: usize, inner: R) -> ChopperBufReader<R> {
        assert!(capacity > 0);

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
            .field("lines", &self.lines)
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

    use crate::util::preview::Preview;
    use crate::util::reader::{ChopperBufPreviewer, ChopperBufReader};

    const TEST_BYTES: &[u8] = "aaaaa\nbbbbb\nccccc".as_bytes();

    #[test]
    fn test_capacity_too_small() {
        let inner = BufReader::new(TEST_BYTES);
        let mut reader = ChopperBufReader::with_capacity(5, inner);
        let result = ChopperBufPreviewer::fill_buffer(&mut reader);

        assert!(result.is_ok());
        assert_eq!(reader.cap, reader.buf.len());
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
        let result = ChopperBufPreviewer::fill_buffer(&mut reader);

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
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn test_preview_1_1() {
        let inner = BufReader::new("\n".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines.get(0).unwrap(), "");
    }

    #[test]
    fn test_preview_1_2() {
        let inner = BufReader::new("\r\n".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines.get(0).unwrap(), "");
    }

    #[test]
    fn test_preview_1_3() {
        let inner = BufReader::new("zzz\n".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines.get(0).unwrap(), "zzz");
    }

    #[test]
    fn test_preview_1_4() {
        let inner = BufReader::new("zzz\r\n".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines.get(0).unwrap(), "zzz");
    }

    #[test]
    fn test_preview_1_5() {
        let inner = BufReader::new("z".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines.get(0).unwrap(), "z");
    }

    #[test]
    fn test_preview_2_1() {
        let inner = BufReader::new("zzz\n\r\n".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines.get(0).unwrap(), "zzz");
        assert_eq!(lines.get(1).unwrap(), "");
    }

    #[test]
    fn test_preview_2_2() {
        let inner = BufReader::new("zzz\r\nxxx".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines.get(0).unwrap(), "zzz");
        assert_eq!(lines.get(1).unwrap(), "xxx");
    }

    #[test]
    fn test_preview_2_3() {
        let inner = BufReader::new("zzz\nxxx\rx\n".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines.get(0).unwrap(), "zzz");
        assert_eq!(lines.get(1).unwrap(), "xxx\rx");
    }

    #[test]
    fn test_preview_2_4() {
        let inner = BufReader::new("zzz\r\nxxx\nxxx".as_bytes());
        let previewer = ChopperBufPreviewer::new(inner).unwrap();
        assert!(previewer.lines.is_some());
        let lines = previewer.lines.unwrap();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines.get(0).unwrap(), "zzz");
        assert_eq!(lines.get(1).unwrap(), "xxx");
        assert_eq!(lines.get(2).unwrap(), "xxx");
    }

    fn setup_test_preview_3(capacity: usize) -> Box<dyn Preview> {
        let inner = BufReader::new("zzz\nxxx\nyyy".as_bytes());
        let reader = ChopperBufReader::with_capacity(capacity, inner);
        let mut previewer = ChopperBufPreviewer {
            reader,
            lines: None,
        };
        previewer.prepare_preview().unwrap();
        Box::new(previewer)
    }

    #[test]
    fn test_preview_3_1_3() {
        for capacity in 1..=3 {
            inner_test_preview_3_1_3(capacity);
        }
    }

    fn inner_test_preview_3_1_3(capacity: usize) {
        let previewer = setup_test_preview_3(capacity);
        let lines = previewer.get_lines();
        assert!(lines.is_some());
        let lines = lines.as_ref().unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn test_preview_3_4_7() {
        for capacity in 4..=7 {
            inner_test_preview_3_4_7(capacity);
        }
    }

    fn inner_test_preview_3_4_7(capacity: usize) {
        let previewer = setup_test_preview_3(capacity);
        let lines = previewer.get_lines();
        assert!(lines.is_some());
        let lines = lines.as_ref().unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines.get(0).unwrap(), "zzz");
    }

    #[test]
    fn test_preview_3_8_11() {
        for capacity in 8..=11 {
            inner_test_preview_3_8_11(capacity);
        }
    }

    fn inner_test_preview_3_8_11(capacity: usize) {
        let previewer = setup_test_preview_3(capacity);
        let lines = previewer.get_lines();
        assert!(lines.is_some());
        let lines = lines.as_ref().unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines.get(0).unwrap(), "zzz");
        assert_eq!(lines.get(1).unwrap(), "xxx");
    }

    #[test]
    fn test_preview_3_12_20() {
        for capacity in 12..=20 {
            inner_test_preview_3_12_20(capacity);
        }
    }

    fn inner_test_preview_3_12_20(capacity: usize) {
        let previewer = setup_test_preview_3(capacity);
        let lines = previewer.get_lines();
        assert!(lines.is_some());
        let lines = lines.as_ref().unwrap();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines.get(0).unwrap(), "zzz");
        assert_eq!(lines.get(1).unwrap(), "xxx");
        assert_eq!(lines.get(2).unwrap(), "yyy");
    }
}
