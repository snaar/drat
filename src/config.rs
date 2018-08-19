use std::fs;
use std::io::{self, Read};
use std::ops::Deref;
use std::path::PathBuf;

use csv;
use flate2::read::GzDecoder;
use lzf;
use reqwest::{Url, Client};

#[derive(Debug)]
pub struct Config {
    path: Option<PathBuf>, // 'None' implies stdin
    delimiter: u8,
    has_headers: bool,
}

impl Config {
    pub fn new(path: &Option<&str>, delimiter: u8, has_headers: bool) -> Config {
        let path = match *path {
            None => None,
            Some(ref s) if s.deref() == "-".to_owned() => None,
            Some(ref s) => Some(PathBuf::from(s)),
        };
        Config {
            path,
            delimiter,
            has_headers,
        }
    }

    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    pub fn has_headers(&self) -> bool {
        self.has_headers
    }

    pub fn is_stdin(&self) -> bool {
        self.path.is_none()
    }

    pub fn writer(&self) -> io::Result<csv::Writer<Box<io::Write+'static>>> {
        Ok(self.from_writer(self.io_writer()?))
    }

    pub fn reader(&self) -> io::Result<csv::Reader<Box<io::Read+'static>>> {
        Ok(self.from_reader(self.io_reader()?))
    }

    pub fn get_reader_from_url(path: PathBuf) -> Box<io::BufRead+'static> {
        let url: Url = path.to_str().unwrap().parse().unwrap();
        let client = Client::new();
        let response = client.get(url).send().unwrap();
        let reader = io::BufReader::new(response);
        Box::new(reader)
    }

    pub fn read_file<R: Read+'static>(reader: R, path: &PathBuf) -> Box<io::Read+'static> {
        if path.extension().unwrap() == "gz" {
            let decoder = GzDecoder::new(reader);
            Box::new(decoder)
        } else if path.extension().unwrap() == "lzf" {
            let mut file = reader;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).unwrap();
            let decompressed = lzf::decompress(&buf[..], buf.len() * 100).unwrap();
            let cursor = io::Cursor::new(decompressed);
            Box::new(Box::new(cursor))
        } else {
            Box::new(reader)
        }
    }

    pub fn io_reader(&self) -> io::Result<Box<io::Read+'static>> {
        Ok(match self.path {
            None => Box::new(io::stdin()),
            Some(ref path) => {
                if path.starts_with("http://") {
                    let path_clone = path.clone();
                    let mut read = Self::get_reader_from_url(path_clone);
                    Self::read_file(read, path)
                } else {
                    match fs::File::open(path) {
                        Ok(x) => {
                            Self::read_file(x, path)
                        },
                        Err(err) => {
                            let msg = format!(
                                "failed to open {}: {}", path.display(), err);
                            return Err(io::Error::new(
                                io::ErrorKind::NotFound,
                                msg,
                            ));
                        }
                    }
                }
            },
        })
    }

    pub fn from_reader<R: Read>(&self, reader: R) -> csv::Reader<R> {
        csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_headers)
            .from_reader(reader)
    }

    pub fn io_writer(&self) -> io::Result<Box<io::Write+'static>> {
        Ok(match self.path {
            None => Box::new(io::stdout()),
            Some(ref path) => Box::new(fs::File::create(path)?),
        })
    }

    pub fn from_writer<W: io::Write>(&self, writer: W) -> csv::Writer<W> {
        csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(writer)
    }
}
