use std::fs;
use std::io::{self, Read};
use std::ops::Deref;
use std::path::PathBuf;

use csv;
use flate2::read::GzDecoder;
use lzf;
use reqwest::{Url, Client};

use read::dr;
use read::{dc_reader, csv_reader};

#[derive(Debug)]
pub struct Config {
    path: Option<PathBuf>, // 'None' implies stdin
    delimiter: u8,
    has_headers: bool,
    timestamp_column: usize,
    file_type: FileType,
}

#[derive(Debug)]
enum FileType {
    Csv,
    Dc,
}

impl Config {
    pub fn new(path: &Option<&str>, delimiter: u8, has_headers: bool, timestamp_column: usize) -> Config {
        let path = match *path {
            None => None,
            Some(ref s) if s.deref() == "-".to_owned() => None,
            Some(ref s) => Some(PathBuf::from(s)),
        };
        let file_type = FileType::Csv;
        Config {
            path,
            delimiter,
            has_headers,
            timestamp_column,
            file_type,
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

    pub fn reader(&mut self) -> io::Result<Box<dr::Reader+'static>> {
        let io_reader = self.io_reader()?;
        Ok(self.from_reader(io_reader)?)
    }

    pub fn get_reader_from_url(path: PathBuf) -> Box<io::BufRead+'static> {
        let url: Url = path.to_str().unwrap().parse().unwrap();
        let client = Client::new();
        let response = client.get(url).send().unwrap();
        let reader = io::BufReader::new(response);
        Box::new(reader)
    }

    pub fn get_file_reader<R: Read+'static>(&mut self, reader: R, path: &PathBuf) -> Box<io::Read+'static> {
        //TODO make .csv and .dc independent from .gz and .lzf
        // i.e. support: .csv, .csv.gz, .csv.lzf, .dc, .dc.gz, .dc.lzf
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
        } else if path.extension().unwrap() == "dc" {
            self.file_type = FileType::Dc;
            Box::new(reader)
        } else {
            Box::new(reader)
        }
    }

    pub fn io_reader(&mut self) -> io::Result<Box<io::Read+'static>> {
        let path = self.path.clone();
        Ok(match path {
            None => Box::new(io::stdin()),
            Some(ref p) => {
                if p.starts_with("http://") {
                    let path_clone = p.clone();
                    let mut read = Self::get_reader_from_url(path_clone);
                    self.get_file_reader(read, p)
                } else {
                    match fs::File::open(p) {
                        Ok(r) => {
                            self.get_file_reader( r, p)
                        },
                        Err(err) => {
                            let msg = format!(
                                "failed to open {}: {}", p.display(), err);
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

    pub fn from_reader<R: Read+'static>(&mut self, reader: R) -> io::Result<Box<dr::Reader+'static>> {
        match self.file_type {
            FileType::Csv => {
                let mut csv_reader_arg = csv::ReaderBuilder::new()
                    .delimiter(self.delimiter)
                    .has_headers(self.has_headers)
                    .from_reader(reader);
                Ok(Box::new(csv_reader::CSVReader::new(csv_reader_arg, 0)))

            },
            FileType::Dc => {
                Ok(Box::new(dc_reader::DCReader::new(reader)))
            }
        }
    }
}
