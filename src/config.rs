use std::io::{self, Read};
use std::ops::Deref;
use std::path::PathBuf;

use csv;
use flate2::read::GzDecoder;
use lzf;

use crate::input::input_factory::InputFactory;
use crate::read::dr;
use crate::read::{dc_reader, csv_reader};

#[derive(Debug)]
pub struct Config {
    path: Option<PathBuf>, // 'None' implies stdin
    delimiter: u8,
    has_headers: bool,
    timestamp_column: usize,
    file_type: FileType,
    input_factories: Vec<Box<InputFactory>>,
}

#[derive(Debug)]
enum FileType {
    Csv,
    Dc,
}

impl Config {
    pub fn new(path: &Option<&str>, delimiter: u8, has_headers: bool,
               timestamp_column: usize, input_factories: Vec<Box<InputFactory>>) -> Config {
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
            input_factories: input_factories,
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

    pub fn io_reader(&mut self) -> io::Result<Box<io::Read+'static>> {
        let path = self.path.clone();
        match path {
            None => {
                println!("no path given");
                Ok(Box::new(io::stdin()))
            },
            Some(ref p) => {
                let mut input_factory: Option<Box<io::Read>> = None;
                for _factory in &mut self.input_factories.iter() {
                    match _factory.can_open(&p) {
                        false => continue,
                        true => match _factory.open(&p) {
                            Ok(r) => {
                                input_factory = Some(r);
                            },
                            Err(err) => {
                                let msg = format!("failed to open {}: {}", p.display(), err);
                                return Err(io::Error::new(io::ErrorKind::Other, msg));
                            }
                        }
                    }
                }
                match input_factory {
                    None => {
                        let msg = "Cannot find any factory";
                        Err(io::Error::new(io::ErrorKind::Other, msg))
                    }
                    Some(r) => Ok(self.get_file_reader(r, &p))
                }
            },
        }
    }

    pub fn get_file_reader<R: Read+'static>(&mut self, reader: R, path: &PathBuf) -> Box<io::Read+'static> {
        match path.extension().unwrap().to_str().unwrap() {
            "gz" => {
                let decoder = GzDecoder::new(reader);
                Box::new(decoder)
            },
            "lzf" => {
                let mut file = reader;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).unwrap();
                let decompressed = lzf::decompress(&buf[..], buf.len() * 100).unwrap();
                let cursor = io::Cursor::new(decompressed);
                Box::new(Box::new(cursor))
            },
            "dc" => {
                self.file_type = FileType::Dc;
                Box::new(reader)
            }
            _ => Box::new(reader)
        }
    }

    pub fn from_reader<R: Read+'static>(&mut self, reader: R) -> io::Result<Box<dr::Reader+'static>> {
        match self.file_type {
            FileType::Csv => {
                let csv_reader_arg = csv::ReaderBuilder::new()
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
