use std::fmt;
use std::io;
use std::ops::Deref;
use std::path::PathBuf;
use std::process;

use csv;
use flate2::read::GzDecoder;
use lzf;

use crate::dr::dr;
use crate::input::input_factory::InputFactory;
use crate::read::{csv_reader, dc_reader};

#[derive(Debug)]
enum FileType {
    Csv,
    Dc,
}

#[derive(Clone)]
pub struct CSVConfig {
    delimiter: u8,
    has_headers: bool,
    timestamp_column_index: usize,
}

impl CSVConfig {
    pub fn new(delimiter: u8, has_headers: bool, timestamp_column_index: usize) -> Self {
        CSVConfig { delimiter, has_headers, timestamp_column_index }
    }

    pub fn has_headers(&self) -> bool {
        self.has_headers
    }

    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }
}

impl fmt::Debug for CSVConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "delimiter: {:?}, has headers: {:?}", self.delimiter, self.has_headers)
    }
}

#[derive(Debug)]
pub struct SourceConfig {
    path: Option<PathBuf>, // 'None' implies stdin. TODO: maybe use custom enum instead of commenting?
    input_factories: Vec<Box<InputFactory>>,
    file_type: FileType, // TODO: refactor the dr::Source factory functions below and get rid of this variable
    csv_config: CSVConfig,
}

impl SourceConfig {
    pub fn new(path: &Option<&str>, input_factories: Vec<Box<InputFactory>>, csv_config: CSVConfig) -> Self {
        let path = match *path {
            None => None,
            Some(ref s) if s.deref() == "-".to_owned() => None,
            Some(ref s) => Some(PathBuf::from(s)),
        };
        let file_type = FileType::Csv;
        SourceConfig { path, input_factories, file_type, csv_config }
    }

    pub fn get_csv_config(&self) -> &CSVConfig {
        &self.csv_config
    }

    pub fn is_stdin(&self) -> bool {
        self.path.is_none()
    }

    pub fn get_reader(&mut self) -> Box<dr::Source+'static> {
        match self.reader() {
            Ok(r) => r,
            Err(err) => {
                werr!("Error: {}", err);
                process::exit(1);
            },
        }
    }

    pub fn reader(&mut self) -> io::Result<Box<dr::Source+'static>> {
        let io_reader = self.get_io_reader()?;
        Ok(self.generate_source(io_reader)?)
    }

    fn get_io_reader(&mut self) -> io::Result<Box<io::Read+'static>> {
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

    fn get_file_reader<R: io::Read+'static>(&mut self, reader: R, path: &PathBuf) -> Box<io::Read+'static> {
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

    fn generate_source<R: io::Read+'static>(&mut self, reader: R) -> io::Result<Box<dr::Source+'static>> {
        match self.file_type {
            FileType::Csv => {
                let csv_reader_arg = csv::ReaderBuilder::new()
                    .delimiter(self.csv_config.delimiter())
                    .has_headers(self.csv_config.has_headers())
                    .from_reader(reader);
                Ok(Box::new(csv_reader::CSVReader::new(
                    csv_reader_arg, self.csv_config.timestamp_column_index)))

            },
            FileType::Dc => {
                Ok(Box::new(dc_reader::DCReader::new(reader)))
            }
        }
    }
}

