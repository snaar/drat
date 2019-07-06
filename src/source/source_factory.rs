use std::io;
use std::path::PathBuf;

use crate::chopper::chopper::Source;
use crate::error::{CliResult, Error};
use crate::source::{csv_config::CSVConfig, csv_factory::CSVFactory, dc_factory::DCFactory, decompress};
use crate::transport::transport_factory::TransportFactory;

pub trait SourceFactory {
    fn can_create_from(&self, path: &PathBuf) -> bool;
    fn create_source(&mut self, reader: Box<io::Read>) -> CliResult<Box<Source+'static>>;
}

pub struct BosuSourceFactory {
    transport_factories: Vec<Box<TransportFactory>>,
    source_factories: Vec<Box<SourceFactory>>
}

impl BosuSourceFactory {
    pub fn new(csv_config: Option<CSVConfig>,
               user_source_factories: Option<Vec<Box<SourceFactory>>>,
               transport_factories: Vec<Box<TransportFactory>>) -> CliResult<Self> {

        let csv_config = match csv_config {
            Some(c) => c,
            None => CSVConfig::new_default()?
        };
        let csv_factory = CSVFactory::new(csv_config);
        let dc_factory = DCFactory::new();
        let mut source_factories: Vec<Box<SourceFactory>> = vec![Box::new(csv_factory), Box::new(dc_factory)];
        if user_source_factories.is_some() {
            source_factories.append(&mut user_source_factories.unwrap());
        }
        Ok(BosuSourceFactory{ transport_factories, source_factories })
    }
}

impl BosuSourceFactory {
    pub fn create_source_from_path(&mut self, path: &str) -> CliResult<Box<Source+'static>> {
        self.create_source(path, None)
    }

    pub fn create_source_from_path_with_file_type(&mut self, path: &str, file_type_override: &str)
                                                                            -> CliResult<Box<Source+'static>> {
        self.create_source(path, Some(file_type_override))
    }

    pub fn create_source_from_stdin(&mut self, file_type: &str) -> CliResult<Box<Source+'static>> {
        self.create_source_from_reader(Box::new(io::stdin()), file_type)
    }

    fn create_source(&mut self, path: &str, file_extension_override: Option<&str>) -> CliResult<Box<Source+'static>> {
        let path = PathBuf::from(path);
        let reader = self.create_io_reader(&path)?;
        let file_extension = match file_extension_override {
            Some(x) => x,
            None => path.extension().unwrap().to_str().unwrap()
        };
        self.create_source_from_reader(reader, file_extension)
    }

    fn create_source_from_reader(&mut self, mut reader: Box<io::Read>, file_extension: &str) -> CliResult<Box<Source+'static>> {
        // create a dummy path with given file extension
        let mut path = PathBuf::from("dummy");
        path.set_extension(file_extension);

        // check if the file is compressed
        if decompress::is_compressed(&path) {
            reader = decompress::decompress(&path, reader)?;
            path = PathBuf::from(path.file_stem().unwrap());
        }
        // get source from matching source factory
        for sf in &mut self.source_factories {
            if sf.can_create_from(&path) {
                return sf.create_source(reader)
            }
        }
        Err(Error::from(format!("Cannot find source factory for file - {:?}", path)))
    }

    fn create_io_reader(&mut self, path: &PathBuf) -> CliResult<Box<io::Read+'static>> {
        let mut io_reader: Option<Box<io::Read>> = None;
        for _factory in &mut self.transport_factories.iter() {
            match _factory.can_open(path) {
                false => continue,
                true => {
                    io_reader = Some(_factory.open(path)?)
                }
            }
        }
        match io_reader {
            None => {
                let msg = format!("Cannot open file {:?}. \
                        Check if the path is valid and/or if a right factory is provided.", &path);
                let err = io::Error::new(io::ErrorKind::Other, msg);
                Err(Error::Io(err))
            }
            Some(r) => Ok(Box::new(r))
        }
    }
}
