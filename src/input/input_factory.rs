use std::io;
use std::path::Path;

use crate::chopper::chopper::Source;
use crate::error::{CliResult, Error};
use crate::source::{csv_factory::CSVFactory, dc_factory::DCFactory, source_factory::SourceFactory};
use crate::source::csv_configs::CSVInputConfig;
use crate::source::decompress;
use crate::transport::{file::FileInput, http::Http, transport_factory::TransportFactory};

pub struct InputFactory {
    fallback_file_ext: Option<String>,
    transport_factories: Vec<Box<dyn TransportFactory>>,
    source_factories: Vec<Box<dyn SourceFactory>>
}

impl InputFactory {
    pub fn new(fallback_file_ext: Option<&str>,
               csv_input_config: Option<CSVInputConfig>,
               user_source_factories: Option<Vec<Box<dyn SourceFactory>>>,
               user_transport_factories: Option<Vec<Box<dyn TransportFactory>>>) -> CliResult<Self>
    {
        let fallback_file_ext: Option<String> = match fallback_file_ext {
            None => None,
            Some(x) => Some(x.to_owned())
        };

        // transport factories
        let mut default_transport_factories = create_default_transport_factories();
        let transport_factories: Vec<Box<dyn TransportFactory>> = match user_transport_factories {
            Some(mut t) => {
                t.append(&mut default_transport_factories);
                t
            },
            None => default_transport_factories
        };

        // source factories
        let csv_input_config = match csv_input_config {
            Some(c) => c,
            None => CSVInputConfig::new_default()?
        };
        let mut default_source_factories = create_default_source_factories(csv_input_config);
        let source_factories = match user_source_factories {
            Some(mut s) => {
                s.append(&mut default_source_factories);
                s
            },
            None => default_source_factories
        };

        Ok(InputFactory { fallback_file_ext, transport_factories, source_factories })
    }
}

impl InputFactory {
    pub fn create_source_from_path(&mut self, path: &str) -> CliResult<Box<dyn Source>> {
        self.create_source(path, None)
    }

    pub fn create_source_from_path_with_file_type(
        &mut self, path: &str, file_type_override: &str) -> CliResult<Box<dyn Source>>
    {
        self.create_source(path, Some(file_type_override))
    }

    pub fn create_source_from_stdin(&mut self, file_type: &str) -> CliResult<Box<dyn Source>> {
        self.create_source_from_reader(Box::new(io::stdin()), file_type)
    }

    fn create_source(&mut self, path: &str, file_extension_override: Option<&str>) -> CliResult<Box<dyn Source>> {
        let mut path = Path::new(path);
        let reader = self.create_io_reader(path)?;
        let file_extension = match file_extension_override {
            Some(x) => x.to_string(),
            None => {
                if path.extension().is_none() {
                    match &self.fallback_file_ext {
                        Some(x) => x.clone(),
                        None => {
                            return Err(Error::from(format!("Unkown file type: [{:?}].\n\
                                No file extension found. Neither file extension override, nor default file extension was specified.", path)));
                        }
                    }
                } else {
                    let mut extension: String = "".to_string();
                    while path.extension().is_some() {
                        let string = path.extension().unwrap().to_str().unwrap();
                        extension = format!(".{}{}", string, extension);
                        path = Path::new(path.file_stem().unwrap());
                    }
                    extension
                }
            }
        };
        self.create_source_from_reader(Box::new(reader), &file_extension)
    }

    fn create_source_from_reader(&mut self,
                                 mut reader: Box<dyn io::Read>,
                                 file_extension: &str) -> CliResult<Box<dyn Source>>
    {
        // create a dummy path with given file extension
        let mut path = Path::new("dummy");
        let buf = path.with_extension(file_extension);
        path = buf.as_path();

        // check if the file is compressed
        if decompress::is_compressed(&path) {
            reader = decompress::decompress(&path, reader)?;
            path = Path::new(path.file_stem().unwrap());
        }
        // get source from matching source factory
        for sf in &mut self.source_factories {
            if sf.can_create_from(&path) {
                return sf.create_source(reader)
            }
        }
        Err(Error::from(format!("Cannot find source factory for file - {:?}", path)))
    }

    fn create_io_reader(&mut self, path: &Path) -> CliResult<Box<dyn io::Read>> {
        let mut io_reader: Option<Box<dyn io::Read>> = None;
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

pub fn create_default_source_factories(csv_input_config: CSVInputConfig) -> Vec<Box<dyn SourceFactory>> {
    let source_factories: Vec<Box<dyn SourceFactory>>
        = vec![Box::new(CSVFactory::new(csv_input_config)), Box::new(DCFactory)];
    source_factories
}

pub fn create_default_transport_factories() -> Vec<Box<dyn TransportFactory>> {
    let transport_factories: Vec<Box<dyn TransportFactory>>
        = vec![Box::new(FileInput), Box::new(Http)];
    transport_factories
}
