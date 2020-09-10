use std::io;
use std::path::Path;

use crate::chopper::chopper::Source;
use crate::error::{CliResult, Error};
use crate::input::input::{Input, InputFormat, InputType};
use crate::source::csv_configs::CSVInputConfig;
use crate::source::decompress::{self, DecompressionFormat};
use crate::source::{
    csv_factory::CSVFactory, dc_factory::DCFactory, source_factory::SourceFactory,
};
use crate::transport::{file::FileInput, http::Http, transport_factory::TransportFactory};
use crate::util::preview::Preview;
use crate::util::reader::ChopperBufPreviewer;

pub struct InputFactory {
    transport_factories: Vec<Box<dyn TransportFactory>>,
    source_factories: Vec<Box<dyn SourceFactory>>,
}

#[derive(Clone, Debug)]
enum FormatAutodetectResult {
    Detected,
    NotDetected,
}

#[derive(Clone, Debug)]
enum Format {
    UserSpecified(String),
    DetectUsingFileNameThenContents(String),
    DetectUsingFileContents,
}

impl InputFactory {
    pub fn new(
        csv_input_config: Option<CSVInputConfig>,
        user_source_factories: Option<Vec<Box<dyn SourceFactory>>>,
        user_transport_factories: Option<Vec<Box<dyn TransportFactory>>>,
    ) -> CliResult<Self> {
        // transport factories
        let mut default_transport_factories = create_default_transport_factories();
        let transport_factories: Vec<Box<dyn TransportFactory>> = match user_transport_factories {
            Some(mut t) => {
                t.append(&mut default_transport_factories);
                t
            }
            None => default_transport_factories,
        };

        // source factories
        let csv_input_config = match csv_input_config {
            Some(c) => c,
            None => CSVInputConfig::new_default()?,
        };
        let mut default_source_factories = create_default_source_factories(csv_input_config);
        let source_factories = match user_source_factories {
            Some(mut s) => {
                s.append(&mut default_source_factories);
                s
            }
            None => default_source_factories,
        };

        Ok(InputFactory {
            transport_factories,
            source_factories,
        })
    }
}

impl InputFactory {
    pub fn create_source_from_path(&mut self, path: &str) -> CliResult<Box<dyn Source>> {
        self.create_source_from_input(&Input {
            input: InputType::Path(path.to_owned()),
            format: InputFormat::Auto,
        })
    }

    pub fn create_source_from_input(&mut self, input: &Input) -> CliResult<Box<dyn Source>> {
        let previewer = match &input.input {
            InputType::Path(path) => self.create_previewer(Path::new(path))?,
            InputType::StdIn => Box::new(ChopperBufPreviewer::new(io::stdin())?),
        };

        let file_name = match &input.input {
            InputType::Path(path) => {
                let path = Path::new(path);
                if let Some(file_name) = path.file_name() {
                    // that's right, unwrap to_str first to panic on os->str conversion if needed
                    Some(file_name.to_str().unwrap().to_owned())
                } else {
                    // weird but ok
                    None
                }
            }
            InputType::StdIn => None,
        };

        let format = match &input.format {
            InputFormat::Extension(extension) => {
                let extension = if extension.starts_with(".") {
                    extension.to_owned()
                } else {
                    ".".to_owned() + extension
                };
                Format::UserSpecified(extension)
            }
            InputFormat::Auto => match file_name {
                None => Format::DetectUsingFileContents,
                Some(file_name) => Format::DetectUsingFileNameThenContents(file_name),
            },
        };

        match format {
            Format::UserSpecified(format) => {
                // user told us exactly what they want, don't do any autodetection
                let (_, previewer, format) = Self::decompress_using_format(previewer, format)?;
                self.create_source_from_format(previewer, format)
            }
            Format::DetectUsingFileNameThenContents(format) => {
                // first try using the file name alone

                let (decompression_result, previewer, format) =
                    Self::decompress_using_format(previewer, format)?;

                // can theoretically somehow share this code with create_source_from_reader_and_format,
                // but seems hard due to ownership of reader needed later in this match block;
                // maybe revisit one day as learning experience
                for sf in &mut self.source_factories {
                    if sf.can_create_from_format(&format) {
                        return sf.create_source(previewer);
                    }
                }

                // if we got here, we failed to find source factory that can handle the file name
                // try to find one using contents of the file

                // first, check if we were able to decompress above, if so, don't need to
                // decompress again
                let previewer = match decompression_result {
                    FormatAutodetectResult::Detected => previewer,
                    FormatAutodetectResult::NotDetected => {
                        let (_, previewer) = Self::decompress_by_autodetecting_format(previewer)?;
                        previewer
                    }
                };

                self.create_source_by_autodetecting_format(previewer)
            }
            Format::DetectUsingFileContents => {
                // we didn't even get a file name as hint, try to figure out using the
                // contents of the file right away
                let (_, previewer) = Self::decompress_by_autodetecting_format(previewer)?;
                self.create_source_by_autodetecting_format(previewer)
            }
        }
    }

    fn decompress_using_format(
        previewer: Box<dyn Preview>,
        format: String,
    ) -> CliResult<(FormatAutodetectResult, Box<dyn Preview>, String)> {
        match decompress::is_compressed_using_format(&format) {
            Some((decompression_format, new_format)) => {
                let new_previewer = Self::decompress(decompression_format, previewer)?;
                Ok((FormatAutodetectResult::Detected, new_previewer, new_format))
            }
            None => Ok((FormatAutodetectResult::NotDetected, previewer, format)),
        }
    }

    fn decompress_by_autodetecting_format(
        previewer: Box<dyn Preview>,
    ) -> CliResult<(FormatAutodetectResult, Box<dyn Preview>)> {
        match decompress::is_compressed_using_previewer(previewer.as_ref()) {
            Some(decompression_format) => {
                let new_previewer = Self::decompress(decompression_format, previewer)?;
                Ok((FormatAutodetectResult::Detected, new_previewer))
            }
            None => Ok((FormatAutodetectResult::NotDetected, previewer)),
        }
    }

    fn decompress(
        decompression_format: DecompressionFormat,
        previewer: Box<dyn Preview>,
    ) -> CliResult<Box<dyn Preview>> {
        let reader = previewer.get_reader();
        let new_reader = match decompression_format {
            DecompressionFormat::GZ => decompress::decompress_gz(reader)?,
            DecompressionFormat::LZF => decompress::decompress_lzf(reader)?,
        };
        Ok(Box::new(ChopperBufPreviewer::new(new_reader)?))
    }

    fn create_source_from_format(
        &mut self,
        previewer: Box<dyn Preview>,
        format: String,
    ) -> CliResult<Box<dyn Source>> {
        for sf in &mut self.source_factories {
            if sf.can_create_from_format(&format) {
                return sf.create_source(previewer);
            }
        }

        Err(Error::from(format!(
            "Cannot find source factory for file format {:?}. \
            Note that this might not be the full file name, due to being able to be decompressed.",
            format
        )))
    }

    fn create_source_by_autodetecting_format(
        &mut self,
        previewer: Box<dyn Preview>,
    ) -> CliResult<Box<dyn Source>> {
        for sf in &mut self.source_factories {
            if sf.can_create_from_previewer(&previewer) {
                return sf.create_source(previewer);
            }
        }

        Err(Error::from(
            "Failed to autodetect file format by peeking at file contents.",
        ))
    }

    fn create_previewer(&mut self, path: &Path) -> CliResult<Box<dyn Preview>> {
        let mut reader: Option<Box<dyn io::Read>> = None;
        for factory in &mut self.transport_factories.iter() {
            match factory.can_open(path) {
                false => continue,
                true => reader = Some(factory.open(path)?),
            }
        }
        match reader {
            None => {
                let msg = format!(
                    "Cannot open file {:?}. \
                    Check if the path is valid and/or if a right transport factory is provided.",
                    &path
                );
                let err = io::Error::new(io::ErrorKind::Other, msg);
                Err(Error::Io(err))
            }
            Some(reader) => {
                let previewer = ChopperBufPreviewer::new(reader)?;
                Ok(Box::new(previewer))
            }
        }
    }
}

pub fn create_default_source_factories(
    csv_input_config: CSVInputConfig,
) -> Vec<Box<dyn SourceFactory>> {
    let source_factories: Vec<Box<dyn SourceFactory>> = vec![
        Box::new(CSVFactory::new(csv_input_config)),
        Box::new(DCFactory),
    ];
    source_factories
}

pub fn create_default_transport_factories() -> Vec<Box<dyn TransportFactory>> {
    let transport_factories: Vec<Box<dyn TransportFactory>> =
        vec![Box::new(FileInput), Box::new(Http)];
    transport_factories
}
