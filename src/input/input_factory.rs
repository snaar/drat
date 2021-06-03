use std::io;
use std::path::Path;

use crate::chopper::error::{ChopperResult, Error};
use crate::input::files_in_dir_provider::FilesInDirPathProvider;
use crate::input::input::{Input, InputFormat, InputType};
use crate::input::single_file::SingleFileInputFactory;
use crate::source::csv_input_config::CSVInputConfig;
use crate::source::multi_file_source::SerialMultiFileSource;
use crate::source::source::Source;
use crate::source::{
    csv_source_factory::CSVSourceFactory, dc_source_factory::DCSourceFactory,
    source_factory::SourceFactory,
};
use crate::transport::dir::dir_transport::DirTransport;
use crate::transport::dir::file::DirFileTransport;
use crate::transport::seekable::file::SeekableFileTransport;
use crate::transport::seekable::seekable_factory::SeekableTransportFactory;
use crate::transport::seekable::seekable_transport::SeekableTransport;
use crate::transport::streaming::file::FileTransport;
use crate::transport::streaming::http::HttpTransport;
use crate::transport::streaming::previewer_factory::PreviewerTransportFactory;
use crate::transport::streaming::streaming_transport::StreamingTransport;
use crate::util::dc_factory::DCFactory;
use crate::util::reader::ChopperBufPreviewer;

pub struct InputFactoryBuilder {
    dc_factory: Option<DCFactory>,
    csv_input_config: Option<CSVInputConfig>,
    user_source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    user_streaming_transports: Option<Vec<Box<dyn StreamingTransport>>>,
}

impl InputFactoryBuilder {
    pub fn new() -> InputFactoryBuilder {
        InputFactoryBuilder {
            dc_factory: None,
            csv_input_config: None,
            user_source_factories: None,
            user_streaming_transports: None,
        }
    }

    pub fn with_dc_factory(mut self, dc_factory: Option<DCFactory>) -> Self {
        self.dc_factory = dc_factory;
        self
    }

    pub fn with_csv_input_config(mut self, csv_input_config: CSVInputConfig) -> Self {
        self.csv_input_config = Some(csv_input_config);
        self
    }

    pub fn with_user_source_factories(
        mut self,
        user_source_factories: Option<Vec<Box<dyn SourceFactory>>>,
    ) -> Self {
        self.user_source_factories = user_source_factories;
        self
    }

    pub fn with_user_streaming_transports(
        mut self,
        user_streaming_transports: Option<Vec<Box<dyn StreamingTransport>>>,
    ) -> Self {
        self.user_streaming_transports = user_streaming_transports;
        self
    }

    pub fn build(self) -> ChopperResult<InputFactory> {
        InputFactory::new(
            self.dc_factory,
            self.csv_input_config,
            self.user_source_factories,
            self.user_streaming_transports,
        )
    }
}

pub struct InputFactory {
    dir_transports: Vec<Box<dyn DirTransport>>,
    single_file_input_factory: SingleFileInputFactory,
}

impl InputFactory {
    fn new(
        dc_factory: Option<DCFactory>,
        csv_input_config: Option<CSVInputConfig>,
        user_source_factories: Option<Vec<Box<dyn SourceFactory>>>,
        user_streaming_transports: Option<Vec<Box<dyn StreamingTransport>>>,
    ) -> ChopperResult<Self> {
        // streaming transports and the previewer factory for them
        let mut default_streaming_transports = create_default_streaming_transports();
        let streaming_transports: Vec<Box<dyn StreamingTransport>> = match user_streaming_transports
        {
            Some(mut t) => {
                t.append(&mut default_streaming_transports);
                t
            }
            None => default_streaming_transports,
        };
        let previewer_transport_factory = PreviewerTransportFactory::new(streaming_transports);

        // seekable transports
        let seekable_transports = create_default_seekable_transports();
        let seekable_transport_factory = SeekableTransportFactory::new(seekable_transports);

        // dir transports
        let dir_transports = create_default_dir_transports();

        // source factories
        let mut default_source_factories =
            create_default_source_factories(dc_factory, csv_input_config);
        let source_factories = match user_source_factories {
            Some(mut s) => {
                s.append(&mut default_source_factories);
                s
            }
            None => default_source_factories,
        };

        let single_file_input_factory = SingleFileInputFactory::new(
            seekable_transport_factory,
            previewer_transport_factory,
            source_factories,
        );

        Ok(InputFactory {
            dir_transports,
            single_file_input_factory,
        })
    }

    pub fn create_source_from_path(&mut self, path: &str) -> ChopperResult<Box<dyn Source>> {
        self.create_source_from_input(&Input {
            input: InputType::Path(path.to_owned()),
            format: InputFormat::Auto,
        })
    }

    pub fn create_source_from_input(&mut self, input: &Input) -> ChopperResult<Box<dyn Source>> {
        // first get stdin out of the way, since it doesn't need a transport
        let path = match &input.input {
            InputType::Path(path) => path,
            InputType::StdIn => {
                let previewer =
                    ChopperBufPreviewer::new(Box::new(io::stdin()) as Box<dyn io::Read>)?;
                return self.single_file_input_factory.create_source_from_previewer(
                    previewer,
                    None,
                    &input.format,
                );
            }
        };

        // next we want to see if this is handled by any of the dir transports
        let path = Path::new(path);
        for transport in &self.dir_transports {
            if !transport.can_handle(path) {
                continue;
            }

            let provider = Box::new(FilesInDirPathProvider::new(transport, path)?);
            let source = SerialMultiFileSource::new(
                self.single_file_input_factory.clone(),
                provider,
                input.format.clone(),
                None,
            )?;
            return Ok(Box::new(source));
        }

        // finally let the single file input factory handle it
        let single_source = self
            .single_file_input_factory
            .create_source_from_path(path, &input.format)?;
        if let Some(source) = single_source {
            return Ok(source);
        }

        Err(Error::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("failed to handle path {:?}", path),
        )))
    }
}

fn create_default_source_factories(
    dc_factory: Option<DCFactory>,
    csv_input_config: Option<CSVInputConfig>,
) -> Vec<Box<dyn SourceFactory>> {
    let mut source_factories: Vec<Box<dyn SourceFactory>> = Vec::new();
    if let Some(csv_input_config) = csv_input_config {
        source_factories.push(Box::new(CSVSourceFactory::new(csv_input_config)));
    }
    if let Some(dc_factory) = dc_factory {
        source_factories.push(Box::new(DCSourceFactory::new(dc_factory)));
    }
    source_factories
}

fn create_default_dir_transports() -> Vec<Box<dyn DirTransport>> {
    vec![Box::new(DirFileTransport)]
}

fn create_default_seekable_transports() -> Vec<Box<dyn SeekableTransport>> {
    vec![Box::new(SeekableFileTransport)]
}

fn create_default_streaming_transports() -> Vec<Box<dyn StreamingTransport>> {
    vec![Box::new(FileTransport), Box::new(HttpTransport)]
}
