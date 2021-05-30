use std::io::Read;
use std::path::Path;

use crate::chopper::error::{ChopperResult, Error};
use crate::decompress::decompress;
use crate::decompress::decompress::DecompressionFormat;
use crate::decompress::zip::{is_zip, new_reader_for_single_file_zip_archive};
use crate::input::input::InputFormat;
use crate::source::source::Source;
use crate::source::source_factory::SourceFactory;
use crate::transport::seekable::seekable_factory::SeekableTransportFactory;
use crate::transport::streaming::previewer_factory::PreviewerTransportFactory;
use crate::util::path::get_file_name;
use crate::util::reader::ChopperBufPreviewer;

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

#[derive(Clone)]
pub struct SingleFileInputFactory {
    seekable_transport_factory: SeekableTransportFactory,
    previewer_transport_factory: PreviewerTransportFactory,
    source_factories: Vec<Box<dyn SourceFactory>>,
}

impl SingleFileInputFactory {
    pub fn new(
        seekable_transport_factory: SeekableTransportFactory,
        previewer_transport_factory: PreviewerTransportFactory,
        source_factories: Vec<Box<dyn SourceFactory>>,
    ) -> SingleFileInputFactory {
        SingleFileInputFactory {
            seekable_transport_factory,
            previewer_transport_factory,
            source_factories,
        }
    }

    pub fn create_source_from_path(
        &mut self,
        path: &Path,
        input_format: &InputFormat,
    ) -> ChopperResult<Option<Box<dyn Source>>> {
        // see if this needs a seekable transport
        let seekable = self.seekable_transport_factory.create_seekable(path)?;
        if let Some(mut seekable) = seekable {
            // it's rare to need seekable, so we are going to support .zip files as special case here
            // with no user extensibility at least for now
            if is_zip(&mut seekable)? {
                let (reader, file_name) = new_reader_for_single_file_zip_archive(seekable)?;
                let previewer = ChopperBufPreviewer::new(reader)?;
                return Ok(Some(self.create_source_from_previewer(
                    previewer,
                    Some(file_name),
                    &input_format,
                )?));
            }
        }

        // try the non-seekable streaming transports
        let previewer = self.previewer_transport_factory.create_previewer(path)?;
        if let Some(previewer) = previewer {
            let file_name = get_file_name(path);

            return Ok(Some(self.create_source_from_previewer(
                previewer,
                file_name,
                &input_format,
            )?));
        }

        Ok(None)
    }

    pub fn create_source_from_previewer(
        &mut self,
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
        file_name: Option<String>,
        input_format: &InputFormat,
    ) -> ChopperResult<Box<dyn Source>> {
        let format = match input_format {
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
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
        format: String,
    ) -> ChopperResult<(
        FormatAutodetectResult,
        ChopperBufPreviewer<Box<dyn Read>>,
        String,
    )> {
        match decompress::is_compressed_using_format(&format) {
            Some((decompression_format, new_format)) => {
                let new_previewer = Self::decompress(decompression_format, previewer)?;
                Ok((FormatAutodetectResult::Detected, new_previewer, new_format))
            }
            None => Ok((FormatAutodetectResult::NotDetected, previewer, format)),
        }
    }

    fn decompress_by_autodetecting_format(
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
    ) -> ChopperResult<(FormatAutodetectResult, ChopperBufPreviewer<Box<dyn Read>>)> {
        match decompress::is_compressed_using_previewer(&previewer) {
            Some(decompression_format) => {
                let new_previewer = Self::decompress(decompression_format, previewer)?;
                Ok((FormatAutodetectResult::Detected, new_previewer))
            }
            None => Ok((FormatAutodetectResult::NotDetected, previewer)),
        }
    }

    fn decompress(
        decompression_format: DecompressionFormat,
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
    ) -> ChopperResult<ChopperBufPreviewer<Box<dyn Read>>> {
        let new_reader = decompress::decompress(decompression_format, previewer)?;
        Ok(ChopperBufPreviewer::new(new_reader)?)
    }

    fn create_source_from_format(
        &mut self,
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
        format: String,
    ) -> ChopperResult<Box<dyn Source>> {
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
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
    ) -> ChopperResult<Box<dyn Source>> {
        for sf in &mut self.source_factories {
            if sf.can_create_from_previewer(&previewer) {
                return sf.create_source(previewer);
            }
        }

        Err(Error::from(
            "Failed to autodetect file format by peeking at file contents.",
        ))
    }
}
