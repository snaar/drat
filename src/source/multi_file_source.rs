use std::io;
use std::path::Path;

use crate::chopper::types::{Header, Row};
use crate::error::{CliResult, Error};
use crate::input::input::InputFormat;
use crate::input::serial_multi_file_provider::SerialMultiFilePathProvider;
use crate::input::single_file::SingleFileInputFactory;
use crate::source::source::Source;

pub struct SerialMultiFileSource {
    input_factory: SingleFileInputFactory,
    path_provider: Box<dyn SerialMultiFilePathProvider>,
    input_format: InputFormat,
    common_header: Header,
    swap_map: Vec<usize>, // empty map means pass-through
    current_source: Option<Box<dyn Source>>,
}

impl SerialMultiFileSource {
    pub fn new(
        mut input_factory: SingleFileInputFactory,
        mut path_provider: Box<dyn SerialMultiFilePathProvider>,
        input_format: InputFormat,
        external_common_header: Option<Header>,
    ) -> CliResult<SerialMultiFileSource> {
        let (common_header, current_source, swap_map) = match path_provider.get_next_path() {
            None => (Header::new(Vec::new(), Vec::new()), None, Vec::new()),
            Some(first_path) => {
                let first_source =
                    Self::create_source_from_path(&mut input_factory, &first_path, &input_format)?;

                let (common_header, swap_map) = match external_common_header {
                    None => (first_source.header().clone(), Vec::new()),
                    Some(header) => {
                        let swap_map = Self::check_headers_match_and_return_swap_map(
                            &header,
                            first_source.header(),
                        )?;
                        (header, swap_map)
                    }
                };

                (common_header, Some(first_source), swap_map)
            }
        };

        Ok(SerialMultiFileSource {
            input_factory,
            path_provider,
            input_format,
            common_header,
            current_source,
            swap_map,
        })
    }

    fn update_to_next_source(&mut self) -> CliResult<()> {
        let next_path = self.path_provider.get_next_path();

        let (next_source, next_swap_map) = match next_path {
            None => (None, Vec::new()),
            Some(path) => {
                let next_source = Self::create_source_from_path(
                    &mut self.input_factory,
                    &path,
                    &self.input_format,
                )?;
                let next_swap_map = Self::check_headers_match_and_return_swap_map(
                    &self.common_header,
                    next_source.header(),
                )?;
                (Some(next_source), next_swap_map)
            }
        };

        self.current_source = next_source;
        self.swap_map = next_swap_map;

        Ok(())
    }

    fn create_source_from_path(
        input_factory: &mut SingleFileInputFactory,
        path: &Path,
        input_format: &InputFormat,
    ) -> CliResult<Box<dyn Source>> {
        let source = input_factory.create_source_from_path(path, input_format)?;
        match source {
            None => {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to handle path {:?}", path),
                )));
            }
            Some(source) => Ok(source),
        }
    }

    fn check_headers_match_and_return_swap_map(
        ref_header: &Header,
        new_header: &Header,
    ) -> CliResult<Vec<usize>> {
        if new_header.field_names().len() < ref_header.field_names().len() {
            return Err(Error::from(format!(
                "next file in a multi-file input has less columns than expected; \
                expected columns: {:?}; found columns: {:?}",
                ref_header.field_names(),
                new_header.field_names()
            )));
        }

        let mut new_header = new_header.clone();
        let mut is_identity_map = true;

        let mut swap_map = Vec::with_capacity(ref_header.field_names().len());
        for (ref_idx, ref_field_name) in ref_header.field_names().iter().enumerate() {
            let new_idx_offset = new_header.field_names()[ref_idx..]
                .iter()
                .position(|name| name == ref_field_name);

            match new_idx_offset {
                None => {
                    return Err(Error::from(format!(
                        "next file in a multi-file input is missing at least one expected column; \
                        expected column missing: {:?}",
                        ref_field_name,
                    )));
                }
                Some(new_idx_offset) => {
                    let new_idx = ref_idx + new_idx_offset;

                    let ref_field_type = &ref_header.field_types()[ref_idx];
                    let new_field_type = &new_header.field_types()[new_idx];

                    if ref_field_type != new_field_type {
                        return Err(Error::from(format!(
                            "next file in a multi-file input has unexpected column type for column {:?}; \
                            expected type: {:?}; found type: {:?}",
                            ref_field_name,
                            ref_field_type,
                            new_field_type
                        )));
                    }

                    swap_map.push(new_idx);
                    if ref_idx != new_idx {
                        new_header.field_names_mut().swap(ref_idx, new_idx);
                        new_header.field_types_mut().swap(ref_idx, new_idx);
                        is_identity_map = false;
                    }
                }
            };
        }

        if is_identity_map {
            swap_map.clear();
        }

        Ok(swap_map)
    }
}

impl Source for SerialMultiFileSource {
    fn header(&self) -> &Header {
        &self.common_header
    }

    fn next_row(&mut self) -> CliResult<Option<Row>> {
        loop {
            match &mut self.current_source {
                None => return Ok(None),
                Some(source) => {
                    let next_row = source.next_row()?;
                    match next_row {
                        None => {
                            self.update_to_next_source()?;
                            continue;
                        }
                        Some(mut next_row) => {
                            if !self.swap_map.is_empty() {
                                for (ref_idx, &new_idx) in self.swap_map.iter().enumerate() {
                                    next_row.field_values.swap(ref_idx, new_idx);
                                }
                                next_row.field_values.truncate(self.swap_map.len());
                            }
                            return Ok(Some(next_row));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::chopper::types::{FieldType, Header};
    use crate::source::multi_file_source::SerialMultiFileSource;

    #[test]
    fn test_check_headers_match_and_return_swap_map() {
        let ref_field_names = vec![
            "f1".to_string(),
            "f2".to_string(),
            "f3".to_string(),
            "f4".to_string(),
            "f5".to_string(),
        ];
        let ref_field_types = vec![
            FieldType::Boolean,
            FieldType::Byte,
            FieldType::String,
            FieldType::Int,
            FieldType::Long,
        ];
        let ref_header = Header::new(ref_field_names, ref_field_types);

        let new_field_names = vec![
            "f1".to_string(),
            "f2".to_string(),
            "f3".to_string(),
            "f4".to_string(),
            "f5".to_string(),
        ];
        let new_field_types = vec![
            FieldType::Boolean,
            FieldType::Byte,
            FieldType::String,
            FieldType::Int,
            FieldType::Long,
        ];
        let new_header = Header::new(new_field_names, new_field_types);
        assert!(
            SerialMultiFileSource::check_headers_match_and_return_swap_map(
                &ref_header,
                &new_header
            )
            .unwrap()
            .is_empty()
        );

        let new_field_names = vec![
            "f1".to_string(),
            "f2".to_string(),
            "f3".to_string(),
            "f4".to_string(),
            "f6".to_string(),
        ];
        let new_field_types = vec![
            FieldType::Boolean,
            FieldType::Byte,
            FieldType::String,
            FieldType::Int,
            FieldType::Long,
        ];
        let new_header = Header::new(new_field_names, new_field_types);
        assert!(
            SerialMultiFileSource::check_headers_match_and_return_swap_map(
                &ref_header,
                &new_header
            )
            .is_err()
        );

        let new_field_names = vec![
            "f1".to_string(),
            "f3".to_string(),
            "f2".to_string(),
            "f4".to_string(),
            "f5".to_string(),
        ];
        let new_field_types = vec![
            FieldType::Boolean,
            FieldType::Byte,
            FieldType::String,
            FieldType::Int,
            FieldType::Long,
        ];
        let new_header = Header::new(new_field_names, new_field_types);
        assert!(
            SerialMultiFileSource::check_headers_match_and_return_swap_map(
                &ref_header,
                &new_header
            )
            .is_err()
        );

        let new_field_names = vec![
            "f1".to_string(),
            "f3".to_string(),
            "f2".to_string(),
            "f4".to_string(),
            "f5".to_string(),
        ];
        let new_field_types = vec![
            FieldType::Boolean,
            FieldType::String,
            FieldType::Byte,
            FieldType::Int,
            FieldType::Long,
        ];
        let new_header = Header::new(new_field_names, new_field_types);
        assert_eq!(
            SerialMultiFileSource::check_headers_match_and_return_swap_map(
                &ref_header,
                &new_header
            )
            .unwrap(),
            vec![0, 2, 2, 3, 4]
        );

        let new_field_names = vec![
            "f1".to_string(),
            "f3".to_string(),
            "q2".to_string(),
            "q2".to_string(),
            "f2".to_string(),
            "f4".to_string(),
            "f5".to_string(),
            "q4".to_string(),
        ];
        let new_field_types = vec![
            FieldType::Boolean,
            FieldType::String,
            FieldType::Double,
            FieldType::Double,
            FieldType::Byte,
            FieldType::Int,
            FieldType::Long,
            FieldType::Double,
        ];
        let new_header = Header::new(new_field_names, new_field_types);
        assert_eq!(
            SerialMultiFileSource::check_headers_match_and_return_swap_map(
                &ref_header,
                &new_header
            )
            .unwrap(),
            vec![0, 4, 4, 5, 6]
        );
    }
}
