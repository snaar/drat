use std::io::{BufReader, Read};

use byteorder::{BigEndian, ReadBytesExt};

use crate::chopper::chopper::Source;
use crate::error::CliResult;
use crate::source::{dc_source::DCSource, source_factory::SourceFactory};
use crate::util::dc_util;
use crate::util::reader::ChopperBufPreviewer;

pub struct DCFactory;

impl SourceFactory for DCFactory {
    fn can_create_from_format(&self, format: &String) -> bool {
        format.ends_with(".dc")
    }

    fn can_create_from_previewer(&self, previewer: &ChopperBufPreviewer<Box<dyn Read>>) -> bool {
        let buf = previewer.get_buf();
        let mut reader = BufReader::new(buf.as_ref());

        match reader.read_u64::<BigEndian>() {
            Ok(magic_num) => {
                if &magic_num != &dc_util::MAGIC_NUM {
                    return false;
                }
            }
            Err(_) => return false,
        }

        match reader.read_u16::<BigEndian>() {
            Ok(version) => {
                if &version != &dc_util::VERSION {
                    return false;
                }
            }
            Err(_) => return false,
        };

        true
    }

    fn create_source(
        &mut self,
        previewer: ChopperBufPreviewer<Box<dyn Read>>,
    ) -> CliResult<Box<dyn Source>> {
        let reader = previewer.get_reader();
        Ok(Box::new(DCSource::new(reader)?))
    }
}
