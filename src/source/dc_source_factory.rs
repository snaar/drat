use std::io::{BufReader, Read};

use byteorder::{BigEndian, ReadBytesExt};

use crate::chopper::error::ChopperResult;
use crate::source::source::Source;
use crate::source::source_factory::SourceFactory;
use crate::util::dc_factory::DCFactory;
use crate::util::dc_util;
use crate::util::reader::ChopperBufPreviewer;

#[derive(Clone)]
pub struct DCSourceFactory {
    dc_factory: DCFactory,
}

impl DCSourceFactory {
    pub fn new(dc_factory: DCFactory) -> DCSourceFactory {
        DCSourceFactory { dc_factory }
    }
}

impl SourceFactory for DCSourceFactory {
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
    ) -> ChopperResult<Box<dyn Source>> {
        let reader = previewer.get_reader();
        Ok(Box::new(self.dc_factory.new_source(reader)?))
    }

    fn box_clone(&self) -> Box<dyn SourceFactory> {
        Box::new((*self).clone())
    }
}
