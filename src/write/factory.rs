use crate::chopper::error::{ChopperResult, Error};
use crate::chopper::sink::DynHeaderSink;
use crate::util::dc_factory::DCFactory;
use crate::util::path::buf_writer_from_file_path;
use crate::write::csv_output_config::CSVOutputConfig;
use crate::write::csv_sink;

pub struct OutputFactory {
    csv_output_config: CSVOutputConfig,
    dc_factory: Option<DCFactory>,
}

impl OutputFactory {
    pub fn new() -> OutputFactory {
        OutputFactory {
            csv_output_config: CSVOutputConfig::new_default(),
            dc_factory: None,
        }
    }

    pub fn with_dc_factory(mut self, dc_factory: Option<DCFactory>) -> Self {
        self.dc_factory = dc_factory;
        self
    }

    pub fn with_csv_output_config(mut self, csv_output_config: CSVOutputConfig) -> Self {
        self.csv_output_config = csv_output_config;
        self
    }

    pub fn new_header_sink(&self, output: Option<&str>) -> ChopperResult<Box<dyn DynHeaderSink>> {
        let writer: Box<dyn DynHeaderSink> = match output {
            Some(p) => {
                let p = p.to_string();
                if p.ends_with("csv") {
                    let writer = buf_writer_from_file_path(&Some(p))?;
                    Box::new(csv_sink::CSVSink::new(
                        writer,
                        self.csv_output_config.clone(),
                    )?)
                } else if p.ends_with("dc") {
                    match &self.dc_factory {
                        None => return Err(Error::DCFactoryMissing),
                        Some(dc_factory) => {
                            let writer = buf_writer_from_file_path(&Some(p))?;
                            Box::new(dc_factory.new_sink(writer)?)
                        }
                    }
                } else {
                    return Err(Error::from(format!("file type -- {} is not supported", p)));
                }
            }
            None => {
                // if none use csv sink
                let writer = buf_writer_from_file_path(&None)?;
                Box::new(csv_sink::CSVSink::new(
                    writer,
                    self.csv_output_config.clone(),
                )?)
            }
        };
        Ok(writer)
    }
}
