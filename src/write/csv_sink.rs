use std::io::Write;

use crate::chopper::error::ChopperResult;
use crate::chopper::sink::{DataSink, DynHeaderSink, TypedHeaderSink};
use crate::chopper::types::{FieldValue, Header, Row};
use crate::source::csv_configs::{CSVOutputConfig, TimestampStyle};
use crate::source::csv_timestamp::TimestampUnits;

pub struct CSVSink<W: 'static + Write> {
    writer: W,
    csv_output_config: CSVOutputConfig,
}

impl<W: 'static + Write> CSVSink<W> {
    pub fn new(writer: W, csv_output_config: CSVOutputConfig) -> ChopperResult<Self> {
        Ok(CSVSink {
            writer,
            csv_output_config,
        })
    }

    fn write_csv_header(&mut self, header: &mut Header) -> ChopperResult<()> {
        let writer = &mut self.writer;
        let field_name = header.field_names().clone();
        let mut first = true;

        if self.csv_output_config.print_time_col() {
            write!(writer, "{},", self.csv_output_config.time_col_name())?;
        }
        for name in field_name {
            if first {
                write!(writer, "{}", name)?;
                first = false;
            } else {
                write!(writer, ",{}", name)?;
            }
        }
        write!(writer, "\n")?;
        Ok(())
    }

    pub fn inner(self) -> W {
        self.writer
    }
}

impl<W: 'static + Write> TypedHeaderSink<Self> for CSVSink<W> {
    fn process_header(mut self, header: &mut Header) -> ChopperResult<Self> {
        self.write_csv_header(header)?;
        Ok(self)
    }
}

impl<W: 'static + Write> DynHeaderSink for CSVSink<W> {
    fn process_header(
        mut self: Box<Self>,
        header: &mut Header,
    ) -> ChopperResult<Box<dyn DataSink>> {
        self.write_csv_header(header)?;
        Ok(Box::new(*self))
    }
}

impl<W: 'static + Write> DataSink for CSVSink<W> {
    fn write_row(&mut self, io_rows: &mut Vec<Row>) -> ChopperResult<()> {
        let row = io_rows.get(0).unwrap();

        let mut first_col = true;
        if self.csv_output_config.print_time_col() {
            let time = match self.csv_output_config.time_col_style() {
                TimestampStyle::Epoch => {
                    let time = match self.csv_output_config.time_col_units() {
                        TimestampUnits::Seconds => row.timestamp / 1_000_000_000,
                        TimestampUnits::Millis => row.timestamp / 1_000_000,
                        TimestampUnits::Micros => row.timestamp / 1_000,
                        TimestampUnits::Nanos => row.timestamp,
                    };
                    time.to_string()
                }
                TimestampStyle::HumanReadable => {
                    let format = match self.csv_output_config.time_col_units() {
                        TimestampUnits::Seconds => "%Y-%m-%dT%H:%M:%S%:z",
                        TimestampUnits::Millis => "%Y-%m-%dT%H:%M:%S%.3f%:z",
                        TimestampUnits::Micros => "%Y-%m-%dT%H:%M:%S%.6f%:z",
                        TimestampUnits::Nanos => "%Y-%m-%dT%H:%M:%S%.9f%:z",
                    };
                    let time = self.csv_output_config.timezone().timestamp(row.timestamp)?;
                    time.format(format).to_string()
                }
            };

            write!(self.writer, "{}", time)?;
            first_col = false;
        }
        let field_values = &row.field_values;
        let delimiter = self.csv_output_config.delimiter();
        for value in field_values {
            if first_col {
                first_col = false;
            } else {
                write!(self.writer, "{}", delimiter)?;
            }

            match value {
                FieldValue::Boolean(x) => {
                    write!(self.writer, "{}", if *x { "true" } else { "false" })?
                }
                FieldValue::Byte(x) => write!(self.writer, "{}", x)?,
                FieldValue::ByteBuf(x) => write!(self.writer, "ByteBuf[len={}]", x.len())?,
                FieldValue::Char(x) => write!(self.writer, "{}", x)?,
                FieldValue::Double(x) => {
                    dtoa::write(&mut self.writer, *x)?;
                }
                FieldValue::Float(x) => {
                    dtoa::write(&mut self.writer, *x)?;
                }
                FieldValue::Int(x) => write!(self.writer, "{}", x)?,
                FieldValue::Long(x) => write!(self.writer, "{}", x)?,
                FieldValue::Short(x) => write!(self.writer, "{}", x)?,
                FieldValue::String(x) => write!(self.writer, "{}", x)?,
                FieldValue::MultiDimDoubleArray(x) => {
                    let dim_str = x
                        .shape()
                        .iter()
                        .map(|d| d.to_string())
                        .collect::<Vec<String>>()
                        .join("x");
                    write!(self.writer, "MultiDimDoubleArray[{}]", dim_str)?
                }
                FieldValue::None => (),
            };
        }
        write!(self.writer, "\n")?;
        Ok(())
    }

    fn flush(&mut self) -> ChopperResult<()> {
        self.writer.flush()?;
        Ok(())
    }
}
