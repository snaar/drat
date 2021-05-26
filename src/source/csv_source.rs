use std::io::Read;

use csv::{self, Trim};

use crate::chopper::types::{FieldType, FieldValue, Header, Nanos, Row};
use crate::cli::util::YesNoAuto;
use crate::error::CliResult;
use crate::source::csv_configs::CSVInputConfig;
use crate::source::csv_timestamp::{self, TimestampCol, TimestampFmt};
use crate::source::source::Source;
use crate::util::csv_util;
use crate::util::reader::{ChopperBufPreviewer, ChopperBufReader};
use crate::util::tz::ChopperTz;

const DELIMITERS: &[u8] = b",\t ";

pub struct CSVSource<R: Read> {
    reader: csv::Reader<ChopperBufReader<R>>,
    header: Header,
    timestamp_col: TimestampCol,
    timestamp_fmt: TimestampFmt,
    timezone: ChopperTz,
    next_row: Row,
    has_next_row: bool,
}

impl<R: Read> CSVSource<R> {
    pub fn new(
        previewer: ChopperBufPreviewer<R>,
        csv_input_config: &CSVInputConfig,
    ) -> CliResult<Self> {
        let (line1, line2) = match previewer.get_lines() {
            None => (None, None),
            Some(lines) => (lines.get(0), lines.get(1)),
        };

        let delimiter = match csv_input_config.delimiter {
            None => {
                match line1 {
                    Some(line) => csv_util::guess_delimiter(line.as_str(), DELIMITERS),
                    None => DELIMITERS[0], // doesn't really matter, since file is empty, just give something back
                }
            }
            Some(d) => d,
        };

        let has_header = match csv_input_config.has_header {
            YesNoAuto::Yes => true,
            YesNoAuto::No => false,
            YesNoAuto::Auto => csv_util::guess_has_header(line1, line2, delimiter),
        };

        let reader = previewer.get_reader();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_header)
            .trim(Trim::All)
            .flexible(true)
            .from_reader(reader);

        // get first row
        let first_row: csv::StringRecord = reader.records().next().unwrap()?;
        let field_count = first_row.len();

        let field_names = if reader.has_headers() {
            // get field names if available
            let mut field_names: Vec<String> = Vec::new();
            let header_record = reader.headers()?;
            for i in header_record {
                field_names.push(i.to_string());
            }
            field_names
        } else {
            // otherwise generate default field names
            Header::generate_default_field_names(field_count)
        };

        // initialize next_row
        let timestamp: Nanos = 0;
        let field_values: Vec<FieldValue> = vec![FieldValue::None; field_count];
        let next_row = Row {
            timestamp,
            field_values,
        };
        let field_types: Vec<FieldType> = vec![FieldType::String; field_count];
        let header: Header = Header::new(field_names, field_types);

        let timestamp_config = &csv_input_config.timestamp_config;
        let timezone = timestamp_config.timezone();
        let (timestamp_col, timestamp_fmt) = csv_timestamp::get_timestamp_col_and_fmt(
            &header,
            &first_row,
            timestamp_config.timestamp_col(),
            timestamp_config.timestamp_fmt(),
            timezone,
        )?;

        let mut csv_reader = CSVSource {
            reader,
            header,
            timestamp_col,
            timestamp_fmt,
            timezone: timezone.clone(),
            next_row,
            has_next_row: true,
        };

        // update next_row with first row
        csv_reader.update_row(first_row)?;

        Ok(csv_reader)
    }

    fn update_row(&mut self, next_record: csv::StringRecord) -> CliResult<()> {
        for i in 0..next_record.len() {
            self.next_row.field_values[i] =
                FieldValue::String(next_record.get(i).unwrap().to_string());
        }

        self.next_row.timestamp = csv_timestamp::get_timestamp(
            &next_record,
            &self.timestamp_col,
            &self.timestamp_fmt,
            &self.timezone,
        )?;
        Ok(())
    }

    fn next_row(&mut self) -> CliResult<Option<Row>> {
        if !self.has_next_row {
            return Ok(None);
        }

        let current_row = self.next_row.clone();
        match self.reader.records().next() {
            Some(r) => self.update_row(r?)?,
            None => self.has_next_row = false,
        }
        Ok(Some(current_row))
    }
}

impl<R: Read> Source for CSVSource<R> {
    fn header(&self) -> &Header {
        &self.header
    }

    fn next_row(&mut self) -> CliResult<Option<Row>> {
        self.next_row()
    }
}
