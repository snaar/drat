use csv::StringRecord;
use std::process;

use args::Args;
use config::Config;
use result::CliResult;

#[derive(Debug, Copy, Clone)]
pub struct ReadFilter {
    begin: Option<u64>,
    end: Option<u64>,
    timestamp_column: usize,
}

#[derive(PartialEq)]
pub enum Action {
    Stop,
    Write,
    Skip,
}

impl ReadFilter {
    pub fn new(begin: Option<u64>, end: Option<u64>, timestamp_column: usize) -> Self {
        ReadFilter { begin, end, timestamp_column }
    }

    pub fn new_from_args(argv: &Args) -> Self {
        ReadFilter::new(argv.begin, argv.end, argv.timestamp_column)
    }

    pub fn filter(&self, row: &StringRecord) -> Action {
        let timestamp = match row.get(self.timestamp_column).unwrap().parse::<u64>() {
            Ok(t) => t,
            Err(_e) => {
                eprintln!("\nERROR: Cannot read timestamp. Check if the file has header (if yes add --header).");
                process::exit(0)
            }
        };

        if self.end.is_some() && timestamp >= self.end.unwrap() {
            return Action::Stop;
        }

        match self.begin.is_none() || timestamp >= self.begin.unwrap() {
            true => Action::Write,
            false => Action::Skip,
        }
    }

    pub fn read(&self, config: &Config, flag_output: &Option<&str>) -> CliResult<()> {
        let mut reader = config.reader()?;
        let mut writer = Config::new(
            flag_output, config.delimiter(), config.has_headers()).writer()?;

        if config.has_headers() {
            let headers = reader.headers()?;
            writer.write_record(headers)?;
        }

        for row_result in reader.records() {
            let row = row_result?;
            let filter = self.filter(&row);

            if filter == Action::Stop {
                break;
            }
            if filter == Action::Write {
                writer.write_record(row.iter())?;
            }
        }
        writer.flush()?;
        Ok(())
    }
}
