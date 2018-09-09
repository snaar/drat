use args::Args;
use config::Config;
use read::types;
use result::CliResult;
use write::{csv_sink, sink::Sink};

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

/* read, filter and output rows */
impl ReadFilter {
    pub fn new(begin: Option<u64>, end: Option<u64>, timestamp_column: usize) -> Self {
        ReadFilter { begin, end, timestamp_column }
    }

    pub fn new_from_args(argv: &Args) -> Self {
        ReadFilter::new(argv.begin, argv.end, argv.timestamp_column)
    }

    pub fn filter(&self, timestamp: types::Nanos) -> Action {
        if self.end.is_some() && timestamp >= self.end.unwrap() {
            return Action::Stop;
        }

        match self.begin.is_none() || timestamp >= self.begin.unwrap() {
            true => Action::Write,
            false => Action::Skip,
        }
    }

    pub fn read(&self, config: &mut Config, output: &Option<&str>) -> CliResult<()> {
        let mut reader = config.reader()?;
        let mut writer = csv_sink::CSVSink::new(output);

        if config.has_headers() {
            let header = reader.header();
            writer.write_header(&header);
        }

        loop {
            let next_row = reader.next_row();
            match next_row {
                Some(r) => {
                    match self.filter(r.timestamp) {
                        Action::Stop => break,
                        Action::Write => writer.write_row(&r),
                        Action::Skip => continue,
                    }
                }
                None => break,
            }
        }
        Ok(())
    }
}
