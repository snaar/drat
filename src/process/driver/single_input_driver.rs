use crate::args::Args;
use crate::source_config::SourceConfig;
use crate::dr::{types, dr};
use crate::result::CliResult;
use crate::write::factory;

#[derive(PartialEq)]
pub enum Action {
    Stop,
    Write,
    Skip,
}

#[derive(Debug, Copy, Clone)]
pub struct SingleInputDriver {
    begin: Option<u64>,
    end: Option<u64>,
}

/* read, filter and output rows */
impl SingleInputDriver {
    pub fn new(begin: Option<u64>, end: Option<u64>) -> Self {
        SingleInputDriver { begin, end }
    }

    pub fn new_from_args(argv: &Args) -> Self {
        SingleInputDriver::new(argv.begin, argv.end)
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

    pub fn read(&self, config: &mut SourceConfig, output: &Option<&str>) -> CliResult<()> {
        let mut source: Box<dr::Source+'static> = config.get_reader();
        let mut writer: Box<dr::Sink> = factory::new_sink(output, &mut source.header(), config.get_csv_config());

        loop {
            let next_row = source.next_row();
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
