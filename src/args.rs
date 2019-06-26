use crate::error::{CliResult, Error};
use crate::source_config::{CSVConfig, SourceConfig};
use crate::input::input_factory::InputFactory;

#[derive(Copy, Clone)]
pub struct DataRange {
    pub begin: Option<u64>,
    pub end: Option<u64>,
}

static DATA_RANGE_DEFAULT: DataRange = DataRange { begin: None, end: None};

pub struct CliArgs {
    pub inputs: Vec<String>,
    pub output: Option<String>,
    pub data_range: DataRange,
    pub csv_config: CSVConfig,
}

impl CliArgs {
    pub fn new(inputs: Vec<String>, output: Option<String>,
               data_range: Option<DataRange>, csv_config: Option<CSVConfig>) -> CliResult<Self> {

        let data_range = match data_range {
            Some(d) => d,
            None => DATA_RANGE_DEFAULT,
        };
        let csv_config = match csv_config {
            Some(c) => c,
            None => CSVConfig::new_default()?
        };
        Ok(CliArgs { inputs, output, data_range, csv_config})
    }
}

pub struct Args {
    pub cli_args: CliArgs,
    pub input_factories: Vec<Box<InputFactory>>,
}

impl Args {
    pub fn new(inputs: Vec<String>, output: Option<String>, begin: Option<u64>, end: Option<u64>,
               csv_config: CSVConfig, input_factories: Vec<Box<InputFactory>>) -> Self {

        let data_range = DataRange { begin, end };
        let cli_args = CliArgs {inputs, output, data_range, csv_config};
        Args { cli_args, input_factories}
    }

    pub fn create_configs(&mut self) -> CliResult<Vec<SourceConfig>> {
        let mut inputs_clone = self.cli_args.inputs.clone();
        if self.cli_args.inputs.is_empty() {
            inputs_clone.push("-".to_string()); // stdin
        }

        let mut input_factories_copy: Vec<Box<InputFactory>> = Vec::with_capacity(self.input_factories.len());
        for item in &mut self.input_factories {
            input_factories_copy.push(item.box_clone());
        }

        let configs = inputs_clone.into_iter()
            .map(move |p|
                SourceConfig::new(
                    &Some(p), input_factories_copy.clone(), self.cli_args.csv_config.clone()))
            .collect::<Vec<_>>();
        check_at_most_one_stdin(&*configs)?;

        Ok(configs)
    }
}

fn check_at_most_one_stdin(inputs: &[SourceConfig]) -> CliResult<()> {
    let stdin_count = inputs.iter().filter(|input| input.is_stdin()).count();
    if stdin_count > 1 {
        return Err(Error::from("At most one stdin input is allowed"))
    }
    Ok(())
}
