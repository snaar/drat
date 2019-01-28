use crate::result::{CliResult, CliError};
use crate::source_config::{CSVConfig, SourceConfig};
use crate::input::input_factory::InputFactory;

pub struct DataRange {
    pub begin: Option<u64>,
    pub end: Option<u64>,
}

pub struct CliArgs {
    pub inputs: Vec<String>,
    pub output: Option<String>,
    pub data_range: DataRange,
    pub csv_config: CSVConfig,
}

pub struct Args {
    pub cli_args: CliArgs,
    pub input_factories: Vec<Box<InputFactory>>,
}

impl Args {
    pub fn create_config(&mut self) -> CliResult<SourceConfig> {
        if self.cli_args.inputs.len() > 1 {
            return Err(CliError::Other("Error: more than one input file specified.".to_owned()));
        }
        let input: Option<String> = match self.cli_args.inputs.len() {
            0 => None,
            1 => Some(self.cli_args.inputs.get(0).unwrap().to_string()),
            _ => unreachable!(),
        };

        let mut input_factories_copy: Vec<Box<InputFactory>> = Vec::with_capacity(self.input_factories.len());
        for item in &mut self.input_factories {
            input_factories_copy.push(item.box_clone());
        }
        let config = SourceConfig::new(&input, input_factories_copy, self.cli_args.csv_config.clone());

        Ok(config)
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
                SourceConfig::new(&Some(p), input_factories_copy.clone(), self.cli_args.csv_config.clone()))
            .collect::<Vec<_>>();
        check_at_most_one_stdin(&*configs)?;

        Ok(configs)
    }
}

fn check_at_most_one_stdin(inputs: &[SourceConfig]) -> Result<(), String> {
    let stdin_count = inputs.iter().filter(|input| input.is_stdin()).count();
    if stdin_count > 1 {
        return Err("At most one stdin input is allowed.".to_owned());
    }
    Ok(())
}
