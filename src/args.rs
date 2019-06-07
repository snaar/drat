use crate::result::CliResult;
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
    pub fn new(inputs: Vec<String>, output: Option<String>, begin: Option<u64>, end: Option<u64>,
                                    csv_config: CSVConfig, input_factories: Vec<Box<InputFactory>>) -> Self {
        let data_range = DataRange { begin, end };
        let cli_args = CliArgs { inputs, output, data_range,  csv_config };
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
