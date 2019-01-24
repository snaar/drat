use crate::result::{CliResult, CliError};
use crate::source_config::{CSVConfig, SourceConfig};
use crate::input::input_factory::InputFactory;

pub struct Args<'a> {
    pub inputs: Vec<&'a str>,
    pub input_factories: Vec<Box<InputFactory>>,
    pub begin: Option<u64>,
    pub end: Option<u64>,
    pub output: Option<&'a str>,
    pub csv_config: CSVConfig,
}

impl <'a> Args<'a> {
    pub fn create_config(&mut self) -> CliResult<SourceConfig> {
        if self.inputs.len() > 1 {
            return Err(CliError::Other("Error: more than one input file specified.".to_owned()));
        }
        let input: Option<&str> = match self.inputs.len() {
            0 => None,
            1 => Some(self.inputs.get(0).unwrap()),
            _ => unreachable!(),
        };

        let mut input_factories_copy: Vec<Box<InputFactory>> = Vec::with_capacity(self.input_factories.len());
        for item in &mut self.input_factories {
            input_factories_copy.push(item.box_clone());
        }
        let config = SourceConfig::new(&input, input_factories_copy, self.csv_config.clone());

        Ok(config)
    }

    pub fn create_configs(&mut self) -> CliResult<Vec<SourceConfig>> {
        let mut inputs_clone = self.inputs.clone();
        if self.inputs.is_empty() {
            inputs_clone.push("-"); // stdin
        }

        let mut input_factories_copy: Vec<Box<InputFactory>> = Vec::with_capacity(self.input_factories.len());
        for item in &mut self.input_factories {
            input_factories_copy.push(item.box_clone());
        }

        let configs = inputs_clone.into_iter()
            .map(move |p|
                SourceConfig::new(&Some(p), input_factories_copy.clone(), self.csv_config.clone()))
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
