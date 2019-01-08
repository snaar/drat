use result::{CliResult, CliError};
use config::Config;
use input::input_factory::InputFactory;

pub struct Args<'a> {
    pub inputs: Vec<&'a str>,
    pub input_factories: Vec<Box<InputFactory>>,
    pub begin: Option<u64>,
    pub end: Option<u64>,
    pub timestamp_column: usize,
    pub output: Option<&'a str>,
    pub has_headers: bool,
    pub delimiter: u8,
}

impl <'a> Args<'a> {
    pub fn create_config(&mut self) -> CliResult<Config> {
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

        let config =
            Config::new(&input, self.delimiter, self.has_headers, self.timestamp_column, input_factories_copy);
        Ok(config)
    }

    pub fn create_configs(&mut self) -> CliResult<Vec<Config>> {
        let mut inputs_clone = self.inputs.clone();
        if inputs_clone.is_empty() {
            inputs_clone.push("-").to_owned(); // stdin
        }

        let mut input_factories_copy: Vec<Box<InputFactory>> = Vec::with_capacity(self.input_factories.len());
        for item in &mut self.input_factories {
            input_factories_copy.push(item.box_clone());
        }

        let configs = inputs_clone.into_iter()
            .map(move |p|
                Config::new(&Some(p), self.delimiter, self.has_headers, self.timestamp_column,  input_factories_copy.clone()))
            .collect::<Vec<_>>();
        check_at_most_one_stdin(&*configs)?;

        Ok(configs)
    }
}

fn check_at_most_one_stdin(inputs: &[Config]) -> Result<(), String> {
    let stdin_count = inputs.iter().filter(|input| input.is_stdin()).count();
    if stdin_count > 1 {
        return Err("At most one stdin input is allowed.".to_owned());
    }
    Ok(())
}
