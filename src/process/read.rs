use crate::args::Args;
use crate::process::driver::single_input_driver::SingleInputDriver;
use crate::result::CliResult;

pub fn run(mut argv: Args) -> CliResult<()> {
    let input_driver = SingleInputDriver::new_from_args(&argv);
    let output: Option<&str> = argv.output.clone();
    let mut config = argv.create_config().unwrap();
    input_driver.read(&mut config, &output)?;

    Ok(())
}
