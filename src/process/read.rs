use crate::args::Args;
use crate::read_filter::ReadFilter;
use crate::result::CliResult;

pub fn run(mut argv: Args) -> CliResult<()> {
    let reader = ReadFilter::new_from_args(&argv);
    let output: Option<&str> = argv.output.clone();
    let mut config = argv.create_config().unwrap();

    reader.read(&mut config, &output)?;
    Ok(())
}
