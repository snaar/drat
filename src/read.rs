use args::Args;
use read_filter::ReadFilter;
use result::CliResult;

pub fn run(argv: Args) -> CliResult<()> {
    let reader = ReadFilter::new_from_args(&argv);
    let config = argv.create_config().unwrap();

    reader.read(&config, &argv.output)?;
    Ok(())
}
