use args::Args;
use read_filter::ReadFilter;
use result::CliResult;

pub fn run(argv: Args) -> CliResult<()> {
    let reader = ReadFilter::new_from_args(&argv);
    let mut config = argv.create_config().unwrap();

    reader.read(&mut config, &argv.output)?;
    Ok(())
}
