use better_panic::Verbosity;

use chopper::chopper_cli::chopper_cli;
use chopper::error::CliResult;

fn main() -> CliResult<()> {
    better_panic::Settings::default()
        .verbosity(Verbosity::Full)
        .install();

    chopper_cli(None, None, None)
}
