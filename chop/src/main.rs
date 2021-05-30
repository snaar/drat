use better_panic::Verbosity;

use chopper::chopper::error::ChopperResult;
use chopper::chopper_cli::chopper_cli;

fn main() -> ChopperResult<()> {
    better_panic::Settings::default()
        .verbosity(Verbosity::Full)
        .install();

    chopper_cli(None, None, None)
}
