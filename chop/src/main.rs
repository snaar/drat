use better_panic::Verbosity;

use chopper::chopper::error::ChopperResult;
use chopper::chopper_cli::ChopperCli;
use chopper::util::dc_factory::DCFactory;

fn main() -> ChopperResult<()> {
    better_panic::Settings::default()
        .verbosity(Verbosity::Full)
        .install();

    ChopperCli::new()
        .with_dc_factory(DCFactory::default())
        .run()
}
