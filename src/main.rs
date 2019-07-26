extern crate chopper_lib;
use chopper_lib::chopper_cli::chopper_cli;
use chopper_lib::error;

fn main() {
    error::handle_drive_error(chopper_cli(None, None, None));
}
