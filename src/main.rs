use chopper_lib::{chopper_cli::chopper_cli, error};

fn main() {
    error::handle_drive_error(chopper_cli(None, None, None));
}
