extern crate backtrace;

extern crate chopper_lib;
use chopper_lib::chopper_cli::chopper_cli;
use chopper_lib::error;
use chopper_lib::input::input_factory::InputFactory;
use chopper_lib::input::file::FileInput;
use chopper_lib::input::http::Http;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let vec: Vec<Box<InputFactory>> = vec![
        Box::new(http),
        Box::new(file)];
    error::handle_drive_error(chopper_cli(vec));
}
