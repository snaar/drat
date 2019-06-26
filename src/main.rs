extern crate drat_lib;

use drat_lib::drat_cli::drat_cli;
use drat_lib::error;
use drat_lib::input::input_factory::InputFactory;
use drat_lib::input::file::FileInput;
use drat_lib::input::http::Http;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let vec: Vec<Box<InputFactory>> = vec![
        Box::new(http),
        Box::new(file)];
    error::handle_drive_error(drat_cli(vec));
}
