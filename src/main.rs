extern crate drat_lib;
use dtoa;

use drat_lib::input::input_factory::InputFactory;
use drat_lib::input::file::FileInput;
use drat_lib::input::http::Http;
use drat_lib::drat_cli::drat_cli;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let mut vec: Vec<Box<InputFactory>> = Vec::new();
    vec.push(Box::new(http));
    vec.push(Box::new(file));
    drat_cli(vec);
}
