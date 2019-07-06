extern crate chopper_lib;

use chopper_lib::chopper_cli::chopper_cli;
use chopper_lib::error;
use chopper_lib::transport::file::FileInput;
use chopper_lib::transport::http::Http;
use chopper_lib::transport::transport_factory::TransportFactory;

fn main() {
    let http: Http = Http;
    let file: FileInput = FileInput;
    let vec: Vec<Box<TransportFactory>> = vec![
        Box::new(http),
        Box::new(file)];
    error::handle_drive_error(chopper_cli(vec));
}
