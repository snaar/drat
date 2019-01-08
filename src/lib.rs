extern crate byteorder;
#[macro_use]
extern crate clap;
extern crate csv;
extern crate flate2;
extern crate lzf;
extern crate reqwest;

pub mod input;
#[macro_use]
mod process;
mod read;
mod write;

mod args;
mod config;
pub mod drat_cli;
mod file_record;
mod read_filter;
mod result;
mod util;
