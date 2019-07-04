use std::io;
use std::path::PathBuf;

use reqwest::{Url, Client};

use crate::transport::transport_factory::TransportFactory;

#[derive(Clone)]
pub struct Http;

impl TransportFactory for Http {
    fn can_open(&self, path: &PathBuf) -> bool {
        path.starts_with("http://") || path.starts_with("https://") || path.starts_with("ftp://")
    }

    fn open(&self, path: &PathBuf) -> io::Result<Box<io::Read+'static>> {
        let url: Url = path.to_str().unwrap().parse().unwrap();
        let client = Client::new();
        let response = match client.get(url).send() {
            Ok(r) => r,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::Other, err))
            }
        };

        let reader = io::BufReader::new(response);
        Ok(Box::new(reader))
    }

    fn box_clone(&self) -> Box<TransportFactory> {
        Box::new((*self).clone())
    }

    fn factory_name(&self) -> &str {
        "http"
    }
}