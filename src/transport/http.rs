use std::io;
use std::path::Path;

use reqwest::{blocking::Client, Url};

use crate::transport::transport_factory::TransportFactory;

#[derive(Clone)]
pub struct Http;

impl TransportFactory for Http {
    fn can_open(&self, path: &Path) -> bool {
        path.starts_with("http://") || path.starts_with("https://") || path.starts_with("ftp://")
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>> {
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

    fn box_clone(&self) -> Box<dyn TransportFactory> {
        Box::new((*self).clone())
    }

    fn factory_name(&self) -> &str {
        "http"
    }
}
