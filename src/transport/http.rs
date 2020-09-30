use std::io;
use std::path::Path;

use crate::transport::transport_factory::TransportFactory;

#[derive(Clone)]
pub struct Http;

impl TransportFactory for Http {
    fn can_open(&self, path: &Path) -> bool {
        path.starts_with("http://") || path.starts_with("https://")
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn io::Read>> {
        let response = ureq::get(path.to_str().unwrap()).call();

        if response.ok() {
            Ok(Box::new(response.into_reader()))
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "http error for {} - status: {}; string: {}",
                    path.to_str().unwrap(),
                    response.status(),
                    response.into_string()?
                ),
            ))
        }
    }

    fn box_clone(&self) -> Box<dyn TransportFactory> {
        Box::new((*self).clone())
    }

    fn factory_name(&self) -> &str {
        "http"
    }
}
