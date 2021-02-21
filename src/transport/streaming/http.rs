use std::io;
use std::path::Path;

use crate::transport::streaming::streaming_transport::StreamingTransport;

#[derive(Clone)]
pub struct HttpTransport;

impl StreamingTransport for HttpTransport {
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

    fn box_clone(&self) -> Box<dyn StreamingTransport> {
        Box::new((*self).clone())
    }

    fn name(&self) -> &str {
        "http"
    }
}
