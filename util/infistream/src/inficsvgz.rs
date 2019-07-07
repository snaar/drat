use actix_web::Error;
use bytes::Bytes;
use flate2::Compression;
use flate2::write;
use futures::Async;
use futures::Poll;
use futures::Stream;
use std::io::prelude::*;

pub struct InfiCSVGZ {
    timestamp: u64,
    encoder: write::GzEncoder<Vec<u8>>,
}

impl InfiCSVGZ {
    pub fn new() -> InfiCSVGZ {
        InfiCSVGZ {
            timestamp: 0,
            encoder: write::GzEncoder::new(Vec::new(), Compression::default()),
        }
    }
}

impl Stream for InfiCSVGZ {
    type Item = Bytes;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let v = self.timestamp;
        self.timestamp += 1;

        let row = format!("{},a{},b{},c{},d{},e{},f{}\n", v, v, v, v, v, v, v);
        self.encoder.write(row.as_bytes()).unwrap();

        let encoder_inner_buf = self.encoder.get_mut();
        let compressed_data = encoder_inner_buf.clone();
        encoder_inner_buf.clear();

        Ok(Async::Ready(Some(Bytes::from(compressed_data))))
    }
}
