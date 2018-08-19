use futures::Stream;
use futures::Poll;
use futures::Async;
use bytes::Bytes;
use actix_web::Error;

pub struct InfiCSV {
    timestamp: u64,
}

impl InfiCSV {
    pub fn new() -> InfiCSV {
        InfiCSV {
            timestamp: 0,
        }
    }
}

impl Stream for InfiCSV {
    type Item = Bytes;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let v = self.timestamp;
        self.timestamp += 1;

        let row = format!("{},a{},b{},c{},d{},e{},f{}\n", v, v, v, v, v, v, v);
        Ok(Async::Ready(Some(Bytes::from(row))))
    }
}
