use actix_web::Error;
use bytes::Bytes;
use futures::Async;
use futures::Poll;
use futures::Stream;

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
