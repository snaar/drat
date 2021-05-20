//TODO clone seems suboptimal, so hopefully in future will not need it
#[derive(Clone)]
pub enum TimestampFieldLocator {
    ByName(String),
    ByIndex(usize),
}
