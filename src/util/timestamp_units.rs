#[derive(Debug, Copy, Clone)]
pub enum TimestampUnits {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl TimestampUnits {
    pub fn from_str(units: &str) -> TimestampUnits {
        match units {
            "s" => TimestampUnits::Seconds,
            "ms" => TimestampUnits::Millis,
            "us" => TimestampUnits::Micros,
            "ns" => TimestampUnits::Nanos,
            _ => panic!("unexpected timestamp units {}", units),
        }
    }

    pub fn to_suffix_str(&self) -> &str {
        match self {
            TimestampUnits::Seconds => "Seconds",
            TimestampUnits::Millis => "Millis",
            TimestampUnits::Micros => "Micros",
            TimestampUnits::Nanos => "Nanos",
        }
    }
}

pub const TIMESTAMP_UNITS: [(TimestampUnits, &'static str); 4] = [
    (TimestampUnits::Seconds, "seconds"),
    (TimestampUnits::Millis, "millis"),
    (TimestampUnits::Micros, "micros"),
    (TimestampUnits::Nanos, "nanos"),
];
