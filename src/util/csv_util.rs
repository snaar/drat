use crate::error::{CliResult, Error};

pub fn parse_into_delimiter(str: &str) -> CliResult<u8> {
    /* Code in this function was adapted from public domain xsv project. */
    match &*str {
        r"\t" => Ok(b'\t'),
        s => {
            if s.len() != 1 {
                return Err(Error::from(
                    format!("Error: specified delimiter '{}' is not a single ASCII character.", s)))
            }
            let c = s.chars().next().unwrap();
            if c.is_ascii() {
                Ok(c as u8)
            } else {
                Err(Error::from(
                    format!("Error: specified delimiter '{}' is not an ASCII character.", c)))
            }
        }
    }
}
