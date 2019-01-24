pub fn parse_into_delimiter(str: &str) -> Result<u8, String> {
    /* Code in this function was adapted from public domain xsv project. */
    match &*str {
        r"\t" => Ok(b'\t'),
        s => {
            if s.len() != 1 {
                let msg = format!(
                    "Error: specified delimiter '{}' is not a single ASCII character.", s);
                return Err(msg);
            }
            let c = s.chars().next().unwrap();
            if c.is_ascii() {
                Ok(c as u8)
            } else {
                let msg = format!(
                    "Error: specified delimiter '{}' is not an ASCII character.", c);
                Err(msg)
            }
        }
    }
}
