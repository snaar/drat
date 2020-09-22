use crate::chopper::chopper::Source;
use crate::chopper::types::FieldType;
use crate::error::{CliResult, Error};
use crate::source::csv_configs::CSVOutputConfig;
use std::collections::HashMap;

pub fn parse_into_delimiter(str: &str) -> CliResult<u8> {
    /* Code in this function was adapted from public domain xsv project. */
    match &*str {
        r"\t" => Ok(b'\t'),
        s => {
            if s.len() != 1 {
                return Err(Error::from(format!(
                    "Error: specified delimiter '{}' is not a single ASCII character.",
                    s
                )));
            }
            let c = s.chars().next().unwrap();
            if c.is_ascii() {
                Ok(c as u8)
            } else {
                Err(Error::from(format!(
                    "Error: specified delimiter '{}' is not an ASCII character.",
                    c
                )))
            }
        }
    }
}

pub fn create_csv_output_config_from_source(
    sources: &mut Vec<Box<dyn Source>>,
    delimiter: &str,
) -> CSVOutputConfig {
    let mut some_sources_have_native_timestamps = false;
    for source in sources {
        if source.has_native_timestamp_column() {
            some_sources_have_native_timestamps = true;
            break;
        }
    }
    CSVOutputConfig::new(delimiter, some_sources_have_native_timestamps)
}

pub fn guess_delimiter(row: &str, possible_delimiters: &[u8]) -> u8 {
    assert_ne!(possible_delimiters.len(), 0);

    let mut counts: HashMap<u8, u32> = HashMap::new();

    for d in possible_delimiters {
        counts.insert(*d, 0);
    }

    for c in row.chars() {
        counts.entry(c as u8).and_modify(|count| *count += 1);
    }

    let mut sorted = possible_delimiters.to_vec();
    sorted.sort_by(|a, b| counts[b].cmp(&counts[a]));

    *sorted.get(0).unwrap()
}

/// we want to be on the conservative side, since we'd rather not silently lose data by
/// skipping the first data row by mistaking it for header;
/// returning false when confused is safest thing - in worst case parsing will fail
/// later and user will have to manually configure the csv file format
pub fn guess_has_header(line1: Option<&String>, line2: Option<&String>, delimiter: u8) -> bool {
    let line1 = match line1 {
        None => return false,
        Some(line) => line,
    };
    let line2 = match line2 {
        None => return false,
        Some(line) => line,
    };

    let line1: Vec<FieldType> = line1
        .split(delimiter as char)
        .map(|s| guess_type(s.trim()))
        .collect();
    let line2: Vec<FieldType> = line2
        .split(delimiter as char)
        .map(|s| guess_type(s.trim()))
        .collect();

    line1 != line2
}

pub fn guess_common_type(field_types: &[FieldType]) -> FieldType {
    if field_types.is_empty() {
        return FieldType::String;
    }

    let mut guess = type_upconvert(field_types[0]);

    for &field_type in field_types {
        if guess == FieldType::String {
            return guess;
        }

        let field_type = type_upconvert(field_type);
        guess = match field_type {
            FieldType::Double => FieldType::Double,
            FieldType::Long => guess, // at this point guess can be only long or double, and we want double to take precedence over long
            _ => return FieldType::String,
        }
    }

    guess
}

fn type_upconvert(field_type: FieldType) -> FieldType {
    match field_type {
        FieldType::Boolean => FieldType::String,
        FieldType::ByteBuf => FieldType::String,
        FieldType::String => FieldType::String,
        FieldType::Byte | FieldType::Char | FieldType::Short | FieldType::Int | FieldType::Long => {
            FieldType::Long
        }
        FieldType::Float | FieldType::Double => FieldType::Double,
    }
}

/// current limitations: only long, double, and string; rust format only
pub fn guess_type(str: &str) -> FieldType {
    if str.parse::<i64>().is_ok() {
        return FieldType::Long;
    };
    if str.parse::<f64>().is_ok() {
        return FieldType::Double;
    }
    FieldType::String
}

#[cfg(test)]
mod tests {
    use crate::chopper::types::FieldType;
    use crate::util::csv_util::{guess_has_header, guess_type};

    #[test]
    fn test_guess_has_header() {
        assert!(guess_has_header(
            Some(&"zzz".to_string()),
            Some(&"123".to_string()),
            b','
        ));
        assert!(!guess_has_header(
            Some(&"zzz".to_string()),
            Some(&"zzz".to_string()),
            b','
        ));
        assert!(!guess_has_header(
            Some(&"123".to_string()),
            Some(&"123".to_string()),
            b','
        ));
        assert!(guess_has_header(
            Some(&"zzz".to_string()),
            Some(&"123".to_string()),
            b','
        ));
        assert!(guess_has_header(
            Some(&"zzz,zzz,zzz,zzz".to_string()),
            Some(&"zzz,zzz,123,zzz".to_string()),
            b','
        ));
        assert!(!guess_has_header(
            Some(&"zzz,zzz,123,zzz".to_string()),
            Some(&"zzz,zzz, 123 ,zzz".to_string()),
            b','
        ));
        assert!(guess_has_header(
            Some(&"123".to_string()),
            Some(&"123.4".to_string()),
            b','
        ));
    }

    #[test]
    fn test_guess_type() {
        assert_eq!(guess_type("zzz"), FieldType::String);
        assert_eq!(guess_type("123"), FieldType::Long);
        assert_eq!(guess_type("123.4"), FieldType::Double);
    }
}
