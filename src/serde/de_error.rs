use std::fmt::Display;

use serde::de::Error as SerdeDeError;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum DeError {
    #[error("serde error: {0}")]
    Serde(String),
}

impl SerdeDeError for DeError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        DeError::Serde(msg.to_string())
    }
}
