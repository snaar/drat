use std::fmt::Display;

use serde::ser::Error as SerdeSerError;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum SerError {
    #[error("serde error: {0}")]
    Serde(String),
    #[error("value type {0} is not supported")]
    TypeNotSupported(String),
    #[error(
        "'none' when serializing header is not supported, \
        since underlying type information is not provided by serde, \
        serialize row with 'some' variant"
    )]
    NoneInHeader,
    #[error("only u8 buffers are supported, type {0} buffer seen instead")]
    NotAByteBuf(String),
    #[error("only string keys are supported, type {0} key seen instead")]
    NotAStringKey(String),
    #[error("only u64 timestamps are supported, type {0} timestamp seen instead")]
    NotAU64Timestamp(String),
    #[error("couldn't find field {0}, specified as timestamp field")]
    TimestampFieldNotFound(String),
    #[error("couldn't find timestamp field")]
    NoTimestampField,
    #[error(
        "timestamp field locator must be by name for data that has names (i.e. structures) \
        and must by index for data that does not (i.e. tuples)"
    )]
    InvalidTimestampFieldLocator,
    #[error("timestamp field should be u64")]
    InvalidTimestampFieldType,
    #[error("CliError: {0}")]
    CliError(#[from] crate::error::Error),
}

impl SerError {
    pub fn type_not_supported<T>(type_name: T) -> Self
    where
        T: Display,
    {
        SerError::TypeNotSupported(type_name.to_string())
    }

    pub fn not_a_byte_buf<T>(type_name: T) -> Self
    where
        T: Display,
    {
        SerError::NotAByteBuf(type_name.to_string())
    }

    pub fn not_a_string_key<T>(type_name: T) -> Self
    where
        T: Display,
    {
        SerError::NotAStringKey(type_name.to_string())
    }

    pub fn not_a_u64_timestamp<T>(type_name: T) -> Self
    where
        T: Display,
    {
        SerError::NotAU64Timestamp(type_name.to_string())
    }

    pub fn timestamp_field_not_found<T>(field_id: T) -> Self
    where
        T: Display,
    {
        SerError::TimestampFieldNotFound(field_id.to_string())
    }
}

impl SerdeSerError for SerError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        SerError::Serde(msg.to_string())
    }
}
