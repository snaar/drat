use std::fs::File;

pub mod file;
pub mod seekable_factory;
pub mod seekable_transport;

pub trait ReadSeek: std::io::Read + std::io::Seek {}

impl ReadSeek for File {}
