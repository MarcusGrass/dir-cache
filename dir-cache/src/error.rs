use std::fmt::{Display, Formatter};

pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    Arithmetic(&'static str),
    ParseManifest(String),
    ParseMetadata(String),
    SystemTime(std::time::SystemTimeError),
    Open(String),
    WriteContent(String, Option<std::io::Error>),
    ReadContent(String, Option<std::io::Error>),
    DeleteContent(String, Option<std::io::Error>),
    InsertWithErr(Box<dyn std::error::Error>),
    DangerousKey(String),
    EncodingError(String),
    PathRelativize(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Arithmetic(s) => f.write_fmt(format_args!("Arithmetic failed: {s}")),
            Error::SystemTime(e) => f.write_fmt(format_args!("Failed to get system time: {e}")),
            Error::WriteContent(p, e) => f.write_fmt(format_args!(
                "Failed to write content to disk at {p:?}, source: {e:?}"
            )),
            Error::ReadContent(p, e) => f.write_fmt(format_args!(
                "Failed to read content from disk at {p:?}, source: {e:?}"
            )),
            Error::DeleteContent(p, e) => f.write_fmt(format_args!(
                "Failed to delete content from disk at {p:?}, source: {e:?}"
            )),
            Error::ParseManifest(e) => {
                f.write_fmt(format_args!("Failed to parse manifest, cause: {e}"))
            }
            Error::Open(s) => f.write_fmt(format_args!("Bad manifest path: {s}")),
            Error::InsertWithErr(user) => {
                f.write_fmt(format_args!("Failed to insert with: {user}"))
            }
            Error::ParseMetadata(s) => f.write_fmt(format_args!("Failed to parse metadata: '{s}'")),
            Error::DangerousKey(e) => f.write_fmt(format_args!("Dangerous key used: {e}")),
            Error::EncodingError(e) => f.write_fmt(format_args!("Failed to encode content: {e}")),
            Error::PathRelativize(s) => {
                f.write_fmt(format_args!("Failed to relativize paths: {s}"))
            }
        }
    }
}
