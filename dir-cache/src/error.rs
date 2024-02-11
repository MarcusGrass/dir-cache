use std::fmt::{Display, Formatter};
use std::path::PathBuf;

pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    UnexpectedDirCacheDoesNotExist,
    ReadManifest(std::io::Error),
    ParseManifest(String),
    SystemTime(std::time::SystemTimeError),
    SyncErr(Vec<(String, Error)>),
    BadManifestPath(String),
    ManifestStringAppendErr(std::fmt::Error),
    WriteContent(PathBuf, std::io::Error),
    ReadContent(PathBuf, std::io::Error),
    DeleteContent(PathBuf, std::io::Error),
    InsertWithErr(Box<dyn std::error::Error>),
    CacheInsertViolation(&'static str),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnexpectedDirCacheDoesNotExist => f.write_str(
                "OnlyIfExists open options was used, but the supplied path is not a dir-cache",
            ),
            Error::ReadManifest(e) => {
                f.write_fmt(format_args!("Failed to read manifest, io error: {e}"))
            }
            Error::SystemTime(e) => f.write_fmt(format_args!("Failed to get system time: {e}")),
            Error::SyncErr(e) => {
                f.write_fmt(format_args!("Failed to sync, errors: \n"))?;
                for (key, e) in e.iter() {
                    f.write_fmt(format_args!("{key}: {e}"))?;
                }
                Ok(())
            }
            Error::ManifestStringAppendErr(e) => {
                f.write_fmt(format_args!("Failed to append to manifest string: {e}"))
            }
            Error::WriteContent(p, e) => f.write_fmt(format_args!(
                "Failed to write content to disk at {p:?}: {e}"
            )),
            Error::ReadContent(p, e) => f.write_fmt(format_args!(
                "Failed to read content from disk at {p:?}: {e}"
            )),
            Error::DeleteContent(p, e) => f.write_fmt(format_args!(
                "Failed to delete content from disk at {p:?}: {e}"
            )),
            Error::ParseManifest(e) => {
                f.write_fmt(format_args!("Failed to parse manifest, cause: {e}"))
            }
            Error::BadManifestPath(s) => f.write_fmt(format_args!("Bad manifest path: {s}")),
            Error::InsertWithErr(user) => {
                f.write_fmt(format_args!("Failed to insert with: {user}"))
            }
            Error::CacheInsertViolation(opt) => {
                f.write_fmt(format_args!("Cache insert violation: {opt}"))
            }
        }
    }
}
