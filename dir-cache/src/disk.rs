use crate::error::{Error, Result};
use std::collections::VecDeque;
use std::fs::{DirEntry, Metadata};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum FileObjectExists {
    No,
    AsDir,
    AsFile,
}

pub(crate) struct ContentItems {
    manifest_file: PathBuf,
    generation_files: VecDeque<PathBuf>,
}

pub(crate) fn read_all_in_dir<F: FnMut(&Path, &Metadata) -> Result<()>>(
    path: &Path,
    mut func: F,
) -> Result<()> {
    for e in
        std::fs::read_dir(path).map_err(|e| Error::ReadContent("Failed to read dir", Some(e)))?
    {
        let entry = e.map_err(|e| Error::ReadContent("Failed to read dir entry", Some(e)))?;
        let entry_path = entry.path();
        let entry_md = entry
            .metadata()
            .map_err(|e| Error::ReadContent("Failed to read entry metadata", Some(e)))?;
        func(&entry_path, &entry_md)?;
    }
    Ok(())
}

#[inline]
pub(crate) fn file_name_utf8(path: &Path) -> Result<&str> {
    let file_name = path
        .to_str()
        .ok_or_else(|| Error::ReadContent("File has no utf8 file name", None))?;
    Ok(file_name)
}

pub(crate) fn expect_dir_at(path: &Path) -> Result<()> {
    let res = exists(path)?;
    match res {
        FileObjectExists::AsDir => Ok(()),
        FileObjectExists::No => Err(Error::ReadContent("Failed expect dir, found nothing", None)),
        FileObjectExists::AsFile => Err(Error::ReadContent("Failed expect dir, found file", None)),
    }
}
#[inline]
pub(crate) fn ensure_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|e| Error::WriteContent("Failed to ensure dir", Some(e)))?;
    Ok(())
}

#[inline]
pub(crate) fn create_dir_if_missing(path: &Path) -> Result<()> {
    // Very inefficient, could use `create_dir_all`, but then there's the potential to
    // create extra dirs where they aren't wanted.
    match exists(path)? {
        FileObjectExists::No => std::fs::create_dir(path).map_err(|e| {
            Error::WriteContent("Failed create dir if missing, failed create", Some(e))
        }),
        FileObjectExists::AsDir => Ok(()),
        FileObjectExists::AsFile => Err(Error::ReadContent(
            "Failed create dir, expected nothing or an already existing dir, found a file",
            None,
        )),
    }
}

pub(crate) fn exists(path: &Path) -> Result<FileObjectExists> {
    match std::fs::metadata(path) {
        Ok(md) => {
            if md.is_dir() {
                Ok(FileObjectExists::AsDir)
            } else if md.is_file() {
                Ok(FileObjectExists::AsFile)
            } else {
                Err(Error::ReadContent("Invalid metadata, was symlink", None))
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(FileObjectExists::No),
        Err(e) => Err(Error::ReadContent(
            "Failed to read metadata to check path existence",
            Some(e),
        )),
    }
}

pub(crate) fn read_metadata_if_present(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(Some(content)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::ReadContent("Failed to read metadata", Some(e))),
    }
}
pub(crate) fn read_raw_if_present(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(content) => Ok(Some(content)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::ReadContent("Failed to read file", Some(e))),
    }
}

pub(crate) fn ensure_removed_file(path: &Path) -> Result<()> {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != ErrorKind::NotFound {
            return Err(Error::DeleteContent(
                "Failed to ensure file was removed",
                Some(e),
            ));
        }
    }
    Ok(())
}

pub(crate) fn try_remove_dir(path: &Path) -> Result<()> {
    let mut any_dirs = false;
    read_all_in_dir(path, |entry_path, entry_metadata| {
        if entry_metadata.is_dir() {
            any_dirs = true;
        } else if entry_metadata.is_file() {
            ensure_removed_file(entry_path)?
        }
        Ok(())
    })?;
    if !any_dirs {
        std::fs::remove_dir(path)
            .map_err(|e| Error::DeleteContent("Failed to remove dir", Some(e)))?;
    }
    Ok(())
}
