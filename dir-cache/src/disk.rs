use crate::error::{Error, Result};
use std::fs::Metadata;
use std::io::ErrorKind;
use std::path::Path;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum FileObjectExists {
    No,
    AsDir,
    AsFile,
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
pub(crate) fn ensure_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|e| Error::WriteContent("Failed to ensure dir", Some(e)))?;
    Ok(())
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
    if exists(path)? == FileObjectExists::No {
        return Ok(());
    }
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
