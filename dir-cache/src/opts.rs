use crate::error::Error;
use crate::{DirCache, Manifest, MANIFEST_FILE_NAME, MANIFEST_VERSION};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::Duration;

pub struct CacheOptionsBuilder {
    cache_open_opt: Option<CacheOpenOptions>,
    cache_insert_opt: Option<CacheInsertOption>,
    cache_write_opt: Option<CacheWriteOpt>,
    sync_opt: Option<SyncErrorOpt>,
    flush_opt: Option<MemPushOpt>,
    pull_opt: Option<MemPullOpt>,
    expiration_opt: Option<ExpirationOpt>,
}

impl CacheOptionsBuilder {
    pub fn new() -> Self {
        Self {
            cache_open_opt: None,
            cache_insert_opt: None,
            cache_write_opt: None,
            sync_opt: None,
            flush_opt: None,
            pull_opt: None,
            expiration_opt: None,
        }
    }

    pub fn with_cache_open_options(mut self, cache_open_options: CacheOpenOptions) -> Self {
        self.cache_open_opt = Some(cache_open_options);
        self
    }

    pub fn with_cache_insert_options(mut self, cache_insert_option: CacheInsertOption) -> Self {
        self.cache_insert_opt = Some(cache_insert_option);
        self
    }

    pub fn with_sync_error_options(mut self, sync_error_opt: SyncErrorOpt) -> Self {
        self.sync_opt = Some(sync_error_opt);
        self
    }

    pub fn with_mem_flush_options(mut self, mem_flush_opt: MemPushOpt) -> Self {
        self.flush_opt = Some(mem_flush_opt);
        self
    }

    pub fn with_mem_pull_options(mut self, mem_pull_opt: MemPullOpt) -> Self {
        self.pull_opt = Some(mem_pull_opt);
        self
    }

    pub fn open(self, path: PathBuf) -> crate::error::Result<DirCache> {
        let open = self.cache_open_opt.unwrap_or_default();
        let expect_manifest = path.join(MANIFEST_FILE_NAME);
        let ttl = self.expiration_opt.unwrap_or(ExpirationOpt::DoNothing);
        let manifest = match open.dir_open {
            DirOpen::OnlyIfExists => match std::fs::read_to_string(expect_manifest) {
                Ok(raw) => Manifest::deserialize(
                    &path,
                    &raw,
                    MANIFEST_VERSION,
                    open.eager_load_to_ram,
                    ttl,
                )?,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    return Err(Error::UnexpectedDirCacheDoesNotExist);
                }
                Err(e) => {
                    return Err(Error::ReadManifest(e));
                }
            },
            DirOpen::CreateIfMissing => match std::fs::read_to_string(expect_manifest) {
                Ok(raw) => Manifest::deserialize(
                    &path,
                    &raw,
                    MANIFEST_VERSION,
                    open.eager_load_to_ram,
                    ttl,
                )?,
                Err(e) if e.kind() == ErrorKind::NotFound => match std::fs::metadata(&path) {
                    Ok(md) => {
                        if !md.is_dir() {
                            return Err(Error::BadManifestPath(format!(
                                "Path {path:?} is not a dir"
                            )));
                        }
                        Manifest {
                            version: MANIFEST_VERSION,
                            store: HashMap::default(),
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        std::fs::create_dir(&path).map_err(|e| {
                            Error::BadManifestPath(format!(
                                "Could not create dir-cache at {path:?}: {e}"
                            ))
                        })?;
                        Manifest {
                            version: MANIFEST_VERSION,
                            store: HashMap::default(),
                        }
                    }
                    Err(e) => {
                        return Err(Error::BadManifestPath(format!(
                            "Could not read dir-cache metadata {e}"
                        )));
                    }
                },
                Err(e) => {
                    return Err(Error::ReadManifest(e));
                }
            },
        };
        Ok(DirCache {
            path,
            insert_opt: self.cache_insert_opt.unwrap_or_default(),
            manifest,
            write_opt: self.cache_write_opt.unwrap_or_default(),
            sync_opt: self.sync_opt.unwrap_or_default(),
            push_opt: self.flush_opt.unwrap_or_default(),
            pull_opt: self.pull_opt.unwrap_or_default(),
            expiration_opt: Default::default(),
        })
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct CacheOpenOptions {
    pub(crate) dir_open: DirOpen,
    pub(crate) eager_load_to_ram: bool,
}

impl CacheOpenOptions {
    pub fn new(dir_open: DirOpen, eager_load_to_ram: bool) -> Self {
        Self {
            dir_open,
            eager_load_to_ram,
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub enum DirOpen {
    /// Only open if a dir-cache already exists at the path, otherwise fail
    OnlyIfExists,
    /// Create a dir-cache if none exists at the path
    #[default]
    CreateIfMissing,
}

#[derive(Debug, Copy, Clone, Default)]
pub enum CacheInsertOption {
    /// Insert or update
    #[default]
    Upsert,
    /// Only write if no value is present
    OnlyIfMissing,
}

/// When the dir-cache should sync file-contents
#[derive(Debug, Copy, Clone, Default)]
pub enum CacheWriteOpt {
    /// Only write to disk manually
    ManualOnly,
    /// Only write to disk manually or when the [`DirCache`] is dropped
    AutoOnDrop,
    /// Write to disk immediately
    #[default]
    OnWrite,
}

/// Memory flush option, determines whether the data should be retained in memory when written to disk
#[derive(Debug, Copy, Clone, Default)]
pub enum MemPushOpt {
    /// Keep the data in memory after writing
    RetainOnWrite,
    /// Remove the data from memory after writing
    #[default]
    DumpOnWrite,
}

/// Memory pull options, determines whether data should be cached in RAM when pulled from disk
#[derive(Debug, Copy, Clone, Default)]
pub enum MemPullOpt {
    #[default]
    KeepInMemoryOnRead,
    DontKeepInMemoryOnRead,
}

/// Expiration options, what to do when an entry is found to be expired
#[derive(Debug, Copy, Clone, Default)]
pub enum ExpirationOpt {
    /// Just leave it
    #[default]
    DoNothing,
    /// Try to delete it from disk
    DeleteAfter(Duration),
}

/// If an error is encountered when syncing the full dir-cache
#[derive(Debug, Copy, Clone, Default)]
pub enum SyncErrorOpt {
    /// Fail immediately, perhaps failing to write good values
    FailFast,
    /// Skip unwritable entries, but write as many as possible
    #[default]
    BestAttempt,
}
