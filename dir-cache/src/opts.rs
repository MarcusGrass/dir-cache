use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::PathBuf;
use crate::{DirCache, Manifest, MANIFEST_FILE_NAME, MANIFEST_VERSION};
use crate::error::Error;

pub struct CacheOptionsBuilder {
    cache_open_opt: Option<CacheOpenOptions>,
    cache_insert_opt: Option<CacheInsertOption>,
    cache_read_opt: Option<CacheReadOpt>,
    cache_write_opt: Option<CacheWriteOpt>,
    sync_opt: Option<SyncErrorOpt>,
    flush_opt: Option<MemFlushOpt>,
}

impl CacheOptionsBuilder {
    pub fn new() -> Self {
        Self {
            cache_open_opt: None,
            cache_insert_opt: None,
            cache_read_opt: None,
            cache_write_opt: None,
            sync_opt: None,
            flush_opt: None,
        }
    }

    pub fn with_cache_open_options(mut self, cache_open_options: CacheOpenOptions)  -> Self {
        self.cache_open_opt = Some(cache_open_options);
        self
    }

    pub fn with_cache_insert_options(mut self, cache_insert_option: CacheInsertOption) -> Self {
        self.cache_insert_opt = Some(cache_insert_option);
        self
    }

    pub fn with_cache_read_options(mut self, cache_read_opt: CacheReadOpt) -> Self {
        self.cache_read_opt = Some(cache_read_opt);
        self
    }

    pub fn with_sync_error_options(mut self, sync_error_opt: SyncErrorOpt) -> Self {
        self.sync_opt = Some(sync_error_opt);
        self
    }

    pub fn with_mem_flush_options(mut self, mem_flush_opt: MemFlushOpt) -> Self {
        self.flush_opt = Some(mem_flush_opt);
        self
    }

    pub fn open(self, path: PathBuf) -> crate::error::Result<DirCache> {
        let open = self.cache_open_opt.unwrap_or_default();
        let read_opts = self.cache_read_opt.unwrap_or_default();
        let expect_manifest = path.join(MANIFEST_FILE_NAME);
        let manifest = match open {
            CacheOpenOptions::OnlyIfExists => match std::fs::read_to_string(expect_manifest) {
                Ok(raw) => Manifest::deserialize(&path, &raw, MANIFEST_VERSION, read_opts)?,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    return Err(Error::UnexpectedDirCacheDoesNotExist);
                }
                Err(e) => {
                    return Err(Error::ReadManifest(e));
                }
            },
            CacheOpenOptions::CreateIfMissing => match std::fs::read_to_string(expect_manifest) {
                Ok(raw) => Manifest::deserialize(&path, &raw, MANIFEST_VERSION, read_opts)?,
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
            read_opt: self.cache_read_opt.unwrap_or_default(),
            sync_opt: self.sync_opt.unwrap_or_default(),
            flush_opt: self.flush_opt.unwrap_or_default(),
        })
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub enum CacheOpenOptions {
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
    /// Only overwrite existing values
    OnlyIfExists,
    /// Only write if no value is present
    OnlyIfMissing,
}

/// When the dir-cache should read cached data into memory
#[derive(Debug, Copy, Clone, Default)]
pub enum CacheReadOpt {
    /// Read all cached data into ram on open (eager)
    OnOpen,
    /// Read cached data per-key when requested (lazy)
    #[default]
    OnRead,
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
pub enum MemFlushOpt {
    /// Keep the data in memory after writing
    RetainOnWrite,
    /// Remove the data from memory after writing
    #[default]
    FlushOnWrite,
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