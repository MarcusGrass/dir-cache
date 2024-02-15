use crate::error::{Error, Result};
use crate::{DirCacheInner};
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

pub struct CacheOptionsBuilder {
    cache_open_opt: Option<CacheOpenOptions>,
    cache_insert_opt: Option<CacheInsertOption>,
    cache_write_opt: Option<ManifestWriteOpt>,
    sync_opt: Option<SyncErrorOpt>,
    flush_opt: Option<MemPushOpt>,
    pull_opt: Option<MemPullOpt>,
    generation_opt: Option<GenerationOpt>,
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
            generation_opt: None,
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

    pub fn open(self, path: PathBuf) -> crate::error::Result<DirCacheInner> {
        todo!()
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
pub enum ManifestWriteOpt {
    /// Only write to disk manually
    ManualOnly,
    /// Only write to disk manually or when the [`DirCacheInner`] is dropped
    AutoOnDrop,
    /// Write to disk immediately
    #[default]
    OnWrite,
}

/// Memory flush option, determines whether the data should be retained in memory when written to disk
#[derive(Debug, Copy, Clone, Default)]
pub enum MemPushOpt {
    /// Keep the data in memory after writing
    RetainAndWrite,
    /// Write the data into ram, don't automatically sync to disk
    MemoryOnly,
    /// Remove the data from memory after writing
    #[default]
    PassthroughWrite,
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

impl ExpirationOpt {
    #[inline]
    pub(crate) fn as_dur(self) -> Duration {
        match self {
            // End of all times
            ExpirationOpt::DoNothing => Duration::MAX,
            ExpirationOpt::DeleteAfter(dur) => dur,
        }
    }
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

/// How to treat generations of data
#[derive(Debug, Copy, Clone)]
pub struct GenerationOpt {
    /// How many old copies to keep
    pub(crate) max_generations: NonZeroUsize,
    /// How to encode older generations
    pub(crate) old_gen_encoding: Encoding,
    /// When a value in any generation is expired
    pub(crate) expiration: ExpirationOpt,
}

impl Default for GenerationOpt {
    #[inline]
    fn default() -> Self {
        Self::new(NonZeroUsize::MIN, Encoding::Plain, ExpirationOpt::DoNothing)
    }
}

impl GenerationOpt {
    pub const fn new(max_generations: NonZeroUsize, old_gen_encoding: Encoding, expiration: ExpirationOpt) -> Self {
        Self { max_generations, old_gen_encoding, expiration }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Encoding {
    Plain,
}

impl Encoding {
    pub(crate) fn serialize(&self) -> impl Display {
        match self {
            Encoding::Plain => 0u8,
        }
    }

    pub(crate) fn deserialize(s: &str) -> Result<Self> {
        match s {
            "0" => Ok(Self::Plain),
            v => Err(Error::ParseMetadata(format!("Failed to parse encoding from {v}")))
        }
    }
}

