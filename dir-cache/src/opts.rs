use crate::disk::{ensure_dir, exists, FileObjectExists};
use crate::error::{Error, Result};
use crate::{DirCache, DirCacheInner};
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

/// Options for controlling the behavior of operations on a [`DirCache`].
/// See the specific options for more details
#[derive(Debug, Copy, Clone, Default)]
pub struct DirCacheOpts {
    pub mem_pull_opt: MemPullOpt,
    pub mem_push_opt: MemPushOpt,
    pub generation_opt: GenerationOpt,
    pub sync_opt: SyncOpt,
}

impl DirCacheOpts {
    #[must_use]
    pub const fn new(
        mem_pull_opt: MemPullOpt,
        mem_push_opt: MemPushOpt,
        generation_opt: GenerationOpt,
        sync_opt: SyncOpt,
    ) -> Self {
        Self {
            mem_pull_opt,
            mem_push_opt,
            generation_opt,
            sync_opt,
        }
    }

    #[must_use]
    pub const fn with_mem_pull_opt(mut self, mem_pull_opt: MemPullOpt) -> Self {
        self.mem_pull_opt = mem_pull_opt;
        self
    }

    #[must_use]
    pub const fn with_mem_push_opt(mut self, mem_push_opt: MemPushOpt) -> Self {
        self.mem_push_opt = mem_push_opt;
        self
    }

    #[must_use]
    pub const fn with_generation_opt(mut self, generation_opt: GenerationOpt) -> Self {
        self.generation_opt = generation_opt;
        self
    }

    #[must_use]
    pub const fn with_sync_opt(mut self, sync_opt: SyncOpt) -> Self {
        self.sync_opt = sync_opt;
        self
    }

    /// Use these [`DirCacheOpts`] to open a [`DirCache`].
    /// # Errors
    /// Depending on the open options a directory already being present or not may cause failure.
    /// Various io-errors, from creating the [`DirCache`].
    pub fn open(self, path: &Path, cache_open_options: CacheOpenOptions) -> Result<DirCache> {
        match cache_open_options.dir_open {
            DirOpenOpt::OnlyIfExists => {
                match exists(path)? {
                    FileObjectExists::AsDir => {}
                    FileObjectExists::No => {
                        return Err(Error::Open(format!(
                            "Opened with OnlyIfExists but path {path:?} does not exist"
                        )));
                    }
                    FileObjectExists::AsFile => {
                        return Err(Error::Open(format!(
                            "Wanted to open at {path:?}, but path is a file"
                        )));
                    }
                };
            }
            DirOpenOpt::CreateIfMissing => {
                ensure_dir(path)?;
            }
        }
        let inner = DirCacheInner::read_from_disk(
            path.to_path_buf(),
            cache_open_options.eager_load_to_ram,
            self.generation_opt,
        )?;
        Ok(DirCache { inner, opts: self })
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct CacheOpenOptions {
    pub(crate) dir_open: DirOpenOpt,
    pub(crate) eager_load_to_ram: bool,
}

impl CacheOpenOptions {
    #[must_use]
    pub fn new(dir_open: DirOpenOpt, eager_load_to_ram: bool) -> Self {
        Self {
            dir_open,
            eager_load_to_ram,
        }
    }
}

/// Options for when a [`DirCache`] is opened
#[derive(Debug, Copy, Clone, Default)]
pub enum DirOpenOpt {
    /// Only open if a `dir-cache` directory already exists at the path, otherwise fail
    OnlyIfExists,
    /// Create a `dir-cache` directory if none exists at the path
    #[default]
    CreateIfMissing,
}

/// Memory push option, determines whether the data should be retained in memory when written to disk
#[derive(Debug, Copy, Clone, Default)]
pub enum MemPushOpt {
    /// Keep the data in memory after writing
    RetainAndWrite,
    /// Write the data into memory, don't automatically sync to disk
    MemoryOnly,
    /// Remove the data from memory after writing
    #[default]
    PassthroughWrite,
}

/// Memory pull options, determines whether data should be cached in memory when pulled from disk,
/// such as during a `get` operation.
#[derive(Debug, Copy, Clone, Default)]
pub enum MemPullOpt {
    /// Reads the value from disk, then retains it in memory
    #[default]
    KeepInMemoryOnRead,
    /// Reads the value from disk, but does not keep it stored in memory
    DontKeepInMemoryOnRead,
}

/// Expiration options, how to determine if an entry has expired
#[derive(Debug, Copy, Clone, Default)]
pub enum ExpirationOpt {
    /// Entries never expire
    #[default]
    NoExpiry,
    /// Entries expire after
    ExpiresAfter(Duration),
}

impl ExpirationOpt {
    #[inline]
    pub(crate) fn as_dur(self) -> Duration {
        match self {
            // End of all times
            ExpirationOpt::NoExpiry => Duration::MAX,
            ExpirationOpt::ExpiresAfter(dur) => dur,
        }
    }
}

/// Data can be saved as generations (keeping older values of keys),
/// these options determine how those generations are managed
#[derive(Debug, Copy, Clone)]
pub struct GenerationOpt {
    /// How many old copies to keep, 1 effectively means no generations, just one value.
    pub max_generations: NonZeroUsize,
    /// How to encode older generations
    pub(crate) old_gen_encoding: Encoding,
    /// How to determine when a value of any generation has expired
    pub(crate) expiration: ExpirationOpt,
}

impl Default for GenerationOpt {
    #[inline]
    fn default() -> Self {
        Self::new(NonZeroUsize::MIN, Encoding::Plain, ExpirationOpt::NoExpiry)
    }
}

impl GenerationOpt {
    #[must_use]
    pub const fn new(
        max_generations: NonZeroUsize,
        old_gen_encoding: Encoding,
        expiration: ExpirationOpt,
    ) -> Self {
        Self {
            max_generations,
            old_gen_encoding,
            expiration,
        }
    }
}

/// Different encoding options
#[derive(Copy, Clone, Debug)]
pub enum Encoding {
    /// No encoding
    Plain,
    /// Compress using lz4
    #[cfg(feature = "lz4")]
    Lz4,
}

impl Encoding {
    pub(crate) fn serialize(self) -> impl Display {
        match self {
            Encoding::Plain => 0u8,
            #[cfg(feature = "lz4")]
            Encoding::Lz4 => 1u8,
        }
    }

    pub(crate) fn deserialize(s: &str) -> Result<Self> {
        match s {
            "0" => Ok(Self::Plain),
            #[cfg(feature = "lz4")]
            "1" => Ok(Self::Lz4),
            v => Err(Error::ParseMetadata(format!(
                "Failed to parse encoding from {v}"
            ))),
        }
    }

    #[inline]
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn encode(self, content: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            Encoding::Plain => Ok(content),
            #[cfg(feature = "lz4")]
            Encoding::Lz4 => {
                let mut buf = Vec::new();
                let mut encoder = lz4::EncoderBuilder::new().build(&mut buf).map_err(|e| {
                    Error::EncodingError(format!("Failed to create lz4 encoder builder: {e}"))
                })?;
                std::io::Write::write(&mut encoder, &content).map_err(|e| {
                    Error::EncodingError(format!("Failed to lz4 encode content: {e}"))
                })?;
                Ok(buf)
            }
        }
    }
}

/// Options controlling syncing, ensuring that the [`DirCache`]'s state kept in memory is committed to disk.
/// Unnecessary if all keys are not written with [`MemPushOpt::MemoryOnly`]
#[derive(Debug, Copy, Clone, Default)]
pub enum SyncOpt {
    /// Sync when dropped (syncing can still be done manually)
    SyncOnDrop,
    /// Only sync manually
    #[default]
    ManualSync,
}
