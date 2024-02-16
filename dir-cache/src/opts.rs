use crate::disk::{ensure_dir, exists, FileObjectExists};
use crate::error::{Error, Result};
use crate::{DirCache, DirCacheInner};
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

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

    pub fn open(self, path: &Path, cache_open_options: CacheOpenOptions) -> Result<DirCache> {
        match cache_open_options.dir_open {
            DirOpen::OnlyIfExists => {
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
            DirOpen::CreateIfMissing => {
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

#[derive(Copy, Clone, Debug)]
pub enum Encoding {
    Plain,
    #[cfg(feature = "lz4")]
    Lz4,
}

impl Encoding {
    pub(crate) fn serialize(&self) -> impl Display {
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

/// Whether syncing should be done on drop
#[derive(Debug, Copy, Clone, Default)]
pub enum SyncOpt {
    /// Sync when dropped (syncing can still be done manually)
    SyncOnDrop,
    /// Only sync manually
    #[default]
    ManualSync,
}
