pub mod error;
pub mod opts;
mod serde;

use error::Result;

use crate::error::Error;
use crate::opts::{CacheInsertOption, CacheReadOpt, CacheWriteOpt, MemFlushOpt, SyncErrorOpt};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

const MANIFEST_VERSION: u64 = 1;

const MANIFEST_FILE_NAME: &str = "manifest.txt";

pub struct DirCache {
    path: PathBuf,
    manifest: Manifest,
    insert_opt: CacheInsertOption,
    write_opt: CacheWriteOpt,
    read_opt: CacheReadOpt,
    sync_opt: SyncErrorOpt,
    flush_opt: MemFlushOpt,
}

impl DirCache {}

struct Manifest {
    pub(crate) version: u64,
    pub(crate) store: HashMap<String, StoreValue>,
}

pub(crate) struct StoreValue {
    pub(crate) content: StoreContent,
    pub(crate) last_updated: Duration,
}

pub(crate) enum StoreContent {
    OnDisk(String),
    InMem(RamStoreValue),
}

pub(crate) struct RamStoreValue {
    content: Vec<u8>,
    prev_sync: Option<PrevSync>,
}

pub(crate) struct PrevSync {
    content_file: String,
    synced_at: Duration,
}

#[inline]
pub(crate) fn unix_time_now() -> Result<Duration> {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(Error::SystemTime)
}
