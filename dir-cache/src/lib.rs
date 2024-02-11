pub mod error;
mod manifest;
pub mod opts;

use error::Result;
use std::borrow::Cow;

use crate::error::Error;
use crate::manifest::Manifest;
use crate::opts::{
    CacheInsertOption, CacheWriteOpt, ExpirationOpt, MemPullOpt, MemPushOpt, SyncErrorOpt,
};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

const MANIFEST_VERSION: u64 = 1;

const MANIFEST_FILE_NAME: &str = "manifest.txt";

pub struct DirCache {
    path: PathBuf,
    manifest: Manifest,
    insert_opt: CacheInsertOption,
    write_opt: CacheWriteOpt,
    sync_opt: SyncErrorOpt,
    push_opt: MemPushOpt,
    pull_opt: MemPullOpt,
    expiration_opt: ExpirationOpt,
}

impl DirCache {
    /// Attempt to read the value of a key from the cache
    /// # Errors
    /// An attempt may be made to read from disk, which could fail
    #[inline]
    pub fn get(&mut self, key: &str) -> Result<Option<Cow<[u8]>>> {
        self.get_opt(key, self.pull_opt)
    }

    pub fn get_opt(&mut self, key: &str, pull_opt: MemPullOpt) -> Result<Option<Cow<[u8]>>> {
        let Some(value) = self.manifest.store.get_mut(key) else {
            return Ok(None);
        };
        // Borrow checker on strike
        let val_cnt_ref = &mut value.content;
        let new = match val_cnt_ref {
            StoreContent::OnDisk(on_disk) => {
                let use_path = self.path.join(on_disk);
                let content =
                    std::fs::read(&use_path).map_err(|e| Error::ReadContent(use_path, e))?;
                match pull_opt {
                    MemPullOpt::KeepInMemoryOnRead => RamStoreValue {
                        content,
                        prev_sync: None,
                    },
                    MemPullOpt::DontKeepInMemoryOnRead => {
                        return Ok(Some(Cow::Owned(content)));
                    }
                }
            }
            StoreContent::InMem(ram) => return Ok(Some(Cow::Borrowed(ram.content.as_slice()))),
        };
        // Need to borrow with this struct's lifetime, so put it back first
        let _ = std::mem::replace(val_cnt_ref, StoreContent::InMem(new));
        let StoreContent::InMem(ram) = val_cnt_ref else {
            unreachable!("[BUG] Just put this value here, how does it not exist?");
        };
        Ok(Some(Cow::Borrowed(ram.content.as_slice())))
    }

    pub fn get_or_insert_with_opt<
        E: Into<Box<dyn std::error::Error>>,
        F: FnOnce() -> core::result::Result<Vec<u8>, E>,
    >(
        &mut self,
        key: &str,
        insert_with: F,
        mem_pull_opt: MemPullOpt,
        mem_push_opt: MemPushOpt,
        cache_write_opt: CacheWriteOpt,
    ) -> Result<Cow<[u8]>> {
        // Borrow checker limitation causes this inefficiency, can't have a conditional lifetime,
        // since Cow is returned if let Some(... borrows mut forever.
        if self.manifest.exists(key) {
            return self
                .get_opt(key, mem_pull_opt)
                .map(|exists| exists.unwrap());
        }
        let val = match insert_with() {
            Ok(val) => val,
            Err(e) => {
                return Err(Error::InsertWithErr(e.into()));
            }
        };

        self.manifest.write_new(
            &self.path,
            key.to_string(),
            val,
            mem_push_opt,
            cache_write_opt,
        )?;
        self.get_opt(key, mem_pull_opt)
            .map(|exists| exists.unwrap())
    }
}

impl Drop for DirCache {
    fn drop(&mut self) {
        let _ = self
            .manifest
            .sync_to_disk(&self.path, self.push_opt, self.sync_opt);
    }
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
