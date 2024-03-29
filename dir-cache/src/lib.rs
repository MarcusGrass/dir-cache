//! # dir-cache - A directory based cache
//!
//! Can be useful in cases when iteratively writing code that uses dynamic data that rarely changes.
//! For example, probing some API.
//!
//! The cache is convenient for some use-cases, but not performant and should not be
//! used in situations were cache-performance is important.
//!
use crate::disk::{
    ensure_dir, ensure_removed_file, read_all_in_dir, read_metadata_if_present,
    read_raw_if_present, try_remove_dir,
};
use crate::error::{Error, Result};
use crate::opts::{DirCacheOpts, Encoding, GenerationOpt, MemPullOpt, MemPushOpt, SyncOpt};
use crate::path_util::{relativize, SafePathJoin};
use crate::time::{duration_from_nano_string, unix_time_now};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

mod disk;
pub mod error;
pub mod opts;
mod path_util;
mod time;

const MANIFEST_VERSION: u64 = 1;
const MANIFEST_FILE: &str = "dir-cache-manifest.txt";

/// A directory-based cache with a map-like interface.
/// # Example
/// ```
/// use std::convert::Infallible;
/// use std::path::Path;
/// use dir_cache::opts::{CacheOpenOptions, DirCacheOpts, DirOpenOpt};
/// fn use_cache() {
///     let temp = tempfile::TempDir::with_prefix("dir-cache-doc-test").unwrap();
///     let mut dir_cache = DirCacheOpts::default()
///         .open(temp.path(), CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false)).unwrap();
///
///     // Be careful about paths used, these are joined onto the base directory and
///     // should ideally not be dynamic
///     let slow_value_key = Path::new("slow-key");
///     // Will only execute the closure if the value is not present.
///     let value = dir_cache.get_or_insert(slow_value_key, || Ok::<_, Infallible>(b"My hard to get value".to_vec())).unwrap();
///     assert_eq!(b"My hard to get value".as_slice(), value.as_ref());
/// }
/// ```
pub struct DirCache {
    inner: DirCacheInner,
    opts: DirCacheOpts,
}

impl DirCache {
    /// Get this [`DirCache`]'s [`DirCacheOpts`].
    /// To change one opt for an operation, for example.
    #[inline]
    #[must_use]
    pub fn opts(&self) -> &DirCacheOpts {
        &self.opts
    }

    /// Get the value of a key using this [`DirCache`]'s options.
    /// Returns [`Option::None`] if the key isn't stored in the cache.
    /// If the key is stored in the cache it will be retrieved either from memory or disk.
    /// The value will be owned only if [`MemPullOpt::DontKeepInMemoryOnRead`] is specified
    /// which is why the return value is a [`Cow<_>`]
    /// # Errors
    /// Various io-errors reading and managing disk state
    #[inline]
    pub fn get(&mut self, key: &Path) -> Result<Option<Cow<[u8]>>> {
        self.inner
            .get_opt(key, self.opts.mem_pull_opt, self.opts.generation_opt)
    }

    /// Same as [`DirCache::get`] but with opts other than what the [`DirCache`] was instantiated
    /// with.
    /// # Errors
    /// Same as [`DirCache::get`]
    #[inline]
    pub fn get_opt(&mut self, key: &Path, opts: DirCacheOpts) -> Result<Option<Cow<[u8]>>> {
        self.inner
            .get_opt(key, opts.mem_pull_opt, opts.generation_opt)
    }

    /// Get a key if it exists and is valid according to [`GenerationOpt`], otherwise
    /// use the provided `insert_with` function to generate and insert a key.
    /// The return value is a [`Cow<_>`] which is borrowed if [`MemPushOpt::MemoryOnly`] or [`MemPushOpt::RetainAndWrite`] is
    /// specified, or owned otherwise.
    /// # Errors
    /// Accepts a fallible function which can fail, in which case that function's converted
    /// error is returned wrapped.
    /// May also perform disk-operations based on opts, which may fail.
    /// Additionally, will fail on paths that are not safe to use with [`DirCache`]
    #[inline]
    pub fn get_or_insert<
        E: Into<Box<dyn std::error::Error>>,
        F: FnOnce() -> core::result::Result<Vec<u8>, E>,
    >(
        &mut self,
        key: &Path,
        insert_with: F,
    ) -> Result<Cow<[u8]>> {
        self.inner.get_or_insert_opt(
            key,
            insert_with,
            self.opts.mem_pull_opt,
            self.opts.mem_push_opt,
            self.opts.generation_opt,
        )
    }

    /// Same as [`DirCache::get_or_insert`] but with [`DirCacheOpts`] different from what
    /// this [`DirCache`] was instantiated with.
    /// # Errors
    /// Same as [`DirCache::get_or_insert`]
    #[inline]
    pub fn get_or_insert_opt<
        E: Into<Box<dyn std::error::Error>>,
        F: FnOnce() -> core::result::Result<Vec<u8>, E>,
    >(
        &mut self,
        key: &Path,
        insert_with: F,
        opts: DirCacheOpts,
    ) -> Result<Cow<[u8]>> {
        self.inner.get_or_insert_opt(
            key,
            insert_with,
            opts.mem_pull_opt,
            opts.mem_push_opt,
            opts.generation_opt,
        )
    }

    /// Insert `content` as a value for the provided `key` into this [`DirCache`].
    /// Will result in direct writes to disk if [`MemPushOpt::MemoryOnly`] isn't used.
    /// If [`MemPushOpt::MemoryOnly`] isn't used and [`GenerationOpt`] specifies more
    /// than one generation, a new generation will be written to disk, and previous generations
    /// will age.
    /// # Errors
    /// Will error on using a key that's not safe to use with [`DirCache`].
    /// May error on various io-errors relating to writing to disk.
    #[inline]
    pub fn insert(&mut self, key: &Path, content: Vec<u8>) -> Result<()> {
        self.inner.insert_opt(
            key,
            content,
            self.opts.mem_push_opt,
            self.opts.generation_opt,
        )
    }

    /// Insert `content` as a value for the provided `key` using the specified `opts` instead
    /// of the [`DirCacheOpts`] that this [`DirCache`] was instantiated with, otherwise same as [`DirCache::insert`].
    /// # Errors
    /// Same as [`DirCache::insert`]
    #[inline]
    pub fn insert_opt(&mut self, key: &Path, content: Vec<u8>, opts: DirCacheOpts) -> Result<()> {
        self.inner
            .insert_opt(key, content, opts.mem_push_opt, opts.generation_opt)
    }

    /// Removes a key from the map, and cleans up the state left on disk.
    /// # Errors
    /// Various io-errors relating to probing and deleting content from disk
    #[inline]
    pub fn remove(&mut self, key: &Path) -> Result<bool> {
        self.inner.remove(key)
    }

    /// Sync in-memory written content to disk, same as [`DirCache::sync`].
    /// If [`SyncOpt::ManualSync`] and [`MemPushOpt::MemoryOnly`] are both enabled,
    /// calling this method is the only way to flush map-state to disk.
    /// # Errors
    /// Various io-errors related to writing to disk
    #[inline]
    pub fn sync(&mut self) -> Result<()> {
        self.inner
            .sync_to_disk(self.opts.mem_push_opt, self.opts.generation_opt)
    }

    /// Sync in-memory written content to disk, same as [`DirCache::sync`] but with options
    /// different to those this [`DirCache`] was instantiated with.
    /// # Errors
    /// Same as [`DirCache::sync`]
    #[inline]
    pub fn sync_opt(&mut self, opts: DirCacheOpts) -> Result<()> {
        self.inner
            .sync_to_disk(opts.mem_push_opt, opts.generation_opt)
    }
}

impl Drop for DirCache {
    fn drop(&mut self) {
        if matches!(self.opts.sync_opt, SyncOpt::SyncOnDrop) {
            let _ = self
                .inner
                .sync_to_disk(self.opts.mem_push_opt, self.opts.generation_opt);
        }
    }
}

struct DirCacheInner {
    base: PathBuf,
    store: HashMap<PathBuf, DirCacheEntry>,
}

impl DirCacheInner {
    fn get_opt(
        &mut self,
        key: &Path,
        mem_pull_opt: MemPullOpt,
        generation_opt: GenerationOpt,
    ) -> Result<Option<Cow<[u8]>>> {
        // Borrow checker...
        if !self.store.contains_key(key) {
            return Ok(None);
        }
        let val = self.store.get(key).unwrap();
        let now = unix_time_now()?;
        let path = self.base.safe_join(key)?;
        // To be able to remove this key, the below Cow borrow-return needs a separate borrow lasting
        // for the remainder of this function, so here we are.
        if val
            .last_updated
            .saturating_add(generation_opt.expiration.as_dur())
            <= now
        {
            // The value in memory should be younger or equal to the first value on disk
            // if it's too old, this key should be cleaned
            try_remove_dir(&path)?;
            self.store.remove(key);
            return Ok(None);
        }

        if let Some(f) = val.on_disk.front() {
            if f.age.saturating_add(generation_opt.expiration.as_dur()) <= now {
                // No value in mem, also first value on disk is too old, clean up
                try_remove_dir(&path)?;
                self.store.remove(key);
                return Ok(None);
            }
        } else if val.in_mem.is_none() {
            // No value in mem, no values on disk, clean
            try_remove_dir(&path)?;
            self.store.remove(key);
            return Ok(None);
        }

        let val_ref_in_mem = &mut self.store.get_mut(key).unwrap().in_mem;
        let store = if let Some(in_mem) = val_ref_in_mem {
            return Ok(Some(Cow::Borrowed(in_mem.content.as_slice())));
        } else {
            let file_path = path.safe_join("dir-cache-generation-0")?;
            let val = read_raw_if_present(&file_path)?.ok_or_else(|| {
                Error::ReadContent(
                    format!("No file present on disk where expected at {file_path:?}"),
                    None,
                )
            })?;
            if matches!(mem_pull_opt, MemPullOpt::DontKeepInMemoryOnRead) {
                return Ok(Some(Cow::Owned(val)));
            }
            val
        };
        *val_ref_in_mem = Some(InMemEntry {
            committed: true,
            content: store,
        });
        Ok(Some(Cow::Borrowed(
            val_ref_in_mem.as_ref().unwrap().content.as_slice(),
        )))
    }

    fn get_or_insert_opt<
        E: Into<Box<dyn std::error::Error>>,
        F: FnOnce() -> core::result::Result<Vec<u8>, E>,
    >(
        &mut self,
        key: &Path,
        insert_with: F,
        mem_pull_opt: MemPullOpt,
        mem_push_opt: MemPushOpt,
        generation_opt: GenerationOpt,
    ) -> Result<Cow<[u8]>> {
        // Dumb borrow checker, going to end up here on an if let https://blog.rust-lang.org/inside-rust/2023/10/06/polonius-update.html
        if self.store.contains_key(key) {
            return Ok(self.get_opt(key, mem_pull_opt, generation_opt)?.unwrap());
        }
        let val = match insert_with() {
            Ok(val) => val,
            Err(e) => {
                return Err(Error::InsertWithErr(e.into()));
            }
        };
        let mut entry = DirCacheEntry::new();
        let use_path = self.base.safe_join(key)?;
        ensure_dir(&use_path)?;
        entry.insert_new_data(&use_path, val, mem_push_opt, generation_opt)?;
        self.store.insert(key.to_path_buf(), entry);
        Ok(self.get_opt(key, mem_pull_opt, generation_opt)?.unwrap())
    }

    fn insert_opt(
        &mut self,
        key: &Path,
        content: Vec<u8>,
        mem_push_opt: MemPushOpt,
        generation_opt: GenerationOpt,
    ) -> Result<()> {
        // Borrow checker strikes again
        let path = self.base.safe_join(key)?;
        if self.store.contains_key(key) {
            let existing = self.store.get_mut(key).unwrap();
            Self::run_dir_cache_entry_write(
                existing,
                &path,
                content,
                mem_push_opt,
                generation_opt,
            )?;
        } else {
            let mut dc = DirCacheEntry::new();
            Self::run_dir_cache_entry_write(&mut dc, &path, content, mem_push_opt, generation_opt)?;
            self.store.insert(key.to_path_buf(), dc);
        }
        Ok(())
    }

    fn remove(&mut self, key: &Path) -> Result<bool> {
        let Some(_prev) = self.store.remove(key) else {
            return Ok(false);
        };
        let path = self.base.safe_join(key)?;
        try_remove_dir(&path)?;
        Ok(true)
    }

    fn run_dir_cache_entry_write(
        dc: &mut DirCacheEntry,
        path: &Path,
        content: Vec<u8>,
        mem_push_opt: MemPushOpt,
        generation_opt: GenerationOpt,
    ) -> Result<()> {
        match mem_push_opt {
            MemPushOpt::RetainAndWrite => {
                ensure_dir(path)?;
                dc.generational_write(
                    path,
                    &content,
                    generation_opt.old_gen_encoding,
                    generation_opt.max_generations.get(),
                )?;
                dc.in_mem = Some(InMemEntry {
                    committed: true,
                    content,
                });
            }
            MemPushOpt::MemoryOnly => {
                dc.in_mem = Some(InMemEntry {
                    committed: false,
                    content,
                });
                dc.last_updated = unix_time_now()?;
            }
            MemPushOpt::PassthroughWrite => {
                dc.in_mem = None;
                ensure_dir(path)?;
                dc.generational_write(
                    path,
                    &content,
                    generation_opt.old_gen_encoding,
                    generation_opt.max_generations.get(),
                )?;
            }
        }
        Ok(())
    }

    fn sync_to_disk(
        &mut self,
        mem_push_opt: MemPushOpt,
        generation_opt: GenerationOpt,
    ) -> Result<()> {
        for (k, v) in &mut self.store {
            let dir = self.base.safe_join(k)?;
            ensure_dir(&dir)?;
            let max_rem = generation_opt.max_generations.get();
            v.dump_in_mem(
                &dir,
                matches!(mem_push_opt, MemPushOpt::RetainAndWrite),
                max_rem,
                generation_opt.old_gen_encoding,
            )?;
        }
        Ok(())
    }

    fn read_from_disk(
        base: PathBuf,
        eager_load: bool,
        generation_opt: GenerationOpt,
    ) -> Result<Self> {
        let mut check_next = VecDeque::new();
        check_next.push_front(base.clone());
        let mut store = HashMap::new();
        while let Some(next) = check_next.pop_front() {
            let entry = DirCacheEntry::read_from_dir(&next, eager_load, generation_opt)?;
            read_all_in_dir(&next, |entry_path, entry_metadata| {
                if entry_metadata.is_dir() {
                    check_next.push_back(entry_path.to_path_buf());
                }
                Ok(())
            })?;
            if let Some(de) = entry {
                let relative = relativize(&base, &next)?;
                store.insert(relative, de);
            }
        }
        Ok(Self { base, store })
    }
}

struct DirCacheEntry {
    in_mem: Option<InMemEntry>,
    on_disk: VecDeque<ContentGeneration>,
    last_updated: Duration,
}

impl DirCacheEntry {
    #[must_use]
    const fn new() -> Self {
        Self {
            in_mem: None,
            on_disk: VecDeque::new(),
            last_updated: Duration::ZERO,
        }
    }

    fn insert_new_data(
        &mut self,
        path: &Path,
        data: Vec<u8>,
        mem_push_opt: MemPushOpt,
        generation_opt: GenerationOpt,
    ) -> Result<()> {
        match mem_push_opt {
            MemPushOpt::RetainAndWrite => {
                self.generational_write(
                    path,
                    &data,
                    generation_opt.old_gen_encoding,
                    generation_opt.max_generations.get(),
                )?;
                self.in_mem = Some(InMemEntry {
                    committed: false,
                    content: data,
                });
            }
            MemPushOpt::MemoryOnly => {
                self.in_mem = Some(InMemEntry {
                    committed: false,
                    content: data,
                });
                self.last_updated = unix_time_now()?;
            }
            MemPushOpt::PassthroughWrite => {
                self.generational_write(
                    path,
                    &data,
                    generation_opt.old_gen_encoding,
                    generation_opt.max_generations.get(),
                )?;
            }
        }
        Ok(())
    }

    fn generational_write(
        &mut self,
        base: &Path,
        data: &[u8],
        old_gen_encoding: Encoding,
        max_rem: usize,
    ) -> Result<()> {
        while self.on_disk.len() > max_rem {
            let file_name = format!("dir-cache-generation-{}", self.on_disk.len());
            let file = base.safe_join(&file_name)?;
            ensure_removed_file(&file)?;
            self.on_disk.pop_back();
        }
        let mut gen_queue = VecDeque::with_capacity(max_rem);
        for (ind, gen) in self.on_disk.drain(..).enumerate().take(max_rem - 1).rev() {
            let n1 = base.safe_join(format!("dir-cache-generation-{ind}"))?;
            let n2 = base.safe_join(format!("dir-cache-generation-{}", ind + 1))?;
            if ind == 0 && !matches!(old_gen_encoding, Encoding::Plain) {
                let content = std::fs::read(&n1).map_err(|e| {
                    Error::ReadContent(
                        format!("Failed to read first generation from {n1:?}"),
                        Some(e),
                    )
                })?;
                let new_content = old_gen_encoding.encode(content)?;
                std::fs::write(&n2, new_content).map_err(|e| {
                    Error::WriteContent(
                        format!("Failed to write encoded content to {n2:?}"),
                        Some(e),
                    )
                })?;
                // Don't need to remove the old file, it'll be overwritten on the next loop, or in the next step
            } else {
                // No recoding necessary, just replace
                std::fs::rename(&n1, &n2).map_err(|e| {
                    Error::WriteContent(
                        format!("Failed to migrate generations from {n1:?} to {n2:?}"),
                        Some(e),
                    )
                })?;
            }
            gen_queue.push_front(gen);
        }
        let last_update = unix_time_now()?;
        let next_gen = ContentGeneration {
            encoding: Encoding::Plain,
            age: last_update,
        };
        self.on_disk.push_front(next_gen);
        for old in gen_queue {
            self.on_disk.push_back(old);
        }
        self.last_updated = last_update;
        let next_gen_path = base.safe_join("dir-cache-generation-0")?;
        std::fs::write(&next_gen_path, data).map_err(|e| {
            Error::WriteContent(
                format!("Failed to write new generation to {next_gen_path:?}"),
                Some(e),
            )
        })?;
        self.dump_metadata(base)?;
        Ok(())
    }

    fn read_from_dir(
        base: &Path,
        eager_load: bool,
        generation_opt: GenerationOpt,
    ) -> Result<Option<Self>> {
        let Some((version, entries)) = Self::read_metadata(base)? else {
            return Ok(None);
        };
        if version != MANIFEST_VERSION {
            return Err(Error::ParseManifest(format!(
                "Version mismatch, want={MANIFEST_VERSION}, got={version}"
            )));
        }
        let now = unix_time_now()?;
        let mut in_mem = None;
        let mut on_disk = VecDeque::with_capacity(entries.len());
        let mut last_updated = None;
        for (ind, (age, enc)) in entries.into_iter().enumerate() {
            if age.saturating_add(generation_opt.expiration.as_dur()) <= now {
                ensure_removed_file(&base.safe_join(format!("dir-cache-generation-{ind}"))?)?;
                continue;
            }
            if ind == 0 {
                last_updated = Some(age);
                if eager_load {
                    let path = base.safe_join(format!("dir-cache-generation-{ind}"))?;
                    let content = std::fs::read(&path).map_err(|e| {
                        Error::ReadContent(
                            format!("Failed to eager load content from {path:?}"),
                            Some(e),
                        )
                    })?;
                    in_mem = Some(InMemEntry {
                        committed: true,
                        content,
                    });
                }
            }
            on_disk.push_back(ContentGeneration { encoding: enc, age });
        }
        if let Some(last_updated) = last_updated {
            Ok(Some(Self {
                in_mem,
                on_disk,
                last_updated,
            }))
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::type_complexity)]
    fn read_metadata(base: &Path) -> Result<Option<(u64, VecDeque<(Duration, Encoding)>)>> {
        let Some(content) = read_metadata_if_present(&base.safe_join(MANIFEST_FILE)?)? else {
            return Ok(None);
        };
        let mut lines = content.lines();
        let Some(first) = lines.next() else {
            return Err(Error::ParseMetadata(format!(
                "Manifest at {base:?} was empty"
            )));
        };
        let version: u64 = first.parse().map_err(|_| {
            Error::ParseMetadata(format!("Failed to parse version from metadata at {base:?}"))
        })?;
        let mut generations = VecDeque::new();
        for line in lines {
            let (age_nanos_raw, encoding_raw) = line.split_once(',').ok_or_else(|| {
                Error::ParseMetadata(format!("Metadata was not comma separated at {base:?}"))
            })?;
            let age = duration_from_nano_string(age_nanos_raw)?;
            let encoding = Encoding::deserialize(encoding_raw)?;
            generations.push_front((age, encoding));
        }
        Ok(Some((version, generations)))
    }

    fn dump_in_mem(
        &mut self,
        base: &Path,
        keep_in_mem: bool,
        keep_generations: usize,
        old_gen_encoding: Encoding,
    ) -> Result<()> {
        let maybe_in_mem = self.in_mem.take();
        if let Some(mut in_mem) = maybe_in_mem {
            if !in_mem.committed {
                self.generational_write(base, &in_mem.content, old_gen_encoding, keep_generations)?;
                if keep_in_mem {
                    in_mem.committed = true;
                    self.in_mem = Some(in_mem);
                }
                return Ok(());
            }
        }
        self.dump_metadata(base)?;
        Ok(())
    }

    fn dump_metadata(&self, base: &Path) -> Result<()> {
        let mut metadata = format!("{MANIFEST_VERSION}\n");
        for gen in &self.on_disk {
            let _ = metadata.write_fmt(format_args!(
                "{},{}\n",
                gen.age.as_nanos(),
                gen.encoding.serialize()
            ));
        }
        let manifest_path = base.safe_join(MANIFEST_FILE)?;
        std::fs::write(&manifest_path, metadata).map_err(|e| {
            Error::WriteContent(
                format!("Failed to write manifest to {manifest_path:?}"),
                Some(e),
            )
        })?;
        Ok(())
    }
}

struct InMemEntry {
    committed: bool,
    content: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct ContentGeneration {
    encoding: Encoding,
    age: Duration,
}
