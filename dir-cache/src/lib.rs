use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use crate::disk::{ensure_dir, ensure_removed_file, read_all_in_dir, read_raw_if_present, read_subdirs, try_remove_dir};
use crate::error::{Result, Error};
use crate::opts::{Encoding, GenerationOpt, MemPullOpt, MemPushOpt};
use crate::time::{duration_from_nano_string, unix_time_now};

pub mod error;
mod disk;
pub mod opts;
mod time;

const MANIFEST_VERSION: u64 = 1;
const MANIFEST_FILE: &str = "manifest.csv";

pub struct DirCacheInner {
    version: u64,
    base: PathBuf,
    store: HashMap<PathBuf, DirCacheEntry>,
}

impl DirCacheInner {
    pub fn get_opt(&mut self, key: &Path, mem_pull_opt: MemPullOpt) -> Result<Option<Cow<[u8]>>> {
        let Some(val) = self.store.get_mut(key) else {
            return Ok(None);
        };
        let val_ref_in_mem = &mut val.in_mem;
        let store = if let Some(in_mem) = val_ref_in_mem {
            return Ok(Some(Cow::Borrowed(in_mem.content.as_slice())));
        } else {
            let val = read_raw_if_present(key)?
                .ok_or_else(|| Error::ReadContent("No file present on disk where expected", None))?;
            if matches!(mem_pull_opt, MemPullOpt::DontKeepInMemoryOnRead) {
                return Ok(Some(Cow::Owned(val)));
            }
            val
        };
        *val_ref_in_mem = Some(InMemEntry {
            committed: true,
            content: store,
        });
        Ok(Some(Cow::Borrowed(val_ref_in_mem.as_ref().unwrap().content.as_slice())))
    }

    pub fn get_or_insert_opt<E: Into<Box<dyn std::error::Error>>,F: FnOnce() -> core::result::Result<Vec<u8>, E>>(&mut self, key: &Path, insert_with: F, mem_pull_opt: MemPullOpt, mem_push_opt: MemPushOpt, generation_opt: GenerationOpt) -> Result<Cow<[u8]>> {
        // Dumb borrow checker, going to end up here on an if let https://blog.rust-lang.org/inside-rust/2023/10/06/polonius-update.html
        if self.store.contains_key(key) {
            return Ok(self.get_opt(key, mem_pull_opt)?.unwrap())
        }
        let val = match insert_with() {
            Ok(val) => val,
            Err(e) => {
                return Err(Error::InsertWithErr(e.into()));
            }
        };
        let mut entry = DirCacheEntry::new();
        let use_path = self.base.join(key);
        entry.insert_new_data(&use_path, val, mem_push_opt, generation_opt)?;
        self.store.insert(key.to_path_buf(), entry);
        Ok(self.get_opt(key, mem_pull_opt)?.unwrap())
    }

    pub fn insert_opt(&mut self, key: &Path, content: Vec<u8>, mem_push_opt: MemPushOpt, generation_opt: GenerationOpt) -> Result<()> {
        // Borrow checker strikes again
        let path = self.base.join(key);
        if self.store.contains_key(key) {
            let existing = self.store.get_mut(key).unwrap();
            Self::run_dir_cache_entry_write(existing, &path, content, mem_push_opt, generation_opt)?;
        } else {
            let mut dc = DirCacheEntry::new();
            Self::run_dir_cache_entry_write(&mut dc, &path, content, mem_push_opt, generation_opt)?;
            self.store.insert(key.to_path_buf(), dc);
        }
        Ok(())
    }

    fn run_dir_cache_entry_write(dc: &mut DirCacheEntry, path: &Path, content: Vec<u8>, mem_push_opt: MemPushOpt, generation_opt: GenerationOpt) -> Result<()> {
        match mem_push_opt {
            MemPushOpt::RetainAndWrite => {
                dc.generational_write(&path, &content, generation_opt.max_generations.get())?;
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
            }
            MemPushOpt::PassthroughWrite => {
                dc.in_mem = None;
                dc.generational_write(&path, &content, generation_opt.max_generations.get())?;
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, key: &Path) -> Result<bool> {
        let Some(prev) = self.store.remove(key) else {
            return Ok(false);
        };
        // Can't remove_dir_all because of potential subdirs
        let path = self.base.join(key);
        try_remove_dir(&path)?;
        Ok(true)
    }

    pub fn sync_to_disk(&mut self, mem_push_opt: MemPushOpt, generation_opt: GenerationOpt) -> Result<()> {
        for (k, v) in &mut self.store {
            let dir = self.base.join(k);
            ensure_dir(&dir)?;
            let max_rem = generation_opt.max_generations.get() - 1;
            v.dump_in_mem(&dir, matches!(mem_push_opt, MemPushOpt::RetainAndWrite), max_rem)?;
        }
        Ok(())
    }

    pub fn read_from_disk(base: PathBuf, eager_load: bool, generation_opt: GenerationOpt) -> Result<Self> {
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
                store.insert(next, de);
            }
        }
        Ok(Self {
            version: MANIFEST_VERSION,
            base,
            store,
        })
    }
}

pub(crate) struct DirCacheEntry {
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

    fn insert_new_data(&mut self, path: &Path, data: Vec<u8>, mem_push_opt: MemPushOpt, generation_opt: GenerationOpt) -> Result<()> {
        match mem_push_opt {
            MemPushOpt::RetainAndWrite => {
                self.generational_write(path, &data, generation_opt.max_generations.get())?;
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
            }
            MemPushOpt::PassthroughWrite => {
                self.generational_write(path, &data, generation_opt.max_generations.get())?;
            }
        }
        Ok(())
    }

    fn generational_write(&mut self, base: &Path, data: &[u8], max_rem: usize) -> Result<()> {
        while self.on_disk.len() > max_rem {
            let file_name = format!("gen_{}", self.on_disk.len());
            let file = base.join(&file_name);
            ensure_removed_file(&file)?;
        }
        let mut gen_queue = VecDeque::with_capacity(max_rem);
        for (ind, gen) in self.on_disk.drain(..).enumerate().take(max_rem).rev() {
            let n1 = base.join(format!("gen_{ind}"));
            let n2 = base.join(format!("get_{}", ind + 1));
            gen_queue.push_front(gen);
            std::fs::rename(&n1, &n2)
                .map_err(|e| Error::WriteContent("Failed to migrate generations", Some(e)))?;
        }
        let last_update = unix_time_now()?;
        let next_gen = ContentGeneration {
            encoding: Encoding::Plain,
            age: last_update,
        };
        self.on_disk.push_front(next_gen);
        self.last_updated = last_update;
        std::fs::write(base.join("gen_0"), data)
            .map_err(|e| Error::WriteContent("Failed to write new generation", Some(e)))?;
        Ok(())
    }

    fn read_from_dir(base: &Path, eager_load: bool, generation_opt: GenerationOpt) -> Result<Option<Self>> {
        let (version, entries) = Self::read_metadata(base)?;
        if version != MANIFEST_VERSION {
            return Err(Error::ParseManifest(format!("Version mismatch, want={MANIFEST_VERSION}, got={version}")));
        }
        let now = unix_time_now()?;
        let mut in_mem = None;
        let mut on_disk = VecDeque::with_capacity(entries.len());
        let mut last_updated = None;
        for (ind, (age, enc)) in entries.into_iter().enumerate() {
            if age + generation_opt.expiration.as_dur() <= now {
                ensure_removed_file(&base.join(format!("gen_{ind}")))?;
                continue;
            }
            if ind == 0 {
                last_updated = Some(age);
                if eager_load {
                    let path = base.join(format!("gen_{ind}"));
                    let content = std::fs::read(&path)
                        .map_err(|e| Error::ReadContent("Failed to eager load content", Some(e)))?;
                    in_mem = Some(InMemEntry {
                        committed: true,
                        content,
                    })
                }

            }
            on_disk.push_back(ContentGeneration {
                encoding: enc,
                age,
            });
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

    fn read_metadata(base: &Path) -> Result<(u64, VecDeque<(Duration, Encoding)>)> {
        let content = std::fs::read_to_string(base.join(MANIFEST_FILE))
            .map_err(|e| Error::ReadContent("Failed to read manifest file", Some(e)))?;
        let mut lines = content.lines();
        let Some(first) = lines.next() else {
            return Err(Error::ParseMetadata(format!("Manifest at {base:?} was empty")));
        };
        let version: u64 = first.parse()
            .map_err(|_| Error::ParseMetadata(format!("Failed to parse version from metadata at {base:?}")))?;
        let mut generations = VecDeque::new();
        for line in lines {
            let (age_nanos_raw, encoding_raw) = line.split_once(',')
                .ok_or_else(|| Error::ParseMetadata(format!("Metadata was not comma separated at {base:?}")))?;
            let age = duration_from_nano_string(age_nanos_raw)?;
            let encoding = Encoding::deserialize(encoding_raw)?;
            generations.push_front((age, encoding));
        }
        Ok((version, generations))

    }

    fn dump_in_mem(&mut self, base: &Path, keep_in_mem: bool, keep_generations: usize) -> Result<()> {
        let max_rem = keep_generations;
        if let Some(in_mem) = &mut self.in_mem {
            while self.on_disk.len() > max_rem {
                let file_name = format!("gen_{}", self.on_disk.len());
                let file = base.join(&file_name);
                ensure_removed_file(&file)?;
            }
            if !in_mem.committed {
                let mut gen_queue = VecDeque::with_capacity(max_rem);
                for (ind, gen) in self.on_disk.drain(..).enumerate().take(max_rem).rev() {
                    let n1 = base.join(format!("gen_{ind}"));
                    let n2 = base.join(format!("get_{}", ind + 1));
                    gen_queue.push_front(gen);
                    std::fs::rename(&n1, &n2)
                        .map_err(|e| Error::WriteContent("Failed to migrate generations", Some(e)))?;
                }
                let last_update = unix_time_now()?;
                let next_gen = ContentGeneration {
                    encoding: Encoding::Plain,
                    age: last_update,
                };
                self.on_disk.push_front(next_gen);
                self.last_updated = last_update;
                std::fs::write(base.join("gen_0"), &in_mem.content)
                    .map_err(|e| Error::WriteContent("Failed to write new generation", Some(e)))?;
                if keep_in_mem {
                    in_mem.committed = true;
                } else {
                    self.in_mem.take();
                }
            }
        }
        self.dump_metadata(base)?;
        Ok(())
    }

    fn dump_metadata(&self, base: &Path) -> Result<()> {
        let mut metadata = String::new();
        for gen in &self.on_disk {
            let _ = metadata.write_fmt(format_args!("{},{}\n", gen.age.as_nanos(), gen.encoding.serialize()));
        }
        std::fs::write(base.join(MANIFEST_FILE), metadata)
            .map_err(|e| Error::WriteContent("Failed to write manifest", Some(e)))?;
        Ok(())
    }
}

pub(crate) struct InMemEntry {
    committed: bool,
    content: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ContentGeneration {
    pub(crate) encoding: Encoding,
    pub(crate) age: Duration,
}

impl ContentGeneration {
    #[inline]
    pub(crate) fn too_old(&self, ttl: Duration, now: Duration) -> bool {
        self.age.saturating_add(ttl) <= now
    }
}